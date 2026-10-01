package sum

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const BatchFlushThreshold = 2000

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                      int
	inputQueue              middleware.Middleware
	outputExchange          middleware.Middleware
	coordinator             *Coordinator
	clientFruitItemMap      map[uint64]map[string]fruititem.FruitItem
	completedClients        map[uint64]bool
	clientPendingItemsCount map[uint64]int
	mu                      sync.Mutex
	aggregationAmount       int
	aggregationPrefix       string
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	sumInstance := &Sum{
		id:                      config.Id,
		inputQueue:              inputQueue,
		outputExchange:          outputExchange,
		clientFruitItemMap:      map[uint64]map[string]fruititem.FruitItem{},
		completedClients:        map[uint64]bool{},
		clientPendingItemsCount: map[uint64]int{},
		aggregationAmount:       config.AggregationAmount,
		aggregationPrefix:       config.AggregationPrefix,
	}

	coordinator, err := newCoordinator(config.Id, connSettings, config.SumPrefix, sumInstance.flushAndSendEOF)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	sumInstance.coordinator = coordinator
	return sumInstance, nil
}

func (sum *Sum) Run(ctx context.Context) error {
	sum.coordinator.Run()

	consumeErrChan := make(chan error, 1)
	go func() {
		err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleMessage(msg, ack, nack)
		})
		consumeErrChan <- err
	}()

	select {
	case <-ctx.Done():
		slog.Info("Shutting down Sum node gracefully...")
		sum.Close()
		return nil
	case err := <-consumeErrChan:
		return err
	}
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	packet, err := inner.DeserializePacket(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch packet.Type {
	case inner.PacketTypeEOF:
		sum.coordinator.coordinateEOF(packet.ClientID, packet.TotalMsgs)
	case inner.PacketTypeData:
		if err := sum.handleDataMessage(packet.ClientID, packet.Records); err != nil {
			slog.Error("While handling data message", "err", err)
		}
	default:
		slog.Warn("Unknown packet type received", "type", packet.Type)
	}
}

func (sum *Sum) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) error {
	sum.mu.Lock()

	sum.coordinator.AddProcessedCount(clientID)

	if _, exists := sum.clientFruitItemMap[clientID]; !exists {
		sum.clientFruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
	}

	fruitMap := sum.clientFruitItemMap[clientID]
	for _, fruitRecord := range fruitRecords {
		if current, ok := fruitMap[fruitRecord.Fruit]; ok {
			fruitMap[fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			fruitMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	sum.clientPendingItemsCount[clientID] += len(fruitRecords)

	var itemsToFlush map[string]fruititem.FruitItem
	if sum.clientPendingItemsCount[clientID] >= BatchFlushThreshold {
		itemsToFlush = sum.clientFruitItemMap[clientID]
		sum.clientFruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
		sum.clientPendingItemsCount[clientID] = 0
	}
	sum.mu.Unlock()

	if len(itemsToFlush) > 0 {
		return sum.sendBatchItems(clientID, itemsToFlush)
	}

	return nil
}

func (sum *Sum) sendBatchItems(clientID uint64, items map[string]fruititem.FruitItem) error {
	partitionBatches := make(map[int][]fruititem.FruitItem)

	for _, item := range items {
		h := fnv.New32a()
		h.Write([]byte(item.Fruit))
		targetID := int(h.Sum32()) % sum.aggregationAmount
		partitionBatches[targetID] = append(partitionBatches[targetID], item)
	}

	for targetID, records := range partitionBatches {
		if len(records) == 0 {
			continue
		}
		message, err := inner.SerializeMessage(clientID, records)
		if err != nil {
			slog.Error("While serializing batch message", "err", err)
			return err
		}

		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, targetID)
		if err := sum.outputExchange.SendTo(routingKey, *message); err != nil {
			slog.Error("While sending batch message", "err", err)
			return err
		}
	}
	return nil
}

func (sum *Sum) flushAndSendEOF(clientID uint64) error {
	sum.mu.Lock()
	if sum.completedClients[clientID] {
		sum.mu.Unlock()
		return nil
	}
	sum.completedClients[clientID] = true

	remainingMap := sum.clientFruitItemMap[clientID]
	sum.mu.Unlock()

	if len(remainingMap) > 0 {
		if err := sum.sendBatchItems(clientID, remainingMap); err != nil {
			return err
		}
	}

	eofMessage, err := inner.SerializeEOFMessage(clientID, 0)
	if err != nil {
		return err
	}

	for i := range sum.aggregationAmount {
		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, i)
		if err := sum.outputExchange.SendTo(routingKey, *eofMessage); err != nil {
			return err
		}
	}
	sum.cleanupClientState(clientID)

	return nil
}

func (sum *Sum) cleanupClientState(clientID uint64) {
	sum.mu.Lock()
	defer sum.mu.Unlock()

	delete(sum.clientFruitItemMap, clientID)
	delete(sum.completedClients, clientID)
	delete(sum.clientPendingItemsCount, clientID)
}

func (sum *Sum) Close() {
	sum.mu.Lock()
	defer sum.mu.Unlock()

	_ = sum.inputQueue.StopConsuming()
	_ = sum.inputQueue.Close()

	sum.coordinator.Close()

	_ = sum.outputExchange.Close()
}
