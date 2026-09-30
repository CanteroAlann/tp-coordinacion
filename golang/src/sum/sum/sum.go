package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

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

type ControlPayload struct {
	Type          ControlMsgType `json:"type"`
	ClientID      uint64         `json:"client_id"`
	SenderID      int            `json:"sender_id"`
	TotalExpected uint64         `json:"total_expected,omitempty"`
	Count         uint64         `json:"count,omitempty"`
}

type Sum struct {
	id                        int
	inputQueue                middleware.Middleware
	outputExchange            middleware.Middleware
	coordinator               *Coordinator
	clientFruitItemMap        map[uint64]map[string]fruititem.FruitItem
	completedClients          map[uint64]bool
	processedMessagesByClient map[uint64]uint64
	reportedMessagesByClient  map[uint64]map[int]uint64
	mu                        sync.Mutex
	iAmCoordinator            bool
	aggregationAmount         int
	aggregationPrefix         string
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
		id:                 config.Id,
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		clientFruitItemMap: map[uint64]map[string]fruititem.FruitItem{},
		completedClients:   map[uint64]bool{},
		aggregationAmount:  config.AggregationAmount,
		aggregationPrefix:  config.AggregationPrefix,
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

func (sum *Sum) Run() {
	sum.coordinator.Run()
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
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
		go sum.coordinator.coordinateEOF(packet.ClientID, packet.TotalMsgs)
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
	defer sum.mu.Unlock()

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
	return nil
}

func (sum *Sum) flushAndSendEOF(clientID uint64) error {
	sum.mu.Lock()
	if sum.completedClients[clientID] {
		sum.mu.Unlock()
		return nil
	}
	sum.completedClients[clientID] = true

	fruitMap := sum.clientFruitItemMap[clientID]
	delete(sum.clientFruitItemMap, clientID)
	sum.mu.Unlock()

	for _, fruitItem := range fruitMap {
		message, err := inner.SerializeMessage(clientID, []fruititem.FruitItem{fruitItem})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		h := fnv.New32a()
		h.Write([]byte(fruitItem.Fruit))
		targetID := int(h.Sum32()) % sum.aggregationAmount
		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, targetID)
		if err := sum.outputExchange.SendTo(routingKey, *message); err != nil {
			return err
		}
	}

	eofMessage, err := inner.SerializeEOFMessage(clientID, sum.processedMessagesByClient[clientID])
	if err != nil {
		return err
	}

	for i := range sum.aggregationAmount {
		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, i)
		if err := sum.outputExchange.SendTo(routingKey, *eofMessage); err != nil {
			return err
		}
	}

	return nil
}
