package join

import (
	"context"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue         middleware.Middleware
	outputQueue        middleware.Middleware
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
	clientCompletedMap map[uint64]int
	topSize            int
	aggregationAmount  int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:         inputQueue,
		outputQueue:        outputQueue,
		topSize:            config.TopSize,
		clientFruitItemMap: make(map[uint64]map[string]fruititem.FruitItem),
		clientCompletedMap: make(map[uint64]int),
		aggregationAmount:  config.AggregationAmount,
	}, nil
}

func (join *Join) Run(ctx context.Context) error {
	consumeErrChan := make(chan error, 1)
	go func() {
		err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			join.handleMessage(msg, ack, nack)
		})
		consumeErrChan <- err
	}()

	select {
	case <-ctx.Done():
		slog.Info("Shutting down Join node gracefully...")
		join.Close()
		return nil
	case err := <-consumeErrChan:
		return err
	}
}

func (join *Join) handleMessage(msg middleware.Message, ack, nack func()) {
	client_id, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Debug("While deserializing message", "err", err)
		nack()
		return
	}
	if isEof {
		if err := join.handleEndOfRecordsMessage(client_id); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
	join.handleDataMessage(client_id, fruitRecords)
}

func (join *Join) handleEndOfRecordsMessage(client_id uint64) error {
	slog.Info("Received End Of Records message")

	join.clientCompletedMap[client_id]++

	if join.clientCompletedMap[client_id] == join.aggregationAmount {

		fruitTopRecords := join.buildFruitTop(client_id)
		message, err := inner.SerializeMessage(client_id, fruitTopRecords)
		if err != nil {
			slog.Debug("While serializing top message", "err", err)
			return err
		}
		if err := join.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending top message", "err", err)
			return err
		}
		join.cleanupClientState(client_id)

		return nil
	}
	return nil
}

func (join *Join) buildFruitTop(client_id uint64) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(join.clientFruitItemMap[client_id]))
	for _, item := range join.clientFruitItemMap[client_id] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (join *Join) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) {

	if _, exists := join.clientFruitItemMap[clientID]; !exists {
		join.clientFruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
	}

	fruitMap := join.clientFruitItemMap[clientID]

	for _, fruitRecord := range fruitRecords {
		if _, ok := fruitMap[fruitRecord.Fruit]; ok {
			fruitMap[fruitRecord.Fruit] = fruitMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) cleanupClientState(clientID uint64) {
	delete(join.clientFruitItemMap, clientID)
	delete(join.clientCompletedMap, clientID)
}

func (join *Join) Close() {
	_ = join.inputQueue.StopConsuming()
	_ = join.inputQueue.Close()
	_ = join.outputQueue.Close()
}
