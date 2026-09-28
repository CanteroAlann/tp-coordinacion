package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue        middleware.Middleware
	inputExchange      middleware.Middleware
	sumAmount          int
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
	clientCompletedMap map[uint64]int
	topSize            int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:        outputQueue,
		inputExchange:      inputExchange,
		sumAmount:          config.SumAmount,
		clientFruitItemMap: map[uint64]map[string]fruititem.FruitItem{},
		clientCompletedMap: map[uint64]int{},
		topSize:            config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	client_id, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(client_id); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(client_id, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(client_id uint64) error {
	slog.Info("Received End Of Records message")

	aggregation.clientCompletedMap[client_id]++

	if aggregation.clientCompletedMap[client_id] == aggregation.sumAmount {

		fruitTopRecords := aggregation.buildFruitTop(client_id)
		message, err := inner.SerializeMessage(client_id, fruitTopRecords)
		if err != nil {
			slog.Debug("While serializing top message", "err", err)
			return err
		}
		if err := aggregation.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending top message", "err", err)
			return err
		}

		message, err = inner.SerializeEOFMessage(client_id, 0)
		if err != nil {
			slog.Debug("While serializing EOF message", "err", err)
			return err
		}
		if err := aggregation.outputQueue.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) {

	if _, exists := aggregation.clientFruitItemMap[clientID]; !exists {
		aggregation.clientFruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
	}

	fruitMap := aggregation.clientFruitItemMap[clientID]

	for _, fruitRecord := range fruitRecords {
		if _, ok := fruitMap[fruitRecord.Fruit]; ok {
			fruitMap[fruitRecord.Fruit] = fruitMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruitMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(client_id uint64) []fruititem.FruitItem {
	fruitMap, exists := aggregation.clientFruitItemMap[client_id]
	if !exists || len(fruitMap) == 0 {
		return []fruititem.FruitItem{}
	}
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.clientFruitItemMap[client_id]))
	for _, item := range aggregation.clientFruitItemMap[client_id] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
