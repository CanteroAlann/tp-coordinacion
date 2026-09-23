package sum

import (
	"fmt"
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

type Sum struct {
	inputQueue         middleware.Middleware
	outputExchange     middleware.Middleware
	controlExchange    middleware.Middleware
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
	completedClients   map[uint64]bool
	mu                 sync.Mutex
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

	controlExchangeName := fmt.Sprintf("%s_control", config.SumPrefix)
	controlExchangeKeys := []string{"sum_eof"}
	controlExchange, err := middleware.CreateExchangeMiddleware(controlExchangeName, controlExchangeKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		controlExchange:    controlExchange,
		clientFruitItemMap: map[uint64]map[string]fruititem.FruitItem{},
		completedClients:   map[uint64]bool{},
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		err := sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			defer ack()
			clientID, _, isEof, err := inner.DeserializeMessage(&msg)
			if err != nil {
				slog.Error("While deserializing control message", "err", err)
				return
			}
			if isEof {
				if err := sum.flushAndSendEOF(clientID); err != nil {
					slog.Error("While processing broadcasted EOF", "err", err)
				}
			}
		})
		if err != nil {
			slog.Error("Control exchange stopped with error", "err", err)
		}
	}()

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		eofMsg, err := inner.SerializeMessage(clientID, []fruititem.FruitItem{})
		if err != nil {
			slog.Error("While serializing control EOF message", "err", err)
			return
		}
		if err := sum.controlExchange.Send(*eofMsg); err != nil {
			slog.Error("While broadcasting EOF to control exchange", "err", err)
			return
		}

		if err := sum.flushAndSendEOF(clientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleDataMessage(clientID uint64, fruitRecords []fruititem.FruitItem) error {
	sum.mu.Lock()
	defer sum.mu.Unlock()

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
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientID, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message to aggregation", "err", err)
		return err
	}

	return nil
}
