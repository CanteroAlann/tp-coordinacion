package sum

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/logger"
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

type ControlMsgType string

const (
	MsgAnnounceEOF ControlMsgType = "ANNOUNCE_EOF"
	MsgReportCount ControlMsgType = "REPORT_COUNT"
	MsgCommitFlush ControlMsgType = "COMMIT_FLUSH"
)

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
	controlExchange           middleware.Middleware
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

	controlExchangeName := fmt.Sprintf("%s_control", config.SumPrefix)
	controlExchangeKeys := []string{"sum_eof"}
	controlExchange, err := middleware.CreateExchangeMiddleware(controlExchangeName, controlExchangeKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	return &Sum{
		id:                        config.Id,
		inputQueue:                inputQueue,
		outputExchange:            outputExchange,
		controlExchange:           controlExchange,
		clientFruitItemMap:        map[uint64]map[string]fruititem.FruitItem{},
		completedClients:          map[uint64]bool{},
		processedMessagesByClient: map[uint64]uint64{},
		reportedMessagesByClient:  map[uint64]map[int]uint64{},
		iAmCoordinator:            false,
		aggregationAmount:         config.AggregationAmount,
		aggregationPrefix:         config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		err := sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			defer ack()
			var payload ControlPayload
			if err := json.Unmarshal([]byte(msg.Body), &payload); err != nil {
				slog.Error("While deserializing control payload", "err", err)
				return
			}
			sum.handleControlMessage(payload)
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

	packet, err := inner.DeserializePacket(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch packet.Type {
	case inner.PacketTypeEOF:
		go sum.coordinateEOF(packet.ClientID, packet.TotalMsgs)
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

	if sum.completedClients[clientID] {
		slog.Error("¡CARRERA DETECTADA! Llegaron datos luego de haber enviado el EOF",
			"client_id", clientID,
			"records", fruitRecords)
		return nil
	}

	sum.processedMessagesByClient[clientID]++

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

func (sum *Sum) coordinateEOF(clientID uint64, totalExpected uint64) {

	logger.Info("Coordinating EOF for client", logger.InProgress, "client_id", clientID, "total_expected", totalExpected, "coordinator_id", sum.id)
	announce := ControlPayload{
		Type:          MsgAnnounceEOF,
		ClientID:      clientID,
		SenderID:      sum.id,
		TotalExpected: totalExpected,
	}
	bytes, _ := json.Marshal(announce)
	_ = sum.controlExchange.Send(middleware.Message{Body: string(bytes)})
}

func (sum *Sum) handleControlMessage(payload ControlPayload) {
	//logger.Info("Handling control message", logger.InProgress, "type", payload.Type, "client_id", payload.ClientID, "sender_id", payload.SenderID, "total_expected", payload.TotalExpected, "count", payload.Count)
	switch payload.Type {
	case MsgAnnounceEOF:
		sum.mu.Lock()
		count := sum.processedMessagesByClient[payload.ClientID]
		sum.mu.Unlock()

		report := ControlPayload{
			Type:     MsgReportCount,
			ClientID: payload.ClientID,
			SenderID: sum.id,
			Count:    count,
		}
		bytes, _ := json.Marshal(report)
		_ = sum.controlExchange.Send(middleware.Message{Body: string(bytes)})

		if payload.SenderID == sum.id && !sum.iAmCoordinator {
			go sum.waitForAllProcessed(payload.ClientID, payload.TotalExpected)
		}

	case MsgReportCount:
		sum.mu.Lock()
		if _, exists := sum.reportedMessagesByClient[payload.ClientID]; !exists {
			sum.reportedMessagesByClient[payload.ClientID] = make(map[int]uint64)
		}
		// Guardar el conteo asignado al sender_id que lo envió
		sum.reportedMessagesByClient[payload.ClientID][payload.SenderID] = payload.Count
		sum.mu.Unlock()
	case MsgCommitFlush:
		if err := sum.flushAndSendEOF(payload.ClientID); err != nil {
			slog.Error("While executing commit flush", "err", err)
		}
	}
}

func (sum *Sum) waitForAllProcessed(clientID uint64, totalExpected uint64) {
	logger.Info("Waiting for all processed messages", logger.InProgress, "client_id", clientID, "total_expected", totalExpected, "coordinator_id", sum.id)

	for {
		sum.mu.Lock()
		// Actualizar el conteo del propio coordinador
		if _, exists := sum.reportedMessagesByClient[clientID]; !exists {
			sum.reportedMessagesByClient[clientID] = make(map[int]uint64)
		}
		sum.reportedMessagesByClient[clientID][sum.id] = sum.processedMessagesByClient[clientID]

		// Sumar los reportes de todos los nodos conocidos
		totalAccum := uint64(0)
		for _, count := range sum.reportedMessagesByClient[clientID] {
			totalAccum += count
		}
		sum.mu.Unlock()

		if totalAccum >= totalExpected {
			logger.Info("All messages processed, sending commit flush", logger.Success, "client_id", clientID, "totalAccum", totalAccum, "totalExpected", totalExpected)
			commit := ControlPayload{
				Type:     MsgCommitFlush,
				ClientID: clientID,
			}
			bytes, _ := json.Marshal(commit)
			_ = sum.controlExchange.Send(middleware.Message{Body: string(bytes)})
			return
		}

		time.Sleep(15 * time.Millisecond)

		// Solicitar nuevamente los conteos sin instanciar nuevas goroutines
		announce := ControlPayload{
			Type:          MsgAnnounceEOF,
			ClientID:      clientID,
			SenderID:      sum.id,
			TotalExpected: totalExpected,
		}
		bytes, _ := json.Marshal(announce)
		_ = sum.controlExchange.Send(middleware.Message{Body: string(bytes)})
	}
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
