package sum

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/logger"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type ControlMsgType string

const (
	MsgAnnounceEOF ControlMsgType = "ANNOUNCE_EOF"
	MsgReportCount ControlMsgType = "REPORT_COUNT"
	MsgCommitFlush ControlMsgType = "COMMIT_FLUSH"
)

type Coordinator struct {
	id                        int
	controlExchange           middleware.Middleware
	reportedMessagesByClient  map[uint64]map[int]uint64
	processedMessagesByClient map[uint64]uint64
	iAmLeader                 bool
	mu                        sync.Mutex
	onFlushCallback           func(clientID uint64) error
}

func newCoordinator(id int, connSettings middleware.ConnSettings, sumPrefix string, onFlushCallback func(clientID uint64) error) (*Coordinator, error) {
	controlExchangeName := fmt.Sprintf("%s_control", sumPrefix)
	controlExchangeKeys := []string{"sum_eof"}
	controlExchange, err := middleware.CreateExchangeMiddleware(controlExchangeName, controlExchangeKeys, connSettings)
	if err != nil {
		controlExchange.Close()
		return nil, err
	}
	return &Coordinator{
		id:                        id,
		controlExchange:           controlExchange,
		reportedMessagesByClient:  map[uint64]map[int]uint64{},
		processedMessagesByClient: map[uint64]uint64{},
		iAmLeader:                 false,
		onFlushCallback:           onFlushCallback,
	}, nil
}

func (coordinator *Coordinator) Run() {
	go func() {
		err := coordinator.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			defer ack()
			var payload ControlPayload
			if err := json.Unmarshal([]byte(msg.Body), &payload); err != nil {
				slog.Error("While deserializing control payload", "err", err)
				return
			}
			coordinator.handleControlMessage(payload)
		})
		if err != nil {
			slog.Error("Control exchange stopped with error", "err", err)
		}
	}()
}

func (coordinator *Coordinator) handleControlMessage(payload ControlPayload) {
	switch payload.Type {
	case MsgAnnounceEOF:

		report := ControlPayload{
			Type:     MsgReportCount,
			ClientID: payload.ClientID,
			SenderID: coordinator.id,
			Count:    coordinator.processedMessagesByClient[payload.ClientID],
		}
		bytes, _ := json.Marshal(report)
		_ = coordinator.controlExchange.Send(middleware.Message{Body: string(bytes)})

		if payload.SenderID == coordinator.id && !coordinator.iAmLeader {
			go coordinator.waitForAllProcessed(payload.ClientID, payload.TotalExpected)
		}

	case MsgReportCount:
		if _, exists := coordinator.reportedMessagesByClient[payload.ClientID]; !exists {
			coordinator.reportedMessagesByClient[payload.ClientID] = make(map[int]uint64)
		}

		coordinator.reportedMessagesByClient[payload.ClientID][payload.SenderID] = payload.Count
	case MsgCommitFlush:
		if err := coordinator.onFlushCallback(payload.ClientID); err != nil {
			slog.Error("While executing commit flush", "err", err)
		}
	}
}

func (coordinator *Coordinator) coordinateEOF(clientID uint64, totalExpected uint64) {

	announce := ControlPayload{
		Type:          MsgAnnounceEOF,
		ClientID:      clientID,
		SenderID:      coordinator.id,
		TotalExpected: totalExpected,
	}
	bytes, _ := json.Marshal(announce)
	_ = coordinator.controlExchange.Send(middleware.Message{Body: string(bytes)})
}

func (coordinator *Coordinator) waitForAllProcessed(clientID uint64, totalExpected uint64) {
	//logger.Info("Waiting for all processed messages", logger.InProgress, "client_id", clientID, "total_expected", totalExpected, "coordinator_id", sum.id)

	for {
		coordinator.mu.Lock()
		if _, exists := coordinator.reportedMessagesByClient[clientID]; !exists {
			coordinator.reportedMessagesByClient[clientID] = make(map[int]uint64)
		}
		coordinator.reportedMessagesByClient[clientID][coordinator.id] = coordinator.processedMessagesByClient[clientID]

		totalAccum := uint64(0)
		for _, count := range coordinator.reportedMessagesByClient[clientID] {
			totalAccum += count
		}
		coordinator.mu.Unlock()

		if totalAccum >= totalExpected {
			logger.Info("All messages processed, sending commit flush", logger.Success, "client_id", clientID, "totalAccum", totalAccum, "totalExpected", totalExpected)
			commit := ControlPayload{
				Type:     MsgCommitFlush,
				ClientID: clientID,
			}
			bytes, _ := json.Marshal(commit)
			_ = coordinator.controlExchange.Send(middleware.Message{Body: string(bytes)})
			return
		}

		time.Sleep(15 * time.Millisecond)

		announce := ControlPayload{
			Type:          MsgAnnounceEOF,
			ClientID:      clientID,
			SenderID:      coordinator.id,
			TotalExpected: totalExpected,
		}
		bytes, _ := json.Marshal(announce)
		_ = coordinator.controlExchange.Send(middleware.Message{Body: string(bytes)})
	}
}
func (coordinator *Coordinator) AddProcessedCount(clientID uint64) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.processedMessagesByClient[clientID]++
}
