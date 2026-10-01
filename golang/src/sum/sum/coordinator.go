package sum

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

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
	expectedMessagesByClient  map[uint64]uint64
	mu                        sync.Mutex
	isEOFAnnounced            map[uint64]bool
	completedClients          map[uint64]bool
	onFlushCallback           func(clientID uint64) error
	ctx                       context.Context
	cancel                    context.CancelFunc
	wg                        sync.WaitGroup
}

type ControlPayload struct {
	Type          ControlMsgType `json:"type"`
	ClientID      uint64         `json:"client_id"`
	SenderID      int            `json:"sender_id"`
	TotalExpected uint64         `json:"total_expected,omitempty"`
	Count         uint64         `json:"count,omitempty"`
}

func newCoordinator(id int, connSettings middleware.ConnSettings, sumPrefix string, onFlushCallback func(clientID uint64) error) (*Coordinator, error) {
	controlExchangeName := fmt.Sprintf("%s_control", sumPrefix)
	controlExchangeKeys := []string{"sum_eof"}
	controlExchange, err := middleware.CreateExchangeMiddleware(controlExchangeName, controlExchangeKeys, connSettings)
	if err != nil {
		controlExchange.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Coordinator{
		id:                        id,
		controlExchange:           controlExchange,
		reportedMessagesByClient:  map[uint64]map[int]uint64{},
		processedMessagesByClient: map[uint64]uint64{},
		expectedMessagesByClient:  map[uint64]uint64{},
		isEOFAnnounced:            map[uint64]bool{},
		completedClients:          map[uint64]bool{},

		onFlushCallback: onFlushCallback,
		ctx:             ctx,
		cancel:          cancel,
	}, nil
}

func (coordinator *Coordinator) Run() {
	coordinator.wg.Add(1)
	go func() {
		defer coordinator.wg.Done()
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

		if payload.SenderID == coordinator.id {
			return
		}

		coordinator.mu.Lock()
		coordinator.isEOFAnnounced[payload.ClientID] = true
		count := coordinator.processedMessagesByClient[payload.ClientID]
		coordinator.mu.Unlock()

		coordinator.sendReportCount(payload.ClientID, count)

	case MsgReportCount:
		coordinator.mu.Lock()
		if _, exists := coordinator.reportedMessagesByClient[payload.ClientID]; !exists {
			coordinator.reportedMessagesByClient[payload.ClientID] = make(map[int]uint64)
		}
		coordinator.reportedMessagesByClient[payload.ClientID][payload.SenderID] = payload.Count

		totalExpected, isCoordinating := coordinator.expectedMessagesByClient[payload.ClientID]
		coordinator.mu.Unlock()

		if isCoordinating {
			coordinator.tryCommitFlush(payload.ClientID, totalExpected)
		}

	case MsgCommitFlush:
		if err := coordinator.onFlushCallback(payload.ClientID); err != nil {
			slog.Error("While executing commit flush", "err", err)
		}
		coordinator.cleanupClientState(payload.ClientID)
	}
}

func (coordinator *Coordinator) coordinateEOF(clientID uint64, totalExpected uint64) {
	coordinator.mu.Lock()
	coordinator.expectedMessagesByClient[clientID] = totalExpected
	coordinator.isEOFAnnounced[clientID] = true

	if _, exists := coordinator.reportedMessagesByClient[clientID]; !exists {
		coordinator.reportedMessagesByClient[clientID] = make(map[int]uint64)
	}
	coordinator.reportedMessagesByClient[clientID][coordinator.id] = coordinator.processedMessagesByClient[clientID]
	coordinator.mu.Unlock()

	announce := ControlPayload{
		Type:          MsgAnnounceEOF,
		ClientID:      clientID,
		SenderID:      coordinator.id,
		TotalExpected: totalExpected,
	}
	bytes, _ := json.Marshal(announce)
	_ = coordinator.controlExchange.Send(middleware.Message{Body: string(bytes)})

	coordinator.tryCommitFlush(clientID, totalExpected)
}

func (coordinator *Coordinator) tryCommitFlush(clientID uint64, totalExpected uint64) {
	coordinator.mu.Lock()

	if coordinator.completedClients[clientID] {
		return
	}
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
	}
}

func (coordinator *Coordinator) sendReportCount(clientID uint64, count uint64) {
	report := ControlPayload{
		Type:     MsgReportCount,
		ClientID: clientID,
		SenderID: coordinator.id,
		Count:    count,
	}
	bytes, _ := json.Marshal(report)
	_ = coordinator.controlExchange.Send(middleware.Message{Body: string(bytes)})
}

func (coordinator *Coordinator) AddProcessedCount(clientID uint64) {
	coordinator.mu.Lock()
	coordinator.processedMessagesByClient[clientID]++
	count := coordinator.processedMessagesByClient[clientID]

	isAnnounced := coordinator.isEOFAnnounced[clientID]
	coordinator.mu.Unlock()

	if isAnnounced {
		coordinator.sendReportCount(clientID, count)
	}
}

func (coordinator *Coordinator) cleanupClientState(clientID uint64) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	delete(coordinator.reportedMessagesByClient, clientID)
	delete(coordinator.processedMessagesByClient, clientID)
	delete(coordinator.expectedMessagesByClient, clientID)
	delete(coordinator.isEOFAnnounced, clientID)
	delete(coordinator.completedClients, clientID)
}

func (coordinator *Coordinator) Close() {
	coordinator.cancel()
	_ = coordinator.controlExchange.StopConsuming()
	coordinator.wg.Wait()
	_ = coordinator.controlExchange.Close()
}
