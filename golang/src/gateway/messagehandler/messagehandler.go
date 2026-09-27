package messagehandler

import (
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var globalClientID uint64

type MessageHandler struct {
	clientID  uint64
	sentCount uint64
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		clientID:  atomic.AddUint64(&globalClientID, 1),
		sentCount: 0,
	}
}
func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	messageHandler.sentCount++
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.clientID, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOFMessage(messageHandler.clientID, messageHandler.sentCount)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientID, fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientID != messageHandler.clientID {
		return nil, nil
	}

	return fruitRecords, nil
}
