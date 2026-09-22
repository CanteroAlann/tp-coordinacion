package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type Packet struct {
	ClientID uint64                `json:"client_id"`
	Records  []fruititem.FruitItem `json:"records"`
}

func SerializeMessage(clientID uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	packet := Packet{
		ClientID: clientID,
		Records:  fruitRecords,
	}

	body, err := json.Marshal(packet)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, bool, error) {
	var packet Packet
	if err := json.Unmarshal([]byte(message.Body), &packet); err != nil {
		return 0, nil, false, err
	}

	isEOF := len(packet.Records) == 0
	return packet.ClientID, packet.Records, isEOF, nil
}
