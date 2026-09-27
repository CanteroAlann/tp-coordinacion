package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type PacketType string

const (
	PacketTypeData PacketType = "DATA"
	PacketTypeEOF  PacketType = "EOF"
)

type Packet struct {
	Type      PacketType            `json:"type"`
	ClientID  uint64                `json:"client_id"`
	Records   []fruititem.FruitItem `json:"records,omitempty"`
	TotalMsgs uint64                `json:"total_msgs,omitempty"`
}

func (p Packet) IsEOF() bool {
	return p.Type == PacketTypeEOF
}

func SerializeMessage(clientID uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	packet := Packet{
		Type:     PacketTypeData,
		ClientID: clientID,
		Records:  fruitRecords,
	}

	body, err := json.Marshal(packet)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeEOFMessage(clientID uint64, totalMsgs uint64) (*middleware.Message, error) {
	packet := Packet{
		Type:      PacketTypeEOF,
		ClientID:  clientID,
		Records:   []fruititem.FruitItem{},
		TotalMsgs: totalMsgs,
	}

	body, err := json.Marshal(packet)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializePacket(message *middleware.Message) (Packet, error) {
	var packet Packet
	if err := json.Unmarshal([]byte(message.Body), &packet); err != nil {
		return Packet{}, err
	}

	if packet.Type == "" {
		if len(packet.Records) == 0 {
			packet.Type = PacketTypeEOF
		} else {
			packet.Type = PacketTypeData
		}
	}

	return packet, nil
}

func DeserializeMessage(message *middleware.Message) (uint64, []fruititem.FruitItem, bool, error) {
	packet, err := DeserializePacket(message)
	if err != nil {
		return 0, nil, false, err
	}
	return packet.ClientID, packet.Records, packet.IsEOF(), nil
}
