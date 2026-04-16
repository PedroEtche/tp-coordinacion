package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type MsgType string

const (
	FruitRecord  MsgType = "fruit_record"
	EndOfRecords MsgType = "eof"
	NewSum       MsgType = "new_queue"
)

type Record struct {
	Fruit  string `json:"fruit"`
	Amount uint32 `json:"amount"`
}

type InnerMessage struct {
	QueryID  string   `json:"query_id"`
	ClientID string   `json:"client_id"`
	Records  []Record `json:"records"`
	Type     MsgType  `json:"type"`
}

func SerializeFruitItems(clientID string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	records := make([]Record, 0, len(fruitRecords))
	for _, fr := range fruitRecords {
		records = append(records, Record{
			Fruit:  fr.Fruit,
			Amount: fr.Amount,
		})
	}

	msg := InnerMessage{
		QueryID:  uuid.New().String(),
		ClientID: clientID,
		Records:  records,
		Type:     FruitRecord,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeEOF(clientID string) (*middleware.Message, error) {
	msg := InnerMessage{
		QueryID:  uuid.New().String(),
		ClientID: clientID,
		Records:  []Record{},
		Type:     EndOfRecords,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeNewSum() (*middleware.Message, error) {
	msg := InnerMessage{
		QueryID:  uuid.New().String(),
		ClientID: "",
		Records:  []Record{},
		Type:     NewSum,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessage(message *middleware.Message) (*InnerMessage, error) {
	var msg InnerMessage

	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}

	if msg.Type == EndOfRecords && len(msg.Records) > 0 {
		return nil, errors.New("EOF message should not contain records")
	}

	return &msg, nil
}

func (m *InnerMessage) ToFruitItems() []fruititem.FruitItem {
	items := make([]fruititem.FruitItem, 0, len(m.Records))
	for _, r := range m.Records {
		items = append(items, fruititem.FruitItem{
			Fruit:  r.Fruit,
			Amount: r.Amount,
		})
	}
	return items
}
