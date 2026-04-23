package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MsgType string

const (
	FruitRecord       MsgType = "fruit_record"
	EndOfRecords      MsgType = "eof"
	SafeToFlush       MsgType = "flush"
	SumQueryProcessed MsgType = "sum_query_processed"
)

type Record struct {
	Fruit  string `json:"fruit"`
	Amount uint32 `json:"amount"`
}

type InnerMessage struct {
	QueryID  uint32   `json:"query_id"`
	ClientID string   `json:"client_id"`
	NodeID   int      `json:"sum_id,omitempty"`
	Records  []Record `json:"records"`
	Type     MsgType  `json:"type"`
}

func NewInnerMessage(clientID string, queryID uint32, msgType MsgType, sumID *int, records []fruititem.FruitItem) *InnerMessage {
	msg := &InnerMessage{
		QueryID:  queryID,
		ClientID: clientID,
		Records:  createRecords(records),
		Type:     msgType,
	}

	if sumID != nil {
		msg.NodeID = *sumID
	}

	return msg
}

func (m *InnerMessage) Validate() error {
	switch m.Type {
	case EndOfRecords, SafeToFlush, SumQueryProcessed:
		if len(m.Records) > 0 {
			return errors.New("control message should not contain records")
		}
	case FruitRecord:
	// No hay validaciones por el momento

	default:
		return errors.New("unknown message type")
	}

	if m.ClientID == "" {
		return errors.New("client_id is required")
	}

	return nil
}

func SerializeFruitItems(clientID string, queryID uint32, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	msg := NewInnerMessage(clientID, queryID, FruitRecord, nil, fruitRecords)

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeFruitItemsWithID(clientID string, queryID uint32, sumID int, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	msg := NewInnerMessage(clientID, queryID, FruitRecord, &sumID, fruitRecords)

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeEOF(clientID string, queryID uint32) (*middleware.Message, error) {
	msg := NewInnerMessage(clientID, queryID, EndOfRecords, nil, []fruititem.FruitItem{})

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeEOFWithID(clientID string, queryID uint32, sumID int) (*middleware.Message, error) {
	msg := NewInnerMessage(clientID, queryID, EndOfRecords, &sumID, []fruititem.FruitItem{})

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeQueryProcessed(clientID string, queryID uint32) (*middleware.Message, error) {
	msg := NewInnerMessage(clientID, queryID, SumQueryProcessed, nil, []fruititem.FruitItem{})

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func SerializeSafeToFlush(clientID string, queryID uint32) (*middleware.Message, error) {
	msg := NewInnerMessage(clientID, queryID, SafeToFlush, nil, []fruititem.FruitItem{})

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

	if err := msg.Validate(); err != nil {
		return nil, err
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

// -----------------------------
// HELPERS
// -----------------------------

func createRecords(fruitRecords []fruititem.FruitItem) []Record {
	records := make([]Record, 0, len(fruitRecords))
	for _, fr := range fruitRecords {
		records = append(records, Record{
			Fruit:  fr.Fruit,
			Amount: fr.Amount,
		})
	}
	return records
}
