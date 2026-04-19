package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type MessageHandler struct {
	clientID     uuid.UUID
	queryCounter uint32
}

func NewMessageHandler() MessageHandler {
	clientID := uuid.New()
	return MessageHandler{clientID: clientID, queryCounter: common.ClientQueryCounterStartValue}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	msg, err := inner.SerializeFruitItems(messageHandler.clientID.String(), messageHandler.queryCounter, data)
	if err != nil {
		return &middleware.Message{}, err
	}
	messageHandler.queryCounter += 1
	return msg, nil
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	msg, err := inner.SerializeEOF(messageHandler.clientID.String(), messageHandler.queryCounter)
	if err != nil {
		return &middleware.Message{}, err
	}
	messageHandler.queryCounter += 1
	return msg, nil
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	msg, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if msg.ClientID != messageHandler.clientID.String() {
		return nil, nil
	}

	return msg.ToFruitItems(), nil
}
