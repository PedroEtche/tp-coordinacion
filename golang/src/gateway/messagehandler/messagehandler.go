package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type MessageHandler struct {
	clientID uuid.UUID
}

func NewMessageHandler() MessageHandler {
	clientID := uuid.New()
	return MessageHandler{clientID: clientID}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeFruitItems(messageHandler.clientID.String(), data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOF(messageHandler.clientID.String())
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
