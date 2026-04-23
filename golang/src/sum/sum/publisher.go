package sum

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type sumPublisher struct {
	sumID               int
	joinOutputQueue     middleware.Middleware
	aggregationExchange middleware.Middleware
}

func newSumPublisher(sumID int, joinOutputQueue middleware.Middleware, aggregationExchange middleware.Middleware) *sumPublisher {
	return &sumPublisher{
		sumID:               sumID,
		joinOutputQueue:     joinOutputQueue,
		aggregationExchange: aggregationExchange,
	}
}

func (publisher *sumPublisher) PublishQueryProcessed(clientID string, queryID uint32) error {
	msg, err := inner.SerializeQueryProcessed(clientID, queryID)
	if err != nil {
		return err
	}
	return publisher.joinOutputQueue.Send(*msg)
}

func (publisher *sumPublisher) PublishClientEOFAnnouncement(clientID string, queryID uint32) error {
	slog.Info("Received End Of Records message")

	message, err := inner.SerializeEOF(clientID, queryID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}

	if err := publisher.joinOutputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message to JOIN", "err", err)
		return err
	}

	return nil
}

func (publisher *sumPublisher) PublishFruitItems(clientID string, queryID uint32, fruitItems []fruititem.FruitItem) error {
	message, err := inner.SerializeFruitItemsFromSum(clientID, queryID, publisher.sumID, fruitItems)
	if err != nil {
		slog.Debug("While serializing message", "err", err)
		return err
	}
	if err := publisher.aggregationExchange.Send(*message); err != nil {
		slog.Debug("While sending message", "err", err)
		return err
	}
	return nil
}

func (publisher *sumPublisher) PublishEOF(clientID string, queryID uint32) error {
	message, err := inner.SerializeEOFFromSum(clientID, queryID, publisher.sumID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := publisher.aggregationExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (publisher *sumPublisher) Close() {
	publisher.joinOutputQueue.Close()
	publisher.aggregationExchange.Close()
}
