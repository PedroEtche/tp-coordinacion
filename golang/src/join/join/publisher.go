package join

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type joinPublisher struct {
	sumOutputQueues []middleware.Middleware
	outputQueue     middleware.Middleware
}

func newJoinPublisher(sumOutputQueues []middleware.Middleware, outputQueue middleware.Middleware) *joinPublisher {
	return &joinPublisher{sumOutputQueues: sumOutputQueues, outputQueue: outputQueue}
}

func (publisher *joinPublisher) PublishSafeToFlush(clientID string, queryID uint32) error {
	msg, err := inner.SerializeSafeToFlush(clientID, queryID)
	if err != nil {
		return err
	}

	for _, queue := range publisher.sumOutputQueues {
		if err := queue.Send(*msg); err != nil {
			return err
		}
	}

	return nil
}

func (publisher *joinPublisher) Close() {
	for _, queue := range publisher.sumOutputQueues {
		queue.Close()
	}
}

func (publisher *joinPublisher) PublishTop(clientID string, queryID uint32, fruitTopRecords []fruititem.FruitItem) error {
	message, err := inner.SerializeFruitItems(clientID, queryID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := publisher.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}
	return nil
}
