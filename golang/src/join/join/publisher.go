package join

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type joinPublisher struct {
	sumOutputExchange middleware.Middleware
}

func newJoinPublisher(sumOutputExchange middleware.Middleware) *joinPublisher {
	return &joinPublisher{sumOutputExchange: sumOutputExchange}
}

func (publisher *joinPublisher) PublishSafeToFlush(clientID string, queryID uint32) error {
	msg, err := inner.SerializeSafeToFlush(clientID, queryID)
	if err != nil {
		return err
	}

	if err := publisher.sumOutputExchange.Send(*msg); err != nil {
		return err
	}

	return nil
}
