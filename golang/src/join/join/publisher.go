package join

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type joinPublisher struct {
	sumOutputQueues []middleware.Middleware
}

func newJoinPublisher(sumOutputQueues []middleware.Middleware) *joinPublisher {
	return &joinPublisher{sumOutputQueues: sumOutputQueues}
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
