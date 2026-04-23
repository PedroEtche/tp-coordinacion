package aggregation

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type aggregationPublisher struct {
	outputQueue middleware.Middleware
}

func newAggregationPublisher(outputQueue middleware.Middleware) *aggregationPublisher {
	return &aggregationPublisher{outputQueue: outputQueue}
}

func (publisher *aggregationPublisher) PublishTop(clientID string, queryID uint32, fruitTopRecords []fruititem.FruitItem) error {
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

func (publisher *aggregationPublisher) Close() {
	publisher.outputQueue.Close()
}
