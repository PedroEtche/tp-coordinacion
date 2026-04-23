package aggregation

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type aggregationPublisher struct {
	aggregationID int
	outputQueue   middleware.Middleware
}

func newAggregationPublisher(aggregationID int, outputQueue middleware.Middleware) *aggregationPublisher {
	return &aggregationPublisher{aggregationID: aggregationID, outputQueue: outputQueue}
}

func (publisher *aggregationPublisher) PublishTop(clientID string, queryID uint32, fruitTopRecords []fruititem.FruitItem) error {
	message, err := inner.SerializeFruitItemsWithID(clientID, queryID, publisher.aggregationID, fruitTopRecords)
	if err != nil {
		slog.Error("While serializing top message", "err", err)
		return err
	}
	if err := publisher.outputQueue.Send(*message); err != nil {
		slog.Error("While sending top message", "err", err)
		return err
	}
	return nil
}

// func (publisher *aggregationPublisher) PublishEOF(clientID string, queryID uint32) error {
// 	message, err := inner.SerializeEOFWithID(clientID, queryID, publisher.aggregationID)
// 	if err != nil {
// 		slog.Debug("While serializing EOF message", "err", err)
// 		return err
// 	}
//
// 	if err := publisher.outputQueue.Send(*message); err != nil {
// 		slog.Debug("While sending EOF message", "err", err)
// 		return err
// 	}
//
// 	return nil
// }

func (publisher *aggregationPublisher) Close() {
	publisher.outputQueue.Close()
}
