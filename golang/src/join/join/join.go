package join

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue                 middleware.Middleware
	sumInputQueue              middleware.Middleware
	sumProcessedTracker        *common.Tracker
	aggregatorProcessedTracker *common.Tracker
	stateStore                 *queryStore
	publisher                  *joinPublisher
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	sumInputQueue, err := middleware.CreateQueueMiddleware(config.SumPrefix, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	sumOutputQueues := make([]middleware.Middleware, 0, config.SumAmount)
	for i := range config.SumAmount {
		sumQueueName := fmt.Sprintf("%s_%d", config.SumPrefix, i)
		sumOutputQueue, queueErr := middleware.CreateQueueMiddleware(sumQueueName, connSettings)
		if queueErr != nil {
			inputQueue.Close()
			outputQueue.Close()
			sumInputQueue.Close()
			for _, q := range sumOutputQueues {
				q.Close()
			}
			return nil, queueErr
		}

		sumOutputQueues = append(sumOutputQueues, sumOutputQueue)
	}

	publisher := newJoinPublisher(sumOutputQueues, outputQueue)

	result := &Join{
		inputQueue:                 inputQueue,
		sumInputQueue:              sumInputQueue,
		sumProcessedTracker:        common.NewTracker(),
		aggregatorProcessedTracker: common.NewTracker(),
		stateStore:                 newClientQueryStore(config.AggregationAmount, config.TopSize),
		publisher:                  publisher,
	}

	return result, nil
}

func (join *Join) Run() {
	done := make(chan struct{})
	go join.handleSignals(done)

	go join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleInputQueue(msg, ack, nack)
	})

	go join.sumInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleSumQueries(msg, ack, nack)
	})

	<-done // bloquea hasta SIGTERM/SIGINT
}

func (join *Join) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals
	slog.Info("SIGTERM signal received")

	join.inputQueue.Close()
	join.sumInputQueue.Close()
	join.publisher.Close()

	close(done)
}

// -----------------------------------------------------------------------------
// INPUT QUEUE FLOW
// -----------------------------------------------------------------------------

func (join *Join) handleInputQueue(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if join.aggregatorProcessedTracker.Load(innerMsg.ClientID, innerMsg.QueryID, string(innerMsg.Type), &innerMsg.NodeID) {
		return
	}

	if innerMsg.Type == inner.FruitRecord {
		join.handleTopMessage(innerMsg)
		return
	}
	slog.Error("Unknown message type", "type", innerMsg.Type)
}

func (join *Join) handleTopMessage(innerMsg *inner.InnerMessage) error {
	slog.Info("Received partial top message")

	join.stateStore.AddPartialTop(innerMsg.ClientID, innerMsg.ToFruitItems())

	if !join.stateStore.RegisterAggregationEOF(innerMsg.ClientID, innerMsg.NodeID) {
		slog.Info("Waiting for more Aggregators data before flushing", "clientID", innerMsg.ClientID)
		return nil

	}

	fruitTopRecords := join.stateStore.BuildTop(innerMsg.ClientID)
	if err := join.publisher.PublishTop(innerMsg.ClientID, innerMsg.QueryID+1, fruitTopRecords); err != nil {
		return err
	}
	slog.Info("Publish top", "clientID", "records", innerMsg.ClientID, fruitTopRecords)

	join.clearClientState(innerMsg.ClientID)

	return nil
}

// -----------------------------------------------------------------------------
// SUM QUERIES FLOW
// -----------------------------------------------------------------------------

func (join *Join) handleSumQueries(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if join.sumProcessedTracker.Load(innerMsg.ClientID, innerMsg.QueryID, string(innerMsg.Type), nil) {
		return
	}

	switch innerMsg.Type {
	case inner.SumQueryProcessed:
		join.handleSumProcessedQuery(innerMsg)
	case inner.EndOfRecords:
		join.handleEndOfRecordsFromSum(innerMsg)
	}
}

func (join *Join) handleSumProcessedQuery(innerMsg *inner.InnerMessage) {
	flushQueryID, shouldFlush := join.stateStore.RegisterQuery(innerMsg.ClientID, innerMsg.QueryID)
	if shouldFlush {
		if err := join.publisher.PublishSafeToFlush(innerMsg.ClientID, flushQueryID); err != nil {
			slog.Error("While publishing safe to flush message", "err", err)
			return
		}
	}
}

func (join *Join) handleEndOfRecordsFromSum(innerMsg *inner.InnerMessage) {
	flushQueryID, shouldFlush, emptyClient := join.stateStore.RegisterEOF(innerMsg.ClientID, innerMsg.QueryID)
	if emptyClient {
		slog.Info("Client send EOF without sending any records")
	}
	if shouldFlush {
		if err := join.publisher.PublishSafeToFlush(innerMsg.ClientID, flushQueryID); err != nil {
			slog.Error("While publishing safe to flush message", "err", err)
			return
		}

		join.clearClientSumState(innerMsg.ClientID)
	}
}

func (join *Join) clearClientSumState(clientID string) {
	join.stateStore.ClearSumCoordination(clientID)
}

func (join *Join) clearClientState(clientID string) {
	join.stateStore.Clear(clientID)
}
