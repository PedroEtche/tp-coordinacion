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
	inputQueue       middleware.Middleware
	outputQueue      middleware.Middleware
	sumInputQueue    middleware.Middleware
	processedTracker *common.Tracker
	stateStore       *clientQueryStore
	publisher        *joinPublisher
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

	publisher := newJoinPublisher(sumOutputQueues)

	result := &Join{
		inputQueue:       inputQueue,
		outputQueue:      outputQueue,
		sumInputQueue:    sumInputQueue,
		processedTracker: common.NewTracker(),
		stateStore:       newClientQueryStore(),
		publisher:        publisher,
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
	join.outputQueue.Close()
	join.sumInputQueue.Close()
	join.publisher.Close()

	close(done)
}

// -----------------------------------------------------------------------------
// INPUT QUEUE FLOW
// -----------------------------------------------------------------------------

func (join *Join) handleInputQueue(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	if err := join.outputQueue.Send(msg); err != nil {
		slog.Error("While sending top", "err", err)
	}
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

	if join.processedTracker.Load(innerMsg.ClientID, innerMsg.QueryID, string(innerMsg.Type), nil) {
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

		join.clearClientState(innerMsg.ClientID)
	}
}

func (join *Join) clearClientState(clientID string) {
	join.stateStore.Clear(clientID)
}
