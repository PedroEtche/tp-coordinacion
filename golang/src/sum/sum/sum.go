package sum

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

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	Id               int
	inputQueue       middleware.Middleware
	joinInputQueue   middleware.Middleware
	processedTracker *common.Tracker
	store            *clientFruitStore
	publisher        *sumPublisher
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	joinOutoutQueue, err := middleware.CreateQueueMiddleware(config.SumPrefix, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	joinInputQueueName := fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)
	joinInputExchange, err := middleware.CreateQueueMiddleware(joinInputQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		joinOutoutQueue.Close()
		return nil, err
	}

	publisher := newSumPublisher(config.Id, joinOutoutQueue, outputExchange)

	return &Sum{
		Id:               config.Id,
		inputQueue:       inputQueue,
		joinInputQueue:   joinInputExchange,
		processedTracker: common.NewTracker(),
		store:            newClientFruitStore(),
		publisher:        publisher,
	}, nil
}

func (sum *Sum) Run() {
	done := make(chan struct{})

	go sum.handleSignals(done)
	go sum.startClientInputListener()
	go sum.startEofCoordinationListener()

	<-done // bloquea hasta SIGTERM/SIGINT
}

func (sum *Sum) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals
	slog.Info("SIGTERM signal received")

	sum.inputQueue.Close()
	sum.joinInputQueue.Close()
	sum.publisher.Close()

	close(done)
}

// -----------------------------------------------------------------------------
// CLIENT INPUT FLOW
// -----------------------------------------------------------------------------

func (sum *Sum) startClientInputListener() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleClientInputMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleClientInputMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack() // TODO: Chequear cuando habria que mandar el ack a Rabbit o si tendria que usar nack
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if sum.processedTracker.Load(innerMsg.ClientID, innerMsg.QueryID, string(innerMsg.Type), nil) {
		return
	}

	if innerMsg.Type == inner.EndOfRecords {
		if err := sum.publisher.PublishClientEOFAnnouncement(innerMsg.ClientID, innerMsg.QueryID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(innerMsg); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleDataMessage(innerMsg *inner.InnerMessage) error {
	sum.store.Add(innerMsg.ClientID, innerMsg.ToFruitItems())
	return sum.publisher.PublishQueryProcessed(innerMsg.ClientID, innerMsg.QueryID)
}

// -----------------------------------------------------------------------------
// EOF COORDINATION FLOW
// -----------------------------------------------------------------------------

func (sum *Sum) startEofCoordinationListener() {
	sum.joinInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleEofCoordinationMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleEofCoordinationMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack() // TODO: Chequear cuando habria que mandar el ack a Rabbit
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	// Si la query ya fue recibida antes, no la vuelvo a procesar
	// Esto evita retransmiciones de RabbitMQ
	if sum.processedTracker.Load(innerMsg.ClientID, innerMsg.QueryID, string(innerMsg.Type), nil) {
		return
	}

	if innerMsg.Type != inner.SafeToFlush {
		slog.Warn("Ignoring non SafeToFlush message in coordination flow", "type", innerMsg.Type)
		return
	}

	if err := sum.flushClientDataOnEof(innerMsg.ClientID, innerMsg.QueryID); err != nil {
		slog.Error("While handling end of record message", "err", err)
	}
}

func (sum *Sum) flushClientDataOnEof(clientID string, queryID uint32) error {
	slog.Info("Received End Of Records message")

	fruitItems, ok := sum.store.GetClientRecords(clientID)

	if !ok {
		slog.Info("No records received for client", "clientID", clientID)
		return sum.publisher.PublishEOF(clientID, queryID)
	}

	if err := sum.publisher.PublishFruitItems(clientID, queryID, fruitItems); err != nil {
		return err
	}

	if err := sum.publisher.PublishEOF(clientID, queryID); err != nil {
		return err
	}

	return nil
}

// -----------------------------------------------------------------------------
// HELPERS
// -----------------------------------------------------------------------------

func (sum *Sum) clearClientState(clientID string) {
	sum.store.Clear(clientID)
	sum.processedTracker.DeleteByClient(clientID)
}
