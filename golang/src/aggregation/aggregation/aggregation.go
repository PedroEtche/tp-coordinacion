package aggregation

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var NoClientIDError = errors.New("No client ID provided in message")

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	inputExchange     middleware.Middleware
	aggregationAmount int
	id                int
	processedTracker  *common.Tracker
	stateStore        *clientAggregationStore
	publisher         *aggregationPublisher
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	publisher := newAggregationPublisher(config.Id, outputQueue)

	return &Aggregation{
		inputExchange:     inputExchange,
		aggregationAmount: config.AggregationAmount,
		id:                config.Id,
		processedTracker:  common.NewTracker(),
		stateStore:        newClientAggregationStore(config.SumAmount, config.TopSize),
		publisher:         publisher,
	}, nil
}

func (aggregation *Aggregation) Run() {
	done := make(chan struct{})
	go aggregation.handleSignals(done)

	go aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})

	<-done // bloquea hasta SIGTERM/SIGINT
}

func (aggregation *Aggregation) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals
	slog.Info("SIGTERM signal received")

	aggregation.inputExchange.Close()
	aggregation.publisher.Close()
	close(done)
}

// -----------------------------------------------------------------------------
// INPUT QUEUE FLOW
// -----------------------------------------------------------------------------

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if !aggregation.shouldProcessClient(innerMsg) {
		return
	}

	if aggregation.processedTracker.Load(innerMsg.ClientID, innerMsg.QueryID, string(innerMsg.Type), &innerMsg.NodeID) {
		slog.Info("Desecho el mensaje", "clientID", "type", innerMsg.ClientID, innerMsg.Type)
		return
	}

	switch innerMsg.Type {
	case inner.EndOfRecords:
		if err := aggregation.handleEndOfRecordsMessage(innerMsg); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	case inner.FruitRecord:
		aggregation.handleDataMessage(innerMsg.ClientID, innerMsg.ToFruitItems())
	}
}

func (aggregation *Aggregation) shouldProcessClient(innerMsg *inner.InnerMessage) bool {
	if aggregation.aggregationAmount <= 1 || innerMsg.Type == inner.EndOfRecords {
		return true
	}

	h := fnv.New32a()
	query := fmt.Sprintf("%d_%s", innerMsg.QueryID, innerMsg.ClientID)
	h.Write([]byte(query))
	targetAggregationID := int(h.Sum32() % uint32(aggregation.aggregationAmount))
	return targetAggregationID == aggregation.id
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(innerMsg *inner.InnerMessage) error {
	slog.Info("Received End Of Records message")

	shouldBuildTop := aggregation.stateStore.RegisterEOF(innerMsg.ClientID)
	if !shouldBuildTop {
		slog.Info("Waiting for more SUM results before creating TOP", "clientID", innerMsg.ClientID)
		return nil
	}

	fruitTopRecords := aggregation.stateStore.BuildTop(innerMsg.ClientID)
	if err := aggregation.publisher.PublishTop(innerMsg.ClientID, innerMsg.QueryID, fruitTopRecords); err != nil {
		slog.Error("Publish top", "clientID", "records", innerMsg.ClientID, fruitTopRecords)
		return err
	}
	slog.Info("Publish top", "clientID", "records", innerMsg.ClientID, fruitTopRecords)

	aggregation.clearClientState(innerMsg.ClientID)

	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	aggregation.stateStore.Add(clientID, fruitRecords)
}

func (aggregation *Aggregation) clearClientState(clientID string) {
	aggregation.stateStore.Clear(clientID)
}
