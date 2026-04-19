package join

import (
	"fmt"
	"log/slog"

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
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	sumInputQueue     middleware.Middleware
	sumOutputExchange middleware.Middleware
	queriesProcessed  map[string]map[uint32]struct{}
	clientsWaiting    map[string]uint32
	processedMessages map[string]struct{}
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

	sumOutputExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, common.ExchangeKey, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		sumInputQueue.Close()
		return nil, err
	}

	result := &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		sumInputQueue:     sumInputQueue,
		sumOutputExchange: sumOutputExchange,
		queriesProcessed:  make(map[string]map[uint32]struct{}),
		clientsWaiting:    make(map[string]uint32),
		processedMessages: make(map[string]struct{}),
	}

	return result, nil
}

func (join *Join) Run() {
	go join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleInputQueue(msg, ack, nack)
	})

	go join.sumInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleSumQueries(msg, ack, nack)
	})

	select {} // TODO: Meter un sigterm
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
	defer ack() // TODO: Chequear cuando habria que mandar el ack a Rabbit o si tendria que usar nack
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if join.sumMessageAlreadyProcessed(innerMsg.ClientID, innerMsg.QueryID, innerMsg.Type) {
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
	if _, ok := join.queriesProcessed[innerMsg.ClientID]; !ok {
		join.queriesProcessed[innerMsg.ClientID] = make(map[uint32]struct{})
	}
	join.queriesProcessed[innerMsg.ClientID][innerMsg.QueryID] = struct{}{}

	// Si ya se recibio el mensaje de EOF del cliente y con esta query se procesaron
	// todas las del cliente entonces hay que hacerle flush a sus datos
	if processedQueries, ok := join.clientsWaiting[innerMsg.ClientID]; ok && len(join.queriesProcessed[innerMsg.ClientID]) == int(processedQueries) {
		// Publish using the EOF query id (last processed query id + 1),
		// independent from the order in which SUM and EOF messages arrive.
		if err := join.publishSafeToFlush(innerMsg.ClientID, processedQueries+1); err != nil {
			slog.Error("While publishing safe to flush message", "err", err)
			return
		}
	}
}

func (join *Join) handleEndOfRecordsFromSum(innerMsg *inner.InnerMessage) {
	if innerMsg.QueryID == common.ClientQueryCounterStartValue {
		slog.Info("Client send EOF without sending any records")
		if err := join.publishSafeToFlush(innerMsg.ClientID, innerMsg.QueryID); err != nil {
			slog.Error("While publishing safe to flush message", "err", err)
			return
		}
		return
	}

	lastFruitQuery := innerMsg.QueryID - 1
	join.clientsWaiting[innerMsg.ClientID] = lastFruitQuery

	clientProcessedQueries, ok := join.queriesProcessed[innerMsg.ClientID]
	if !ok {
		// Puede ser que llego el EOF de un cliente antes que otros mensajes del mismo
		return
	}

	if len(clientProcessedQueries) == int(lastFruitQuery) {
		if err := join.publishSafeToFlush(innerMsg.ClientID, innerMsg.QueryID); err != nil {
			slog.Error("While publishing safe to flush message", "err", err)
			return
		}
	}
}

func (join *Join) publishSafeToFlush(clientID string, queryID uint32) error {
	msg, err := inner.SerializeSafeToFlush(clientID, queryID)
	if err != nil {
		return err
	}

	if err := join.sumOutputExchange.Send(*msg); err != nil {
		return err
	}

	// join.clearClientState(clientID)
	return nil
}

func (join *Join) sumMessageAlreadyProcessed(clientID string, queryID uint32, msgType inner.MsgType) bool {
	key := fmt.Sprintf("%s_%d_%s", clientID, queryID, msgType)
	if _, ok := join.processedMessages[key]; ok {
		return true
	}

	join.processedMessages[key] = struct{}{}
	return false
}

func (join *Join) clearClientState(clientID string) {
	delete(join.queriesProcessed, clientID)
	delete(join.clientsWaiting, clientID)

	prefix := fmt.Sprintf("%s_", clientID)
	for key := range join.processedMessages {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(join.processedMessages, key)
		}
	}
}
