package sum

import (
	"fmt"
	"log/slog"

	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
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
	Id                  int
	inputQueue          middleware.Middleware
	aggregationExchange middleware.Middleware
	joinOutputQueue     middleware.Middleware
	joinInputExchange   middleware.Middleware
	fruitItemMap        map[string]map[string]fruititem.FruitItem
	fruitItemMapMu      sync.RWMutex
	solvedQueries       sync.Map // map[string]bool,
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

	joinInputExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, common.ExchangeKey, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		joinOutoutQueue.Close()
		return nil, err
	}

	return &Sum{
		Id:                  config.Id,
		inputQueue:          inputQueue,
		aggregationExchange: outputExchange,
		joinOutputQueue:     joinOutoutQueue,
		joinInputExchange:   joinInputExchange,
		fruitItemMap:        map[string]map[string]fruititem.FruitItem{},
		solvedQueries:       sync.Map{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.startClientInputListener()
	go sum.startEofCoordinationListener()

	select {} // TODO: Agregar control de sigterm para que se termine el proceso y se liberen los recursos
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

	if sum.queryAlredyReceived(innerMsg.ClientID, innerMsg.QueryID, innerMsg.Type) {
		return
	}

	if innerMsg.Type == inner.EndOfRecords {
		if err := sum.publishClientEofAnnouncement(innerMsg.ClientID, innerMsg.QueryID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(innerMsg); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleDataMessage(innerMsg *inner.InnerMessage) error {
	sum.fruitItemMapMu.Lock()
	defer sum.fruitItemMapMu.Unlock()

	if _, ok := sum.fruitItemMap[innerMsg.ClientID]; !ok {
		sum.fruitItemMap[innerMsg.ClientID] = make(map[string]fruititem.FruitItem)
	}

	for _, fruitRecord := range innerMsg.ToFruitItems() {
		_, ok := sum.fruitItemMap[innerMsg.ClientID][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[innerMsg.ClientID][fruitRecord.Fruit] = sum.fruitItemMap[innerMsg.ClientID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[innerMsg.ClientID][fruitRecord.Fruit] = fruitRecord
		}
	}

	msg, err := inner.SerializeQueryProcessed(innerMsg.ClientID, innerMsg.QueryID)
	if err != nil {
		return err
	}

	if err := sum.joinOutputQueue.Send(*msg); err != nil {
		return err
	}

	return nil
}

func (sum *Sum) publishClientEofAnnouncement(clientID string, queryID uint32) error {
	slog.Info("Received End Of Records message")

	message, err := inner.SerializeEOF(clientID, queryID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}

	if err := sum.joinOutputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message to JOIN", "err", err)
		return err
	}

	return nil
}

// -----------------------------------------------------------------------------
// EOF COORDINATION FLOW
// -----------------------------------------------------------------------------

func (sum *Sum) startEofCoordinationListener() {
	sum.joinInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
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
	if sum.queryAlredyReceived(innerMsg.ClientID, innerMsg.QueryID, innerMsg.Type) {
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

	sum.fruitItemMapMu.RLock()
	fruitMap, ok := sum.fruitItemMap[clientID]
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitMap))
	if ok {
		for _, item := range fruitMap {
			fruitItems = append(fruitItems, item)
		}
	}
	sum.fruitItemMapMu.RUnlock()

	if !ok {
		slog.Info("No records received for client", "clientID", clientID)
		return sum.sendoEofToOutputExchange(clientID, queryID)
	}

	message, err := inner.SerializeFruitItemsFromSum(clientID, queryID, sum.Id, fruitItems)
	if err != nil {
		slog.Debug("While serializing message", "err", err)
		return err
	}
	if err := sum.aggregationExchange.Send(*message); err != nil {
		slog.Debug("While sending message", "err", err)
		return err
	}

	if err := sum.sendoEofToOutputExchange(clientID, queryID); err != nil {
		return err
	}

	// sum.clearClientState(clientID)
	return nil
}

func (sum *Sum) sendoEofToOutputExchange(clientID string, queryID uint32) error {
	message, err := inner.SerializeEOFFromSum(clientID, queryID, sum.Id)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.aggregationExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

// -----------------------------------------------------------------------------
// HELPERS
// -----------------------------------------------------------------------------

func (sum *Sum) queryAlredyReceived(clientID string, queryID uint32, msgType inner.MsgType) bool {
	queryKey := fmt.Sprintf("%s_%d_%s", clientID, queryID, msgType)

	// Si la query ya fue recibida antes, no la vuelvo a procesar
	// Esto evita retransmiciones de RabbitMQ
	if _, ok := sum.solvedQueries.Load(queryKey); ok {
		return true
	} else {
		sum.solvedQueries.Store(queryKey, true)
	}
	return false
}

func (sum *Sum) clearClientState(clientID string) {
	sum.fruitItemMapMu.Lock()
	delete(sum.fruitItemMap, clientID)
	sum.fruitItemMapMu.Unlock()

	prefix := fmt.Sprintf("%s_", clientID)
	sum.solvedQueries.Range(func(key, _ any) bool {
		keyStr, ok := key.(string)
		if ok && len(keyStr) >= len(prefix) && keyStr[:len(prefix)] == prefix {
			sum.solvedQueries.Delete(keyStr)
		}
		return true
	})
}
