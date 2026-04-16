package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var PubSubExchangeKey = []string{"el_cliente_sape"} // TODO: Eligir un mejor nombre (aunque sea dificil)

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
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	pubExchange    middleware.Middleware
	subExchange    middleware.Middleware
	fruitItemMap   map[string]map[string]fruititem.FruitItem
	solvedQueries  map[string]bool
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

	pubExchange, err := middleware.CreateExchangeMiddleware("clientFIN", PubSubExchangeKey, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	subExchange, err := middleware.CreateExchangeMiddleware("clientFIN", PubSubExchangeKey, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		pubExchange.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		pubExchange:    pubExchange,
		subExchange:    subExchange,
		fruitItemMap:   map[string]map[string]fruititem.FruitItem{},
		solvedQueries:  make(map[string]bool),
	}, nil
}

func (sum *Sum) Run() {
	announcement, err := inner.SerializeNewSum()
	if err != nil {
		slog.Debug("While creating announcement message", "err", err)
		return
	}
	err = sum.outputExchange.Send(*announcement)
	if err != nil {
		slog.Debug("While sending announcement message to Aggregators", "err", err)
		return
	}
	slog.Info("Sum announce send correctly")

	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessageFromInputQueue(msg, ack, nack)
	})

	go sum.subExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessageFromExchange(msg, ack, nack)
	})

	select {} // TODO: Agregar control de sigterm para que se termine el proceso y se liberen los recursos
}

func (sum *Sum) handleMessageFromInputQueue(msg middleware.Message, ack func(), nack func()) {
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	// Si la query ya fue recibida antes, no la vuelvo a procesar
	// Esto evita retransmiciones de RabbitMQ
	if _, ok := sum.solvedQueries[innerMsg.QueryID]; ok {
		return
	} else {
		sum.solvedQueries[innerMsg.QueryID] = true
	}
	defer ack() // TODO: Chequear cuando habria que mandar el ack a Rabbit o si tendria que usar nack

	if innerMsg.Type == inner.EndOfRecords {
		if err := sum.handleEofFromInputQueue(innerMsg.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(innerMsg.ClientID, innerMsg.ToFruitItems()); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) error {
	if _, ok := sum.fruitItemMap[clientID]; !ok {
		sum.fruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
	}

	for _, fruitRecord := range fruitRecords {
		_, ok := sum.fruitItemMap[clientID][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[clientID][fruitRecord.Fruit] = sum.fruitItemMap[clientID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[clientID][fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func (sum *Sum) handleEofFromInputQueue(clientID string) error {
	slog.Info("Received End Of Records message")

	message, err := inner.SerializeEOF(clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.pubExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	return nil
}

func (sum *Sum) handleMessageFromExchange(msg middleware.Message, ack func(), nack func()) {
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	// Si la query ya fue recibida antes, no la vuelvo a procesar
	// Esto evita retransmiciones de RabbitMQ
	if _, ok := sum.solvedQueries[innerMsg.QueryID]; ok {
		return
	} else {
		sum.solvedQueries[innerMsg.QueryID] = true
	}
	defer ack() // TODO: Chequear cuando habria que mandar el ack a Rabbit

	if innerMsg.Type != inner.EndOfRecords {
		slog.Error("Found a not EOF message in the EOF exchange")
		return
	}
	if err := sum.handleEndOfRecordMessageFromExchange(innerMsg.ClientID); err != nil {
		slog.Error("While handling end of record message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessageFromExchange(clientID string) error {
	slog.Info("Received End Of Records message")

	fruitMap, ok := sum.fruitItemMap[clientID]
	if !ok {
		slog.Info("No records received for client", "clientID", clientID)
		return sum.sendoEofToOutputExchange(clientID)
	}

	for _, item := range fruitMap {
		fruitRecord := []fruititem.FruitItem{item}
		message, err := inner.SerializeFruitItems(clientID, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	return sum.sendoEofToOutputExchange(clientID)
}

func (sum *Sum) sendoEofToOutputExchange(clientID string) error {
	message, err := inner.SerializeEOF(clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}
