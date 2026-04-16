package aggregation

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"

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
	outputQueue       middleware.Middleware
	inputExchange     middleware.Middleware
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	clientSumsCounter map[string]int
	topSize           int
	activeSums        int
	solvedQueries     map[string]bool
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

	return &Aggregation{
		outputQueue:       outputQueue,
		inputExchange:     inputExchange,
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		topSize:           config.TopSize,
		activeSums:        0,
		clientSumsCounter: make(map[string]int),
		solvedQueries:     map[string]bool{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	// Si la query ya fue recibida antes, no la vuelvo a procesar
	// Esto evita retransmiciones de RabbitMQ
	if _, ok := aggregation.solvedQueries[innerMsg.QueryID]; ok {
		return
	} else {
		aggregation.solvedQueries[innerMsg.QueryID] = true
	}

	defer ack()

	switch innerMsg.Type {
	case inner.NewSum:
		aggregation.activeSums += 1
		slog.Info("Received announcement of new Sum Entity")
	case inner.EndOfRecords:
		if err := aggregation.handleEndOfRecordsMessage(innerMsg.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	case inner.FruitRecord:
		aggregation.handleDataMessage(innerMsg.ClientID, innerMsg.ToFruitItems())
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID string) error {
	slog.Info("Received End Of Records message")

	if err := aggregation.clientHealthCheck(clientID); err != nil {
		return err
	}

	aggregation.clientSumsCounter[clientID] += 1

	if aggregation.clientSumsCounter[clientID] != aggregation.activeSums {
		slog.Info("Todavia se neceita mas registros del cliente antes de hacer el top")
		return nil

	}

	fruitTopRecords := aggregation.buildFruitTop(clientID)
	message, err := inner.SerializeFruitItems(clientID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	return nil
}

func (aggregation *Aggregation) clientHealthCheck(clientID string) error {
	if _, ok := aggregation.clientSumsCounter[clientID]; !ok {
		slog.Debug("Client has no register", "clientID", clientID)
		return NoClientIDError
	}
	if _, ok := aggregation.fruitItemMap[clientID]; !ok {
		slog.Debug("No records received for client", "clientID", clientID)
		return NoClientIDError
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	if _, ok := aggregation.fruitItemMap[clientID]; !ok {
		aggregation.fruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
		aggregation.clientSumsCounter[clientID] = 0
	}

	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMap[clientID][fruitRecord.Fruit]; ok {
			aggregation.fruitItemMap[clientID][fruitRecord.Fruit] = aggregation.fruitItemMap[clientID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[clientID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID string) []fruititem.FruitItem {
	slog.Info("Creating Fruit TOP")
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap[clientID]))
	for _, item := range aggregation.fruitItemMap[clientID] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	// NOTE: Esto probablemente seria mas eficiente usando un algoritmo Top K
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
