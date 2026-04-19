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
	SumAmount         int
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
		SumAmount:         config.SumAmount,
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

	if aggregation.queryAlredyReceived(innerMsg.ClientID, innerMsg.QueryID, innerMsg.Type, innerMsg.SumID) {
		return
	}

	defer ack()

	switch innerMsg.Type {
	case inner.EndOfRecords:
		if err := aggregation.handleEndOfRecordsMessage(innerMsg); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	case inner.FruitRecord:
		aggregation.handleDataMessage(innerMsg.ClientID, innerMsg.ToFruitItems())
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(innerMsg *inner.InnerMessage) error {
	slog.Info("Received End Of Records message")

	if _, ok := aggregation.clientSumsCounter[innerMsg.ClientID]; !ok {
		aggregation.clientSumsCounter[innerMsg.ClientID] = 0
	}

	aggregation.clientSumsCounter[innerMsg.ClientID] += 1

	if aggregation.clientSumsCounter[innerMsg.ClientID] != aggregation.SumAmount {
		slog.Info("Waiting for more SUM results before creating TOP", "clientID", innerMsg.ClientID, "currentSums", aggregation.clientSumsCounter[innerMsg.ClientID], "expectedSums", aggregation.SumAmount)
		return nil

	}

	fruitTopRecords := []fruititem.FruitItem{}
	aggregation.buildFruitTop(innerMsg.ClientID, &fruitTopRecords)

	message, err := inner.SerializeFruitItems(innerMsg.ClientID, innerMsg.QueryID+1, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	// aggregation.clearClientState(innerMsg.ClientID)

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

func (aggregation *Aggregation) buildFruitTop(clientID string, fruitTopRecords *[]fruititem.FruitItem) {
	clientFruitMap, ok := aggregation.fruitItemMap[clientID]
	if !ok {
		slog.Info("No records received for client; publishing empty TOP", "clientID", clientID)
		*fruitTopRecords = []fruititem.FruitItem{}
		return
	}

	slog.Info("Creating Fruit TOP")
	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruitMap))
	for _, item := range clientFruitMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	// NOTE: Esto probablemente seria mas eficiente usando un algoritmo Top K
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	*fruitTopRecords = fruitItems[:finalTopSize]
}

func (aggregation *Aggregation) queryAlredyReceived(clientID string, queryID uint32, msgType inner.MsgType, sumID int) bool {
	queryKey := fmt.Sprintf("%s_%d_%s_%d", clientID, queryID, msgType, sumID)

	// Si la query ya fue recibida antes, no la vuelvo a procesar
	// Esto evita retransmiciones de RabbitMQ
	if _, ok := aggregation.solvedQueries[queryKey]; ok {
		return true
	} else {
		aggregation.solvedQueries[queryKey] = true
	}

	return false
}

func (aggregation *Aggregation) clearClientState(clientID string) {
	delete(aggregation.fruitItemMap, clientID)
	delete(aggregation.clientSumsCounter, clientID)

	prefix := fmt.Sprintf("%s_", clientID)
	for key := range aggregation.solvedQueries {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(aggregation.solvedQueries, key)
		}
	}
}
