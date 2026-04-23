package join

import (
	"sort"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type queryStore struct {
	mu                sync.RWMutex
	queriesProcessed  map[string]map[uint32]struct{}
	clientsWaiting    map[string]uint32
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	aggregationEOFs   map[string]map[int]struct{}
	aggregationAmount int
	topSize           int
}

func newClientQueryStore(aggregationAmount int, topSize int) *queryStore {
	return &queryStore{
		queriesProcessed:  make(map[string]map[uint32]struct{}),
		clientsWaiting:    make(map[string]uint32),
		fruitItemMap:      make(map[string]map[string]fruititem.FruitItem),
		aggregationEOFs:   make(map[string]map[int]struct{}),
		aggregationAmount: aggregationAmount,
		topSize:           topSize,
	}
}

func (store *queryStore) RegisterQuery(clientID string, queryID uint32) (uint32, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, ok := store.queriesProcessed[clientID]; !ok {
		store.queriesProcessed[clientID] = make(map[uint32]struct{})
	}
	store.queriesProcessed[clientID][queryID] = struct{}{}

	lastFruitQuery, waiting := store.clientsWaiting[clientID]
	if !waiting {
		return 0, false
	}

	if len(store.queriesProcessed[clientID]) != int(lastFruitQuery) {
		return 0, false
	}

	return lastFruitQuery + 1, true
}

func (store *queryStore) RegisterEOF(clientID string, queryID uint32) (flushQueryID uint32, shouldFlush bool, emptyClient bool) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if queryID == common.ClientQueryCounterStartValue {
		return queryID, true, true
	}

	lastFruitQuery := queryID - 1
	store.clientsWaiting[clientID] = lastFruitQuery

	clientProcessedQueries, ok := store.queriesProcessed[clientID]
	if !ok {
		return 0, false, false
	}

	if len(clientProcessedQueries) == int(lastFruitQuery) {
		return queryID, true, false
	}

	return 0, false, false
}

func (store *queryStore) AddPartialTop(clientID string, records []fruititem.FruitItem) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, ok := store.fruitItemMap[clientID]; !ok {
		store.fruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
	}

	for _, record := range records {
		if current, ok := store.fruitItemMap[clientID][record.Fruit]; ok {
			store.fruitItemMap[clientID][record.Fruit] = current.Sum(record)
			continue
		}

		store.fruitItemMap[clientID][record.Fruit] = record
	}
}

func (store *queryStore) RegisterAggregationEOF(clientID string, aggregationID int) bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, ok := store.aggregationEOFs[clientID]; !ok {
		store.aggregationEOFs[clientID] = make(map[int]struct{})
	}

	store.aggregationEOFs[clientID][aggregationID] = struct{}{}
	return len(store.aggregationEOFs[clientID]) == store.aggregationAmount
}

func (store *queryStore) BuildTop(clientID string) []fruititem.FruitItem {
	store.mu.RLock()
	defer store.mu.RUnlock()

	clientFruitMap, ok := store.fruitItemMap[clientID]
	if !ok {
		return []fruititem.FruitItem{}
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruitMap))
	for _, item := range clientFruitMap {
		fruitItems = append(fruitItems, item)
	}

	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})

	finalTopSize := min(store.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (store *queryStore) ClearSumCoordination(clientID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.queriesProcessed, clientID)
	delete(store.clientsWaiting, clientID)
}

func (store *queryStore) Clear(clientID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.queriesProcessed, clientID)
	delete(store.clientsWaiting, clientID)
	delete(store.fruitItemMap, clientID)
	delete(store.aggregationEOFs, clientID)
}
