package aggregation

import (
	"sort"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type clientAggregationStore struct {
	mu                sync.RWMutex
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	clientSumsCounter map[string]int
	sumAmount         int
	topSize           int
}

func newClientAggregationStore(sumAmount int, topSize int) *clientAggregationStore {
	return &clientAggregationStore{
		fruitItemMap:      make(map[string]map[string]fruititem.FruitItem),
		clientSumsCounter: make(map[string]int),
		sumAmount:         sumAmount,
		topSize:           topSize,
	}
}

func (store *clientAggregationStore) Add(clientID string, fruitRecords []fruititem.FruitItem) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, ok := store.fruitItemMap[clientID]; !ok {
		store.fruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
		store.clientSumsCounter[clientID] = 0
	}

	for _, fruitRecord := range fruitRecords {
		if current, ok := store.fruitItemMap[clientID][fruitRecord.Fruit]; ok {
			store.fruitItemMap[clientID][fruitRecord.Fruit] = current.Sum(fruitRecord)
			continue
		}
		store.fruitItemMap[clientID][fruitRecord.Fruit] = fruitRecord
	}
}

func (store *clientAggregationStore) RegisterEOF(clientID string) (shouldBuildTop bool) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, ok := store.clientSumsCounter[clientID]; !ok {
		store.clientSumsCounter[clientID] = 0
	}

	store.clientSumsCounter[clientID] += 1
	return store.clientSumsCounter[clientID] == store.sumAmount
}

func (store *clientAggregationStore) BuildTop(clientID string) []fruititem.FruitItem {
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

func (store *clientAggregationStore) Clear(clientID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.fruitItemMap, clientID)
	delete(store.clientSumsCounter, clientID)
}
