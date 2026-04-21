package sum

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type clientFruitStore struct {
	mu    sync.RWMutex
	items map[string]map[string]fruititem.FruitItem
}

func newClientFruitStore() *clientFruitStore {
	return &clientFruitStore{items: make(map[string]map[string]fruititem.FruitItem)}
}

func (store *clientFruitStore) Add(clientID string, records []fruititem.FruitItem) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, ok := store.items[clientID]; !ok {
		store.items[clientID] = make(map[string]fruititem.FruitItem)
	}

	for _, record := range records {
		if current, ok := store.items[clientID][record.Fruit]; ok {
			store.items[clientID][record.Fruit] = current.Sum(record)
			continue
		}
		store.items[clientID][record.Fruit] = record
	}
}

func (store *clientFruitStore) GetClientRecords(clientID string) ([]fruititem.FruitItem, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	clientMap, ok := store.items[clientID]
	if !ok {
		return nil, false
	}

	result := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, item := range clientMap {
		result = append(result, item)
	}

	return result, true
}

func (store *clientFruitStore) Clear(clientID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.items, clientID)
}
