package join

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common"
)

type clientQueryStore struct {
	mu               sync.RWMutex
	queriesProcessed map[string]map[uint32]struct{}
	clientsWaiting   map[string]uint32
}

func newClientQueryStore() *clientQueryStore {
	return &clientQueryStore{
		queriesProcessed: make(map[string]map[uint32]struct{}),
		clientsWaiting:   make(map[string]uint32),
	}
}

func (store *clientQueryStore) RegisterQuery(clientID string, queryID uint32) (uint32, bool) {
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

func (store *clientQueryStore) RegisterEOF(clientID string, queryID uint32) (flushQueryID uint32, shouldFlush bool, emptyClient bool) {
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

func (store *clientQueryStore) Clear(clientID string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.queriesProcessed, clientID)
	delete(store.clientsWaiting, clientID)
}
