package common

import (
	"fmt"
	"strings"
	"sync"
)

type Tracker struct {
	processed sync.Map
}

func NewTracker() *Tracker {
	return &Tracker{}
}

// Load checks if the message has already been processed. If not, it marks it as processed and returns false
// If it has already been processed, it returns true.
func (tracker *Tracker) Load(clientID string, queryID uint32, msgType string, sourceID *int) bool {
	key := BuildMessageKey(clientID, queryID, msgType, sourceID)
	_, alreadyLoaded := tracker.processed.LoadOrStore(key, struct{}{})
	return alreadyLoaded
}

func (tracker *Tracker) DeleteByClient(clientID string) {
	prefix := fmt.Sprintf("%s_", clientID)
	tracker.processed.Range(func(key, _ any) bool {
		keyStr, ok := key.(string)
		if ok && strings.HasPrefix(keyStr, prefix) {
			tracker.processed.Delete(keyStr)
		}
		return true
	})
}

func BuildMessageKey(clientID string, queryID uint32, msgType string, sourceID *int) string {
	if sourceID == nil {
		return fmt.Sprintf("%s_%d_%s", clientID, queryID, msgType)
	}

	return fmt.Sprintf("%s_%d_%s_%d", clientID, queryID, msgType, *sourceID)
}
