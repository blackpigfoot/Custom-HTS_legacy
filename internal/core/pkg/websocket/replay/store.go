package replay

import "sync"

// intentKind identifies one replayable write-intent operation.
type intentKind int

const (
	// intentSubscribe appends one subscribe-like replayable write intent.
	intentSubscribe intentKind = iota
	// intentUnsubscribe appends one unsubscribe-like replayable write intent.
	intentUnsubscribe
)

// intentOp is one append-only replay operation processed in insertion order.
type intentOp struct {
	// kind identifies whether this replay operation subscribes or unsubscribes.
	kind intentKind
	// key is the logical replay key for this operation.
	key string
	// intent stores the payload generator and write options.
	intent WriteIntent
}

// intentStore tracks desired replay state using two structures:
//   - pending: ordered local intent log that has not yet been written
//   - desired: authoritative desired replay set used after reconnects
//   - desiredOrder: stable replay order for desired subscriptions
type intentStore struct {
	// mu guards pending, desired, and desiredOrder.
	mu sync.Mutex
	// pending stores ordered local replay operations that have not been written yet.
	pending []intentOp
	// desired stores the authoritative replay set keyed by logical subscription key.
	desired map[string]WriteIntent
	// desiredOrder keeps stable replay order for desired subscriptions.
	desiredOrder []string
}

func newIntentStore() *intentStore {
	return &intentStore{
		pending:      make([]intentOp, 0),
		desired:      make(map[string]WriteIntent),
		desiredOrder: make([]string, 0),
	}
}

func (store *intentStore) recordSubscribe(key string, intent WriteIntent) bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.isLogicallySubscribed(key) {
		return false
	}

	store.pending = append(store.pending, intentOp{
		kind:   intentSubscribe,
		key:    key,
		intent: intent,
	})
	return true
}

func (store *intentStore) recordUnsubscribe(key string, intent WriteIntent) bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.isLogicallySubscribed(key) {
		return false
	}

	store.pending = append(store.pending, intentOp{
		kind:   intentUnsubscribe,
		key:    key,
		intent: intent,
	})
	return true
}

func (store *intentStore) peekFront() (intentOp, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.pending) == 0 {
		return intentOp{}, false
	}
	return store.pending[0], true
}

func (store *intentStore) confirmSubscribe(key string, intent WriteIntent) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.upsertDesiredLocked(key, intent)
	store.popFrontLocked()
}

func (store *intentStore) confirmUnsubscribe(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.removeDesiredLocked(key)
	store.popFrontLocked()
}

func (store *intentStore) discardFront() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.popFrontLocked()
}

func (store *intentStore) resetForReconnect() {
	store.mu.Lock()
	defer store.mu.Unlock()

	for _, op := range store.pending {
		switch op.kind {
		case intentSubscribe:
			store.upsertDesiredLocked(op.key, op.intent)
		case intentUnsubscribe:
			store.removeDesiredLocked(op.key)
		}
	}

	store.pending = make([]intentOp, 0, len(store.desiredOrder))
	for _, key := range store.desiredOrder {
		intent, ok := store.desired[key]
		if !ok {
			continue
		}
		store.pending = append(store.pending, intentOp{
			kind:   intentSubscribe,
			key:    key,
			intent: intent,
		})
	}
}

func (store *intentStore) logicalKeys() []string {
	store.mu.Lock()
	defer store.mu.Unlock()

	// state is the merged logical replay set after pending local intents.
	state := make(map[string]bool, len(store.desired))
	for key := range store.desired {
		state[key] = true
	}
	for _, op := range store.pending {
		switch op.kind {
		case intentSubscribe:
			state[op.key] = true
		case intentUnsubscribe:
			delete(state, op.key)
		}
	}

	// result is the flattened logical replay key slice exposed to callers.
	result := make([]string, 0, len(state))
	for key := range state {
		result = append(result, key)
	}
	return result
}

func (store *intentStore) isLogicallySubscribed(key string) bool {
	for i := len(store.pending) - 1; i >= 0; i-- {
		if store.pending[i].key == key {
			return store.pending[i].kind == intentSubscribe
		}
	}
	_, ok := store.desired[key]
	return ok
}

func (store *intentStore) popFrontLocked() {
	if len(store.pending) == 0 {
		return
	}
	store.pending[0] = intentOp{}
	store.pending = store.pending[1:]
}

func (store *intentStore) upsertDesiredLocked(key string, intent WriteIntent) {
	if _, exists := store.desired[key]; !exists {
		store.desiredOrder = append(store.desiredOrder, key)
	}
	store.desired[key] = intent
}

func (store *intentStore) removeDesiredLocked(key string) {
	if _, exists := store.desired[key]; !exists {
		return
	}

	delete(store.desired, key)
	for i, existing := range store.desiredOrder {
		if existing != key {
			continue
		}
		copy(store.desiredOrder[i:], store.desiredOrder[i+1:])
		store.desiredOrder[len(store.desiredOrder)-1] = ""
		store.desiredOrder = store.desiredOrder[:len(store.desiredOrder)-1]
		return
	}
}
