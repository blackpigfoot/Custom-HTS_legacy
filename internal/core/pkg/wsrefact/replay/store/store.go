package store

import (
	"sync"

	supervisorpkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact/supervisor"
)

// IntentKind identifies one WAL-like replay operation.
type IntentKind int

const (
	// IntentSubscribe appends one subscribe-like replay operation.
	IntentSubscribe IntentKind = iota
	// IntentUnsubscribe appends one unsubscribe-like replay operation.
	IntentUnsubscribe
)

// IntentOp is one ordered WAL-like replay operation.
type IntentOp struct {
	// Kind identifies whether this operation subscribes or unsubscribes.
	Kind IntentKind
	// Key is the logical replay key for this operation.
	Key string
	// Request is the supervisor write request that realizes this operation.
	Request supervisorpkg.WriteRequest
}

// Store tracks desired replay state using WAL-like local intent recording.
type Store struct {
	// mu guards pending, desired, and desiredOrder.
	mu sync.Mutex
	// pending stores ordered local replay operations that have not been committed yet.
	pending []IntentOp
	// desired stores the authoritative replay set keyed by logical subscription key.
	desired map[string]supervisorpkg.WriteRequest
	// desiredOrder keeps stable replay order for desired subscriptions.
	desiredOrder []string
}

// New creates an empty WAL-like replay store.
func New() *Store {
	return &Store{
		pending:      make([]IntentOp, 0),
		desired:      make(map[string]supervisorpkg.WriteRequest),
		desiredOrder: make([]string, 0),
	}
}

// RecordSubscribe appends one desired subscribe intent.
func (store *Store) RecordSubscribe(key string, request supervisorpkg.WriteRequest) bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.isLogicallySubscribed(key) {
		return false
	}

	store.pending = append(store.pending, IntentOp{
		Kind:    IntentSubscribe,
		Key:     key,
		Request: request,
	})
	return true
}

// RecordUnsubscribe appends one desired unsubscribe intent.
func (store *Store) RecordUnsubscribe(key string, request supervisorpkg.WriteRequest) bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	if !store.isLogicallySubscribed(key) {
		return false
	}

	store.pending = append(store.pending, IntentOp{
		Kind:    IntentUnsubscribe,
		Key:     key,
		Request: request,
	})
	return true
}

// PeekFront returns the first pending replay operation without removing it.
func (store *Store) PeekFront() (IntentOp, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.pending) == 0 {
		return IntentOp{}, false
	}
	return store.pending[0], true
}

// ConfirmSubscribe commits the front subscribe operation into the desired replay set.
func (store *Store) ConfirmSubscribe(key string, request supervisorpkg.WriteRequest) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.upsertDesiredLocked(key, request)
	store.popFrontLocked()
}

// ConfirmUnsubscribe commits the front unsubscribe operation and removes it from the desired set.
func (store *Store) ConfirmUnsubscribe(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.removeDesiredLocked(key)
	store.popFrontLocked()
}

// DiscardFront removes the first pending replay item without touching the desired replay set.
func (store *Store) DiscardFront() {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.popFrontLocked()
}

// ResetForReconnect rebuilds the pending WAL-like queue from the desired replay set.
func (store *Store) ResetForReconnect() {
	store.mu.Lock()
	defer store.mu.Unlock()

	for _, op := range store.pending {
		switch op.Kind {
		case IntentSubscribe:
			store.upsertDesiredLocked(op.Key, op.Request)
		case IntentUnsubscribe:
			store.removeDesiredLocked(op.Key)
		}
	}

	store.pending = make([]IntentOp, 0, len(store.desiredOrder))
	for _, key := range store.desiredOrder {
		request, ok := store.desired[key]
		if !ok {
			continue
		}
		store.pending = append(store.pending, IntentOp{
			Kind:    IntentSubscribe,
			Key:     key,
			Request: request,
		})
	}
}

// LogicalKeys returns the logical replay key set after pending local intents.
func (store *Store) LogicalKeys() []string {
	store.mu.Lock()
	defer store.mu.Unlock()

	// state is the merged logical replay set after pending local intents.
	state := make(map[string]bool, len(store.desired))
	for key := range store.desired {
		state[key] = true
	}
	for _, op := range store.pending {
		switch op.Kind {
		case IntentSubscribe:
			state[op.Key] = true
		case IntentUnsubscribe:
			delete(state, op.Key)
		}
	}

	// result is the flattened logical replay key slice exposed to callers.
	result := make([]string, 0, len(state))
	for key := range state {
		result = append(result, key)
	}
	return result
}

func (store *Store) isLogicallySubscribed(key string) bool {
	for i := len(store.pending) - 1; i >= 0; i-- {
		if store.pending[i].Key == key {
			return store.pending[i].Kind == IntentSubscribe
		}
	}
	_, ok := store.desired[key]
	return ok
}

func (store *Store) popFrontLocked() {
	if len(store.pending) == 0 {
		return
	}
	store.pending[0] = IntentOp{}
	store.pending = store.pending[1:]
}

func (store *Store) upsertDesiredLocked(key string, request supervisorpkg.WriteRequest) {
	if _, exists := store.desired[key]; !exists {
		store.desiredOrder = append(store.desiredOrder, key)
	}
	store.desired[key] = request
}

func (store *Store) removeDesiredLocked(key string) {
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
