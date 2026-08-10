package replay

import (
	"container/list"
	"sync"
)

// store keeps registered subscriptions in registration order.
//
// Backed by a linked list for O(1) Unsubscribe and a map for O(1) lookup.
// Iteration walks the list in insertion order, which ReplayAll relies on
// to re-send subscriptions in the same order they were registered.
//
// All exported store methods are safe for concurrent use; per-key
// serialization (preventing concurrent Subscribe/Unsubscribe of the
// same key from interleaving) is the caller's responsibility and is
// implemented by the Replay wrapper.
type store struct {
	mu sync.Mutex

	// order preserves registration order. Each element holds *entry.
	order *list.List
	// index maps key -> list element for O(1) lookup and removal.
	index map[string]*list.Element
}

// entry is one registered subscription.
type entry struct {
	key     string
	request SubscribeRequest
}

func newStore() *store {
	return &store{
		order: list.New(),
		index: make(map[string]*list.Element),
	}
}

// add registers a new subscription. Returns ErrAlreadySubscribed if the
// key already exists.
func (s *store) add(req SubscribeRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.index[req.Key]; exists {
		return ErrAlreadySubscribed
	}
	element := s.order.PushBack(&entry{key: req.Key, request: req})
	s.index[req.Key] = element
	return nil
}

// remove deletes a subscription. Returns the stored request and true on
// success, or zero value and false if the key is not registered.
//
// The returned request lets callers (Unsubscribe) access the stored
// UnsubGen without a separate lookup.
func (s *store) remove(key string) (SubscribeRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, exists := s.index[key]
	if !exists {
		return SubscribeRequest{}, false
	}
	delete(s.index, key)
	s.order.Remove(element)
	return element.Value.(*entry).request, true
}

// snapshot returns all registered subscriptions in registration order.
//
// Returns a copy so the caller can iterate without holding the store
// lock. Used by ReplayAll, which iterates and sends without serializing
// other Subscribe/Unsubscribe calls during the replay.
func (s *store) snapshot() []SubscribeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]SubscribeRequest, 0, s.order.Len())
	for e := s.order.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*entry).request)
	}
	return out
}

// keys returns all registered keys in registration order. Used by
// Subscriptions() for diagnostic exposure.
func (s *store) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, s.order.Len())
	for e := s.order.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*entry).key)
	}
	return out
}
