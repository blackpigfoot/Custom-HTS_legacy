package wsrefact_v5

// subscriptionIntent stores the replay metadata for one logical subscription key.
type subscriptionIntent struct {
	// key is the logical subscription identifier.
	key string
	// options stores the subscribe and unsubscribe generators plus write overrides.
	options SubscribeOptions
}

// intentStore stores logical subscription intent in caller order.
type intentStore struct {
	// order preserves logical subscribe order for reconnect replay.
	order []string
	// entries stores the live desired-state subscription metadata by key.
	entries map[string]subscriptionIntent
}

// newIntentStore allocates one single-thread-owned intent store.
func newIntentStore() *intentStore {
	// store owns reconnect replay intent for one Client loop.
	store := &intentStore{
		entries: make(map[string]subscriptionIntent),
	}
	return store
}

// put records one desired subscription.
func (store *intentStore) put(key string, options SubscribeOptions) error {
	if _, exists := store.entries[key]; exists {
		return ErrAlreadySubscribed
	}

	// intent stores the logical desired state for this key.
	intent := subscriptionIntent{
		key:     key,
		options: options,
	}
	store.order = append(store.order, key)
	store.entries[key] = intent
	return nil
}

// remove deletes and returns one desired subscription.
func (store *intentStore) remove(key string) (subscriptionIntent, error) {
	// intent stores the removed logical subscription metadata.
	intent, exists := store.entries[key]
	if !exists {
		return subscriptionIntent{}, ErrNotSubscribed
	}

	delete(store.entries, key)
	for index, orderedKey := range store.order {
		if orderedKey != key {
			continue
		}
		store.order = append(store.order[:index], store.order[index+1:]...)
		break
	}
	return intent, nil
}

// snapshot returns the current replay order as a detached slice.
func (store *intentStore) snapshot() []subscriptionIntent {
	// snapshot stores the detached replay sequence returned to the caller.
	snapshot := make([]subscriptionIntent, 0, len(store.order))
	for _, key := range store.order {
		// intent stores the desired subscription metadata for this ordered key.
		intent, exists := store.entries[key]
		if !exists {
			continue
		}
		snapshot = append(snapshot, intent)
	}
	return snapshot
}

// keys returns the logical desired-state key set in replay order.
func (store *intentStore) keys() []string {
	// keys stores the detached logical key snapshot returned to the caller.
	keys := make([]string, len(store.order))
	copy(keys, store.order)
	return keys
}
