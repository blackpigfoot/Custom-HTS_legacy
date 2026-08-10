package replay

import (
	"container/list"
	"errors"
	"sync"

	supervisorpkg "Custom-HTS/internal/core/pkg/wsrefact_v3/supervisor"
)

var (
	// ErrURLRequired is returned when replay configuration omitted the endpoint URL.
	ErrURLRequired = supervisorpkg.ErrURLRequired
	// ErrAlreadySubscribed is returned by Subscribe when the key is already registered.
	ErrAlreadySubscribed = errors.New("replay: already subscribed")
	// ErrNotSubscribed is returned by Unsubscribe when the key is not registered.
	ErrNotSubscribed = errors.New("replay: not subscribed")
	// ErrNotConnected is returned when no live session is available for replay I/O.
	ErrNotConnected = errors.New("replay: not connected")
	// ErrGeneratorsRequired is returned when one or more payload generators are missing.
	ErrGeneratorsRequired = errors.New("replay: SubGen and UnsubGen are required")
)

// Replay tracks active subscriptions and replays them on reconnect.
//
// Lifecycle and ordering:
//
//   - Subscribe / Unsubscribe / ReplayAll are mutually exclusive: only
//     one runs at a time.
//   - The send IO of each operation happens inside the mutex, so
//     concurrent operations cannot produce duplicate or interleaved
//     frames on the wire.
//   - ReplayAll is wired into supervisor.Hooks.OnReady internally by
//     New. The supervisor exposes the session AFTER OnReady completes,
//     so external Subscribe / Unsubscribe calls during the replay
//     window see ErrNotConnected (no active session) rather than
//     racing with replay frames.
//
// Lock-window cost: SubGen and the websocket write run inside the
// mutex. For a single subscribe this is one closure call plus one
// websocket frame write, typically a few milliseconds. The next
// caller waits behind that. This is the price of a single-mutex model
// that guarantees no duplicate frames; the alternative (release lock,
// race with ReplayAll) is unsafe.
type Replay struct {
	// sender is assigned in New AFTER the supervisor is constructed,
	// because the supervisor IS the sender. Reads of this field
	// happen only from Subscribe/Unsubscribe, which the caller cannot
	// invoke before New returns. No synchronization is needed because
	// the assignment happens-before any caller use, established by
	// the function return.
	sender Sender

	// mu serializes Subscribe / Unsubscribe / ReplayAll, including
	// the send IO performed inside each operation.
	mu sync.Mutex

	// order preserves registration order for ReplayAll. Each list
	// element holds *entry.
	order *list.List
	// index maps key -> list element for O(1) lookup and removal.
	index map[string]*list.Element
}

// entry is one registered subscription.
type entry struct {
	// key uniquely identifies one logical subscription.
	key string
	// options stores the subscribe/unsubscribe payload generators for the key.
	options SubscribeOptions
}

// New builds the full replay+supervisor+session stack from a single
// flattened Config.
//
// Construction order resolves the apparent circular dependency
// (supervisor needs OnReady that calls replay; replay needs sender
// that is the supervisor):
//
//  1. Allocate Replay with sender unset.
//  2. Build supervisor.Config with OnReady wired to a closure that
//     calls replay.ReplayAll. The closure captures the replay pointer.
//  3. Construct the supervisor. The OnReady closure is stored but
//     not yet invoked (Run has not started).
//  4. Assign replay.sender = supervisor.
//  5. Return.
//
// The closure observes a fully initialized replay by the time Run starts
// and OnReady fires.
func New(config Config) (*Replay, error) {
	// replayInstance stores subscription state and wiring callbacks.
	replayInstance := &Replay{
		order: list.New(),
		index: make(map[string]*list.Element),
	}

	// supervisorConfig flattens every lower-layer setting replay needs to forward.
	supervisorConfig := supervisorpkg.Config{
		Dialer:       config.Dialer,
		URL:          config.URL,
		Header:       config.Header,
		ReadLimit:    config.ReadLimit,
		PongTimeout:  config.PongTimeout,
		PingInterval: config.PingInterval,
		WriteTimeout: config.WriteTimeout,
		Handlers:     config.Handlers,
		DialTimeout:  config.DialTimeout,
		ReconnectMin: config.ReconnectMin,
		ReconnectMax: config.ReconnectMax,
		Hooks: supervisorpkg.Hooks{
			OnReady: func(replayer supervisorpkg.Replayer, attempt int) error {
				return replayInstance.ReplayAll(replayer)
			},
			OnEvent: config.OnEvent,
		},
	}

	// supervisor owns reconnect lifecycle and active-session writes.
	supervisor, err := supervisorpkg.New(supervisorConfig)
	if errors.Is(err, ErrURLRequired) {
		return nil, ErrURLRequired
	}
	if err != nil {
		return nil, err
	}

	replayInstance.sender = supervisor
	return replayInstance, nil
}

// Subscribe registers a subscription and sends it on the active session.
//
// Returns:
//
//   - nil on successful send and registration.
//   - ErrAlreadySubscribed when the key is already registered.
//   - ErrNotConnected when no live session is exposed (during dial,
//     replay window, or after Stop). The subscription is NOT
//     registered; caller may retry on the next Connected event.
//   - The error from SubGen if payload generation fails.
//   - The error from the underlying send for other transport failures.
//
// The send happens inside the lock, so a concurrent ReplayAll cannot
// produce a duplicate frame for the same key.
func (replay *Replay) Subscribe(key string, options SubscribeOptions) error {
	if options.SubGen == nil || options.UnsubGen == nil {
		return ErrGeneratorsRequired
	}

	replay.mu.Lock()
	defer replay.mu.Unlock()

	if _, exists := replay.index[key]; exists {
		return ErrAlreadySubscribed
	}

	// payload is the freshly generated subscribe frame for this call.
	payload, err := options.SubGen()
	if err != nil {
		return err
	}

	// sendErr is the active-session send result for this subscription attempt.
	sendErr := replay.sender.Enqueue(supervisorpkg.WriteRequest{
		Payload:      payload,
		MessageType:  options.MessageType,
		WriteTimeout: options.WriteTimeout,
	})
	if errors.Is(sendErr, supervisorpkg.ErrNotConnected) {
		return ErrNotConnected
	}
	if sendErr != nil {
		// Send failed (write error, broker-side close, etc.). Do NOT register;
		// the caller observes the error and decides whether to retry.
		return sendErr
	}

	// element records this key so ReplayAll can resend it after reconnect.
	element := replay.order.PushBack(&entry{key: key, options: options})
	replay.index[key] = element
	return nil
}

// Unsubscribe removes a subscription and sends the unsubscribe payload.
//
// Returns:
//
//   - nil on successful send.
//   - nil when the send returns ErrNotConnected; the local
//     registration is removed and the subscription will simply be
//     absent from the next replay.
//   - ErrNotSubscribed when the key is not registered.
//   - The error from UnsubGen if payload generation fails. The local
//     registration is still removed.
//   - The error from the underlying send for other transport failures.
//     The local registration is still removed; the caller's intent
//     to unsubscribe is honored locally.
func (replay *Replay) Unsubscribe(key string) error {
	replay.mu.Lock()
	defer replay.mu.Unlock()

	// element is the stored registration entry for the key.
	element, exists := replay.index[key]
	if !exists {
		return ErrNotSubscribed
	}

	// storedOptions captures the unsubscribe generator before removal.
	storedOptions := element.Value.(*entry).options

	// Remove from store regardless of send outcome below; the caller's
	// intent is to unsubscribe locally even if the broker notification
	// fails.
	delete(replay.index, key)
	replay.order.Remove(element)

	// payload is the freshly generated unsubscribe frame for this call.
	payload, err := storedOptions.UnsubGen()
	if err != nil {
		return err
	}

	// sendErr is the active-session send result for the unsubscribe request.
	sendErr := replay.sender.Enqueue(supervisorpkg.WriteRequest{
		Payload:      payload,
		MessageType:  storedOptions.MessageType,
		WriteTimeout: storedOptions.WriteTimeout,
	})
	if sendErr == nil || errors.Is(sendErr, supervisorpkg.ErrNotConnected) {
		return nil
	}
	return sendErr
}

// ReplayAll re-sends every registered subscription on the given replayer.
//
// Wired into supervisor.Hooks.OnReady by New. The supervisor holds
// external Enqueue callers off (active is not yet exposed) until this
// returns successfully, so replay frames reach the wire before any
// external frame.
//
// Fails fast: returns the first error encountered. The supervisor
// treats a non-nil return as a session-unusable signal and tears down
// the session before exposing it.
//
// Subscriptions are sent in registration order.
func (replay *Replay) ReplayAll(replayer supervisorpkg.Replayer) error {
	replay.mu.Lock()
	defer replay.mu.Unlock()

	for element := replay.order.Front(); element != nil; element = element.Next() {
		// storedEntry is the replay candidate for this registration slot.
		storedEntry := element.Value.(*entry)

		// payload is the replay-time subscribe frame for the stored key.
		payload, err := storedEntry.options.SubGen()
		if err != nil {
			return err
		}

		// sendErr is the session-scoped replay write result for this frame.
		sendErr := replayer.Send(supervisorpkg.WriteRequest{
			Payload:      payload,
			MessageType:  storedEntry.options.MessageType,
			WriteTimeout: storedEntry.options.WriteTimeout,
		})
		if errors.Is(sendErr, supervisorpkg.ErrNotConnected) {
			return ErrNotConnected
		}
		if sendErr != nil {
			return sendErr
		}
	}
	return nil
}

// Subscriptions returns the registered keys in registration order.
//
// Diagnostic helper: the returned slice is a copy and reflects state
// at the moment of the call.
func (replay *Replay) Subscriptions() []string {
	replay.mu.Lock()
	defer replay.mu.Unlock()

	// keys is the copied registration snapshot returned to the caller.
	keys := make([]string, 0, replay.order.Len())
	for element := replay.order.Front(); element != nil; element = element.Next() {
		keys = append(keys, element.Value.(*entry).key)
	}
	return keys
}
