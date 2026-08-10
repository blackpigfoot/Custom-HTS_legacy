// Replay performs synchronous re-subscription on reconnect: ReplayAll
// runs inside supervisor's OnReady hook, which blocks the supervisor
// from exposing the new session until replay completes. External
// Enqueue callers see ErrNotConnected during this window.
//
// For asynchronous re-subscription (overlapping replay with external
// traffic), see the asyncreplay package, which trades ordering
// guarantees for reduced reconnect latency.
package replay

import (
	"errors"
	"sync"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/common"
	supervisorpkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/supervisor"
)

// Replay tracks active subscriptions and replays them on reconnect.
//
// Replay is a thin layer above a reconnect supervisor: it does not own
// connection lifecycle, backoff, or message framing. Its only
// responsibilities are:
//
//   - record which subscriptions the caller has registered
//   - serialize concurrent Subscribe/Unsubscribe of the same key
//   - re-send all active subscriptions on demand (ReplayAll)
//
// Replay is intentionally domain-agnostic. Payload format, broker
// authentication, and request/response correlation are caller concerns,
// expressed through the PayloadGen closures stored in SubscribeRequest.
//
// Integration: the caller installs a supervisor.Hooks.OnReady handler
// that invokes ReplayAll on the replayer passed in. ReplayAll runs
// before the new session is exposed to external Enqueue callers, so
// replay frames always reach the wire before any frame from a
// concurrent Subscribe.
type Replay struct {
	sender Sender
	store  *store

	// keyLocks holds one *sync.Mutex per registered key, so concurrent
	// Subscribe/Unsubscribe calls for the same key serialize at this
	// layer rather than racing in the store. Different keys remain
	// fully concurrent.
	//
	// Entries are never removed: a key's mutex outlives its
	// subscription so a Subscribe-Unsubscribe-Subscribe sequence under
	// concurrent callers cannot drop the lock between phases.
	keyLocks sync.Map
}

// New creates a Replay bound to the given Sender (typically a
// *supervisor.Supervisor).
func New(sender Sender) *Replay {
	return &Replay{
		sender: sender,
		store:  newStore(),
	}
}

// Subscribe registers a subscription and attempts to send it on the
// active session.
//
// Returns:
//
//   - SubscribedNow, nil on successful send.
//   - SubscribedDeferred, nil when no live session is available; the
//     subscription is registered and will be sent on the next ReplayAll.
//   - 0, ErrAlreadySubscribed when the key is already registered.
//   - 0, error when SubGen fails or the send returns a non-recoverable
//     transport error. The subscription is NOT registered in this case.
//
// Subscribe is safe for concurrent use. Calls for different keys may
// run in parallel; calls for the same key serialize at this layer.
// Acquisition order between contending callers of the same key is not
// guaranteed (Go's sync.Mutex is unfair); callers requiring "first call
// wins" semantics must coordinate above this layer.
func (r *Replay) Subscribe(req SubscribeRequest) (SubscribeStatus, error) {
	if req.SubGen == nil || req.UnsubGen == nil {
		return 0, errors.New("replay: SubGen and UnsubGen are required")
	}

	keyMu := r.lockForKey(req.Key)
	keyMu.Lock()
	defer keyMu.Unlock()

	if err := r.store.add(req); err != nil {
		return 0, err
	}

	payload, err := req.SubGen()
	if err != nil {
		// SubGen failure: roll back registration so the caller's error
		// observation matches the registry state.
		r.store.remove(req.Key)
		return 0, err
	}

	sendErr := r.sender.Enqueue(supervisorpkg.WriteRequest{
		Payload:      payload,
		MessageType:  req.MessageType,
		WriteTimeout: req.WriteTimeout,
	})
	if sendErr == nil {
		return SubscribedNow, nil
	}
	if errors.Is(sendErr, common.ErrNotConnected) {
		// Conn unavailable: keep the registration so ReplayAll picks
		// it up on reconnect. Caller observes the deferred status and
		// can surface "pending" UI or similar.
		return SubscribedDeferred, nil
	}

	// Transport failure that is neither "no conn" nor "bad payload":
	// the conn died mid-write or similar. Roll back and surface the
	// error; the caller decides whether to retry.
	r.store.remove(req.Key)
	return 0, sendErr
}

// Unsubscribe removes a subscription and attempts to send the
// unsubscribe payload on the active session.
//
// Returns:
//
//   - nil on successful send.
//   - nil when no live session is available; the subscription is
//     removed locally and the broker-side state will reconcile on
//     reconnect (replay will simply not include this key).
//   - ErrNotSubscribed when the key is not registered.
//   - error when UnsubGen fails or the send returns a transport error.
//     The subscription is removed regardless of send outcome: the
//     caller's intent to unsubscribe is honored locally.
//
// Unsubscribe is safe for concurrent use under the same per-key
// serialization rules as Subscribe.
func (r *Replay) Unsubscribe(key string) error {
	keyMu := r.lockForKey(key)
	keyMu.Lock()
	defer keyMu.Unlock()

	stored, ok := r.store.remove(key)
	if !ok {
		return ErrNotSubscribed
	}

	payload, err := stored.UnsubGen()
	if err != nil {
		// UnsubGen failed: the local registration is already removed
		// (caller's intent honored). Return the error so the caller
		// knows the broker was not notified.
		return err
	}

	sendErr := r.sender.Enqueue(supervisorpkg.WriteRequest{
		Payload:      payload,
		MessageType:  stored.MessageType,
		WriteTimeout: stored.WriteTimeout,
	})
	if sendErr == nil || errors.Is(sendErr, common.ErrNotConnected) {
		// Either the broker was notified, or there is no conn and the
		// subscription will simply be absent from the next replay.
		// Both outcomes match the caller's intent.
		return nil
	}
	return sendErr
}

// ReplayAll re-sends every registered subscription on the given replayer.
//
// Intended to be called from supervisor.Hooks.OnReady, where the
// replayer is scoped to a freshly connected session that has not yet
// been exposed to external Enqueue callers. Sending here guarantees
// replay frames reach the wire before any concurrent Subscribe.
//
// Fails fast: returns the first error encountered. The remaining
// subscriptions are not attempted, and the supervisor treats the error
// as a session-unusable signal (tears down the session and proceeds to
// the next reconnect attempt).
//
// Subscriptions are sent in registration order. Subscribe/Unsubscribe
// calls running concurrently with ReplayAll observe a snapshot taken
// at the start of ReplayAll; later changes are not reflected until the
// next reconnect.
func (r *Replay) ReplayAll(replayer supervisorpkg.Replayer) error {
	for _, req := range r.store.snapshot() {
		payload, err := req.SubGen()
		if err != nil {
			return err
		}
		if err := replayer.Send(supervisorpkg.WriteRequest{
			Payload:      payload,
			MessageType:  req.MessageType,
			WriteTimeout: req.WriteTimeout,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Subscriptions returns the registered keys in registration order.
//
// Diagnostic helper: the returned slice is a copy and reflects state
// at the moment of the call. Useful for UI listings and debugging.
func (r *Replay) Subscriptions() []string {
	return r.store.keys()
}

// lockForKey returns the per-key mutex, creating one on first access.
// Mutexes are never removed: a Subscribe-Unsubscribe-Subscribe sequence
// under concurrent callers must not drop the lock between phases, or
// two Subscribe calls could interleave with the Unsubscribe.
func (r *Replay) lockForKey(key string) *sync.Mutex {
	actual, _ := r.keyLocks.LoadOrStore(key, &sync.Mutex{})
	return actual.(*sync.Mutex)
}
