package replay

import (
	"errors"
	"time"

	supervisorpkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/supervisor"
)

// ErrAlreadySubscribed is returned by Subscribe when the key already exists.
var ErrAlreadySubscribed = errors.New("replay: already subscribed")

// ErrNotSubscribed is returned by Unsubscribe when the key is not registered.
var ErrNotSubscribed = errors.New("replay: not subscribed")

// SubscribeStatus reports the disposition of a Subscribe call.
type SubscribeStatus int

const (
	// SubscribedNow means the subscribe payload was sent on the active session.
	SubscribedNow SubscribeStatus = iota
	// SubscribedDeferred means the subscription is registered but no live
	// session was available to send on. The payload will be sent on the
	// next ReplayAll invocation (typically from supervisor's OnReady hook).
	SubscribedDeferred
)

// PayloadGen produces a websocket payload at send time.
//
// Invoked synchronously when the subscription must reach the wire:
// initial Subscribe, ReplayAll on reconnect, and Unsubscribe (for the
// unsub variant). Callers capture freshness-sensitive state (auth
// tokens, timestamps, nonces) via closure so each invocation produces
// a payload valid at that moment.
type PayloadGen func() ([]byte, error)

// SubscribeRequest registers one logical subscription with the replay store.
//
// SubGen is used for the initial send and for every replay on reconnect.
// UnsubGen is used when Unsubscribe is called for this key. Both are
// stored by the replay package and may be invoked many times across
// the subscription's lifetime.
type SubscribeRequest struct {
	// Key uniquely identifies this logical subscription within the
	// replay store. Subscribe with a key that already exists returns
	// ErrAlreadySubscribed.
	Key string
	// SubGen produces the subscribe payload. Required.
	SubGen PayloadGen
	// UnsubGen produces the unsubscribe payload. Required.
	UnsubGen PayloadGen
	// MessageType is the websocket opcode for both subscribe and
	// unsubscribe frames. Defaults to TextMessage when zero.
	MessageType int
	// WriteTimeout overrides the default websocket write timeout for
	// this subscription's frames when non-zero.
	WriteTimeout time.Duration
}

// Sender is the narrow interface replay needs from a reconnect supervisor.
//
// Defined here (rather than imported from supervisor) so replay can be
// unit-tested with a mock and so the dependency direction stays
// replay -> supervisor, never the reverse.
type Sender interface {
	// Enqueue forwards a write request to the active session. Returns
	// supervisor.ErrNotConnected when no live session is available; any
	// other non-nil error indicates a write or transport failure.
	Enqueue(request supervisorpkg.WriteRequest) error
}
