package replay

import (
	"net/http"
	"time"

	supervisorpkg "Custom-HTS/internal/core/pkg/wsrefact_v3/supervisor"

	"github.com/gorilla/websocket"
)

// Supervisor aliases the wrapped supervisor type for upper callers.
type Supervisor = supervisorpkg.Supervisor

// MessageHandler aliases the wrapped session message callback type for upper callers.
type MessageHandler = supervisorpkg.MessageHandler

// Handlers aliases the wrapped session handler set for upper callers.
type Handlers = supervisorpkg.Handlers

// OperationError aliases the wrapped lower-layer operation error for upper callers.
type OperationError = supervisorpkg.OperationError

// ConnState aliases the wrapped supervisor lifecycle state for upper callers.
type ConnState = supervisorpkg.ConnState

const (
	// ConnStateStopped reports that the transport is not running.
	ConnStateStopped ConnState = supervisorpkg.ConnStateStopped
	// ConnStateReconnecting reports that the transport is trying to establish a session.
	ConnStateReconnecting ConnState = supervisorpkg.ConnStateReconnecting
	// ConnStateConnected reports that the transport currently owns a live session.
	ConnStateConnected ConnState = supervisorpkg.ConnStateConnected
)

// ConnEventType aliases the wrapped supervisor lifecycle event type for upper callers.
type ConnEventType = supervisorpkg.ConnEventType

const (
	// Connected reports that the first live session became ready.
	Connected ConnEventType = supervisorpkg.Connected
	// Reconnected reports that a later live session became ready after a disconnect.
	Reconnected ConnEventType = supervisorpkg.Reconnected
	// Disconnected reports that the live session ended.
	Disconnected ConnEventType = supervisorpkg.Disconnected
	// Reconnecting reports that the transport is waiting before the next dial attempt.
	Reconnecting ConnEventType = supervisorpkg.Reconnecting
)

// ConnEvent aliases the wrapped supervisor lifecycle event for upper callers.
type ConnEvent = supervisorpkg.ConnEvent

// Config configures a complete replay+supervisor+session stack.
//
// All lower-layer config is flattened into this single struct. Replay
// constructs the supervisor internally, and the supervisor constructs
// the session factory internally.
type Config struct {
	// Dialer is the gorilla websocket dialer. Nil falls back to websocket.DefaultDialer.
	Dialer *websocket.Dialer
	// URL is the websocket endpoint URL. Required.
	URL string
	// Header carries optional handshake headers.
	Header http.Header
	// ReadLimit is the maximum inbound frame size in bytes.
	ReadLimit int64
	// PongTimeout is the read deadline extension applied on each pong.
	PongTimeout time.Duration
	// PingInterval controls ping cadence. Zero disables pings.
	PingInterval time.Duration
	// WriteTimeout is the default websocket write timeout.
	WriteTimeout time.Duration
	// Handlers receives synchronous inbound message callbacks.
	Handlers Handlers
	// DialTimeout caps a single dial attempt. Zero defaults to 10s.
	DialTimeout time.Duration
	// ReconnectMin is the minimum reconnect backoff.
	ReconnectMin time.Duration
	// ReconnectMax is the maximum reconnect backoff.
	ReconnectMax time.Duration
	// OnEvent receives lifecycle transitions after replay wiring.
	OnEvent func(event ConnEvent)
}

// PayloadGen produces the bytes for one subscribe or unsubscribe frame
// at call time. Replay invokes the generator both for the initial send
// and for every reconnect-time replay, so callers that need fresh
// authentication tokens, nonces, or timestamps build them inside the
// generator rather than capturing them at registration time.
type PayloadGen func() ([]byte, error)

// SubscribeOptions configures one logical subscription.
//
// SubGen is used for the initial send and for every replay on
// reconnect. UnsubGen is used when Unsubscribe is called for this
// subscription's key. Both are stored by the replay package and may
// be invoked many times across the subscription's lifetime.
type SubscribeOptions struct {
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

// Sender is the narrow interface Replay needs for outbound writes.
// *Supervisor satisfies it via its Enqueue method.
//
// Exposed for tests and for callers that want to drive Replay against
// a non-supervisor sender.
type Sender interface {
	// Enqueue forwards one write request to the active live session.
	Enqueue(request supervisorpkg.WriteRequest) error
}
