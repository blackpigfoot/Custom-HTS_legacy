package wsrefact_v4

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// MessageType identifies a websocket frame opcode.
//
// Values match RFC 6455 opcodes and gorilla/websocket constants
// (TextMessage=1, BinaryMessage=2). Defined here so callers can
// express message types without importing gorilla/websocket directly.
type MessageType int

const (
	// MessageTypeText is the text frame opcode (RFC 6455).
	MessageTypeText MessageType = 1
	// MessageTypeBinary is the binary frame opcode (RFC 6455).
	MessageTypeBinary MessageType = 2
)

// ConnState reports the connection lifecycle state of a Client.
type ConnState int

const (
	// ConnStateStopped reports that the client is not running.
	ConnStateStopped ConnState = iota
	// ConnStateReconnecting reports that the client is trying to
	// establish a session (dialing, replaying, or backing off).
	ConnStateReconnecting
	// ConnStateConnected reports that the client currently owns a
	// fully ready session (post-replay).
	ConnStateConnected
)

// String formats the lifecycle state for diagnostics.
func (state ConnState) String() string {
	switch state {
	case ConnStateStopped:
		return "Stopped"
	case ConnStateReconnecting:
		return "Reconnecting"
	case ConnStateConnected:
		return "Connected"
	default:
		return fmt.Sprintf("ConnState(%d)", int(state))
	}
}

// ConnEventType identifies one websocket lifecycle event.
type ConnEventType string

const (
	// Connected reports that the first live session became ready
	// (post-replay).
	Connected ConnEventType = "connected"
	// Reconnected reports that a later live session became ready
	// after a disconnect (post-replay).
	Reconnected ConnEventType = "reconnected"
	// Disconnected reports that the live session ended.
	Disconnected ConnEventType = "disconnected"
	// Reconnecting reports that the client is waiting before the next
	// dial attempt.
	Reconnecting ConnEventType = "reconnecting"
	// SendDuringBackoff reports that a Send / Subscribe / Unsubscribe
	// arrived while no live session was exposed. Callers that disable
	// or ignore the built-in replay can use this to track or
	// reschedule those operations themselves.
	SendDuringBackoff ConnEventType = "send_during_backoff"
)

// ConnEvent reports one websocket lifecycle transition.
type ConnEvent struct {
	// Type identifies the lifecycle transition.
	Type ConnEventType
	// Err carries the lifecycle failure when applicable.
	Err error
	// Attempt stores the reconnect attempt count when applicable.
	Attempt int
	// Key carries the subscription key for SendDuringBackoff events
	// that originated from Subscribe / Unsubscribe. Empty for
	// SendDuringBackoff events from Send.
	Key string
	// At is the local time when the event was emitted.
	At time.Time
}

// Handlers contains synchronous websocket callbacks delivered from the
// session's read loop.
type Handlers struct {
	// OnMessage handles one inbound websocket payload.
	//
	// Called synchronously by the read loop and must return promptly.
	// The data slice is valid only until OnMessage returns; copy it
	// if the handler stores it or passes it to another goroutine.
	OnMessage func(data []byte)
}

var noopHandlers = Handlers{
	OnMessage: func([]byte) {},
}

// normalizeHandlers replaces nil callbacks with fast no-op handlers.
func normalizeHandlers(handlers Handlers) Handlers {
	if handlers.OnMessage == nil {
		handlers.OnMessage = noopHandlers.OnMessage
	}
	return handlers
}

// Hooks delivers lifecycle notifications from the Client.
//
// OnReady runs after the built-in subscription replay completes on
// each new session. OnEvent is the observer for every lifecycle
// transition. While OnReady runs, Send / Subscribe / Unsubscribe see
// ErrNotConnected (the session is exposed only after the entire
// OnReady chain succeeds). Connected and Reconnected events fire
// only after OnReady completes successfully, so observing them means
// the session is fully ready for external traffic.
type Hooks struct {
	// OnReady runs after subscription replay completes on each new
	// session. Optional. The attempt argument is 0 for the initial
	// dial and 1+ for reconnects.
	//
	// Returning a non-nil error aborts session activation; the
	// session is torn down and the client proceeds to the next
	// reconnect attempt.
	OnReady func(replayer Replayer, attempt int) error
	// OnEvent is invoked for every lifecycle transition. Optional.
	OnEvent func(event ConnEvent)
}

// PayloadGen produces the bytes for one subscribe or unsubscribe frame
// at call time. The Client invokes the generator both for the initial
// send and for every reconnect-time replay, so callers that need
// fresh authentication tokens, nonces, or timestamps build them
// inside the generator rather than capturing them at registration
// time.
type PayloadGen func() ([]byte, error)

// SubscribeOptions configures one logical subscription.
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

// Replayer is the narrow interface exposed to OnReady so the upper
// layer can write replay payloads to a freshly connected session
// before any other goroutine can observe it.
//
// Replayer is valid only for the duration of one OnReady call.
// Storing the reference for use after OnReady returns is undefined:
// the session may be torn down at any time.
type Replayer interface {
	// Send writes one frame to the underlying session synchronously.
	// Returns nil when the frame has been written; ErrNotConnected if
	// the session is already shutting down; an OperationError on
	// write failure.
	Send(payload []byte, options ...SendOption) error
}

// SendOption mutates the per-call write parameters used by Send,
// Replayer.Send, and the subscription frames.
type SendOption func(*sendOptions)

type sendOptions struct {
	messageType  int
	writeTimeout time.Duration
}

// WithMessageType overrides the default text opcode for one Send call.
func WithMessageType(messageType int) SendOption {
	return func(o *sendOptions) { o.messageType = messageType }
}

// WithWriteTimeout overrides the default write timeout for one Send
// call.
func WithWriteTimeout(timeout time.Duration) SendOption {
	return func(o *sendOptions) { o.writeTimeout = timeout }
}

// Subscription is the handle returned by Send and the subscription
// methods. The caller waits on Done() for the result of the operation.
//
// The result channel is read-only (`<-chan error`). Callers cannot
// close it, which prevents accidental panics from double-close or
// closing while the worker still writes.
//
// The channel is buffered (capacity 1) and closed by the client after
// the result has been delivered. A typical use is:
//
//	sub := client.Send(payload)
//	if err := <-sub.Done(); err != nil {
//	    // handle failure
//	}
//
// Callers that do not care about the result may discard the
// Subscription; the client still drains its internal state correctly
// because the buffered channel never blocks the worker.
type Subscription struct {
	done chan error
}

// Done returns the read-only result channel. The channel yields one
// value (the operation result) and is then closed. Subsequent reads
// observe the zero value (nil) repeatedly.
func (s Subscription) Done() <-chan error {
	return s.done
}

// newSubscription allocates a Subscription with a buffered done
// channel. Buffer 1 lets the worker write the result without waiting
// for the caller, so a caller that discards the Subscription does not
// block the worker.
func newSubscription() Subscription {
	return Subscription{done: make(chan error, 1)}
}

// resolve writes the result and closes the channel. Worker-side only.
func (s Subscription) resolve(err error) {
	s.done <- err
	close(s.done)
}

// Config configures a complete Client.
//
// All lower-layer settings are flattened into this single struct.
// Field ownership by layer:
//
//   - Dial / connection: Dialer, URL, Header, ReadLimit, PongTimeout
//   - Session runtime:   PingInterval, WriteTimeout, Handlers
//   - Reconnect policy:  DialTimeout, ReconnectMin, ReconnectMax
//   - Worker queue:      EventQueueSize
//   - Lifecycle hooks:   Hooks
//
// PongTimeout serves both the connection-tuning role (read-deadline
// extension on each pong) and the session config role (paired with
// PingInterval); a single field covers both because they must agree.
type Config struct {
	// === Dial / connection ===

	// Dialer is the gorilla websocket dialer. Nil falls back to
	// websocket.DefaultDialer.
	Dialer *websocket.Dialer
	// URL is the websocket endpoint URL. Required.
	URL string
	// Header carries optional handshake headers (e.g. Authorization).
	Header http.Header
	// ReadLimit is the maximum inbound frame size in bytes. Zero or
	// negative leaves the gorilla default in place.
	ReadLimit int64
	// PongTimeout is the read deadline extension applied on each pong
	// AND the matching pong-wait used when ping cadence is enabled.
	PongTimeout time.Duration

	// === Session runtime ===

	// PingInterval controls ping cadence. Zero disables pings.
	PingInterval time.Duration
	// WriteTimeout is the default websocket write timeout. Defaults
	// to 10s.
	WriteTimeout time.Duration
	// Handlers receives synchronous inbound messages.
	Handlers Handlers

	// === Reconnect policy ===

	// DialTimeout caps a single dial attempt. Defaults to 10s.
	DialTimeout time.Duration
	// ReconnectMin is the minimum reconnect backoff. Defaults to 1s.
	ReconnectMin time.Duration
	// ReconnectMax is the maximum reconnect backoff. Defaults to 60s.
	ReconnectMax time.Duration

	// === Worker queue ===

	// EventQueueSize is the buffer size of the worker's input queue.
	// Send / Subscribe / Unsubscribe push events here and the worker
	// drains them in order. Zero defaults to 256.
	EventQueueSize int

	// === Lifecycle hooks ===

	// Hooks receives lifecycle notifications and an optional caller
	// OnReady that runs after the built-in replay.
	Hooks Hooks
}

// applyDefaults normalizes zero-valued settings and validates required
// fields. Returns ErrURLRequired when URL is empty.
func (config *Config) applyDefaults() error {
	if config.URL == "" {
		return ErrURLRequired
	}
	if config.PingInterval < 0 {
		config.PingInterval = 0
	}
	if config.PingInterval > 0 && config.PongTimeout <= 0 {
		config.PongTimeout = 10 * time.Second
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = 10 * time.Second
	}
	if config.ReconnectMin <= 0 {
		config.ReconnectMin = 1 * time.Second
	}
	if config.ReconnectMax <= 0 {
		config.ReconnectMax = 60 * time.Second
	}
	if config.ReconnectMax < config.ReconnectMin {
		config.ReconnectMax = config.ReconnectMin
	}
	if config.EventQueueSize <= 0 {
		config.EventQueueSize = 256
	}
	config.Handlers = normalizeHandlers(config.Handlers)
	return nil
}
