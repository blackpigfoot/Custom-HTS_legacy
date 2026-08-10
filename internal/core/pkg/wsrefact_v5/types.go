package wsrefact_v5

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// MessageType identifies a websocket frame opcode.
type MessageType int

const (
	// MessageTypeText is the websocket text frame opcode.
	MessageTypeText MessageType = 1
	// MessageTypeBinary is the websocket binary frame opcode.
	MessageTypeBinary MessageType = 2
)

// ConnState reports the public lifecycle state of a Client.
type ConnState int

const (
	// ConnStateStopped reports that the client is not running.
	ConnStateStopped ConnState = iota
	// ConnStateReconnecting reports that the client is dialing, replaying, or backing off.
	ConnStateReconnecting
	// ConnStateConnected reports that the client owns a fully ready live session.
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
	// Connected reports that the initial live session became fully ready.
	Connected ConnEventType = "connected"
	// Reconnected reports that a later live session became fully ready.
	Reconnected ConnEventType = "reconnected"
	// Disconnected reports that the current live session ended.
	Disconnected ConnEventType = "disconnected"
	// Reconnecting reports that the client is waiting before the next dial attempt.
	Reconnecting ConnEventType = "reconnecting"
	// SendDuringBackoff reports that one write-intent arrived while no writable worker was exposed.
	SendDuringBackoff ConnEventType = "send_during_backoff"
	// TransportWriteFailed reports that the active ioWorker observed a transport write failure.
	TransportWriteFailed ConnEventType = "transport_write_failed"
	// HandlerError reports one user handler failure exactly as observed.
	HandlerError ConnEventType = "handler_error"
	// Stopped reports that the websocket pipeline fully stopped.
	Stopped ConnEventType = "stopped"
)

// ConnEvent reports one websocket lifecycle transition or diagnostic event.
type ConnEvent struct {
	// Type identifies the lifecycle transition.
	Type ConnEventType
	// Err carries the related failure when applicable.
	Err error
	// Attempt stores the reconnect attempt count when applicable.
	Attempt int
	// Key carries the subscription key when the event relates to one logical subscription.
	Key string
	// At is the local time when the event was emitted.
	At time.Time
}

// Handlers contains synchronous websocket callbacks delivered from the session read loop.
type Handlers struct {
	// OnMessage handles one inbound websocket payload and may return policy errors.
	OnMessage func(data []byte) error
}

// noopHandlers stores the normalized nil-safe callback set.
var noopHandlers = Handlers{
	OnMessage: func([]byte) error { return nil },
}

// normalizeHandlers replaces nil callbacks with fast no-op handlers.
func normalizeHandlers(handlers Handlers) Handlers {
	if handlers.OnMessage == nil {
		handlers.OnMessage = noopHandlers.OnMessage
	}
	return handlers
}

// Hooks delivers lifecycle notifications from the Client.
type Hooks struct {
	// OnReady runs after built-in replay completes on each new live session.
	OnReady func(replayer Replayer, attempt int) error
	// OnEvent is invoked for every lifecycle or diagnostic event.
	OnEvent func(event ConnEvent)
}

// normalizeHooks replaces nil callbacks with fast no-op hooks.
func normalizeHooks(hooks Hooks) Hooks {
	if hooks.OnEvent == nil {
		hooks.OnEvent = func(ConnEvent) {}
	}
	return hooks
}

// PayloadGen produces the bytes for one subscribe or unsubscribe frame at call time.
type PayloadGen func() ([]byte, error)

// SubscribeOptions configures one logical replayable subscription.
type SubscribeOptions struct {
	// SubGen produces the subscribe payload. Required.
	SubGen PayloadGen
	// UnsubGen produces the unsubscribe payload. Required.
	UnsubGen PayloadGen
	// MessageType is the websocket opcode used for both subscribe and unsubscribe frames.
	MessageType int
	// WriteTimeout overrides the default websocket write timeout for this logical subscription.
	WriteTimeout time.Duration
}

// Replayer is the narrow interface exposed to OnReady during one activation window.
type Replayer interface {
	// Send writes one frame synchronously through the current live ioWorker.
	Send(payload []byte, options ...SendOption) error
}

// SendOption mutates the per-call websocket write parameters.
type SendOption func(*sendOptions)

// sendOptions stores normalized per-call write overrides.
type sendOptions struct {
	// messageType is the websocket opcode used for this write.
	messageType int
	// writeTimeout overrides the default websocket write timeout.
	writeTimeout time.Duration
}

// WithMessageType overrides the default text opcode for one Send call.
func WithMessageType(messageType int) SendOption {
	return func(options *sendOptions) {
		options.messageType = messageType
	}
}

// WithWriteTimeout overrides the default write timeout for one Send call.
func WithWriteTimeout(timeout time.Duration) SendOption {
	return func(options *sendOptions) {
		options.writeTimeout = timeout
	}
}

// Subscription is the asynchronous result handle returned by Client operations.
type Subscription struct {
	// done carries exactly one operation result and is then closed.
	done chan error
}

// Done returns the read-only result channel for this operation.
func (subscription Subscription) Done() <-chan error {
	return subscription.done
}

// newSubscription allocates a buffered completion handle.
func newSubscription() Subscription {
	// doneCh buffers one terminal result so workers never block on callers.
	doneCh := make(chan error, 1)
	return Subscription{done: doneCh}
}

// newResolvedSubscription allocates and resolves one completion handle immediately.
func newResolvedSubscription(err error) Subscription {
	// subscription is the immediate completion handle returned to the caller.
	subscription := newSubscription()
	subscription.resolve(err)
	return subscription
}

// resolve writes one terminal result and closes the channel.
func (subscription Subscription) resolve(err error) {
	subscription.done <- err
	close(subscription.done)
}

// Config configures a complete reconnecting websocket Client.
type Config struct {
	// Dialer is the gorilla websocket dialer. Nil falls back to websocket.DefaultDialer.
	Dialer *websocket.Dialer
	// URL is the websocket endpoint URL. Required.
	URL string
	// Header carries optional websocket handshake headers.
	Header http.Header
	// ReadLimit is the maximum inbound frame size in bytes. Zero leaves the gorilla default.
	ReadLimit int64
	// PongTimeout extends the read deadline on each pong when ping cadence is enabled.
	PongTimeout time.Duration
	// PingInterval controls outbound ping cadence. Zero disables pings.
	PingInterval time.Duration
	// WriteTimeout is the default websocket write timeout.
	WriteTimeout time.Duration
	// DialTimeout caps one dial attempt.
	DialTimeout time.Duration
	// ReconnectMin is the minimum reconnect backoff duration.
	ReconnectMin time.Duration
	// ReconnectMax is the maximum reconnect backoff duration.
	ReconnectMax time.Duration
	// EventQueueSize is the buffer size of the top-level single-thread event queue.
	EventQueueSize int
	// WriteQueueSize is the buffer size of one conn-bound ioWorker queue.
	WriteQueueSize int
	// Handlers receives synchronous inbound websocket messages.
	Handlers Handlers
	// Hooks receives lifecycle events and an optional post-replay readiness callback.
	Hooks Hooks
}

// applyDefaults normalizes zero-valued settings and validates required fields.
func (config *Config) applyDefaults() error {
	if config.URL == "" {
		return ErrURLRequired
	}
	if config.Dialer == nil {
		config.Dialer = websocket.DefaultDialer
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
	if config.WriteQueueSize <= 0 {
		config.WriteQueueSize = 256
	}
	config.Handlers = normalizeHandlers(config.Handlers)
	config.Hooks = normalizeHooks(config.Hooks)
	return nil
}
