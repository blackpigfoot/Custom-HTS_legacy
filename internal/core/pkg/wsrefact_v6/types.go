package wsrefact_v6

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Subscription is the asynchronous result handle returned by caller-visible writes.
type Subscription struct {
	// done stores one terminal write result and then closes.
	done chan error
	// once guarantees that each subscription publishes one terminal result at most once.
	once sync.Once
}

// Done returns the read-only completion channel for this write.
func (subscription *Subscription) Done() <-chan error {
	return subscription.done
}

// newSubscription allocates one buffered completion handle.
func newSubscription() *Subscription {
	// doneCh buffers one terminal result so worker and mux loops never block on abandoned callers.
	doneCh := make(chan error, 1)
	return &Subscription{done: doneCh}
}

// resolve writes one terminal result and closes the completion channel.
func (subscription *Subscription) resolve(err error) {
	subscription.once.Do(func() {
		subscription.done <- err
		close(subscription.done)
	})
}

// WriteIntentAction classifies one logical outbound websocket action.
type WriteIntentAction string

const (
	// WriteIntentActionSubscribe asks the shared encoder to build one subscribe payload.
	WriteIntentActionSubscribe WriteIntentAction = "subscribe"
	// WriteIntentActionUnsubscribe asks the shared encoder to build one unsubscribe payload.
	WriteIntentActionUnsubscribe WriteIntentAction = "unsubscribe"
	// WriteIntentActionPing asks the shared encoder to build one websocket ping frame.
	WriteIntentActionPing WriteIntentAction = "ping"
)

// WriteIntent stores one logical outbound websocket instruction owned by upper layers.
type WriteIntent struct {
	// Key stores the caller-owned subscription key or routing identifier.
	Key string
	// Action stores the logical websocket action that the shared encoder should translate.
	Action WriteIntentAction
}

// EncodedWrite stores one fully encoded websocket frame produced by the shared encoder.
type EncodedWrite struct {
	// Payload stores the outbound websocket frame body.
	Payload []byte
	// MessageType stores the websocket opcode that should carry Payload.
	MessageType int
}

// WriteEncoder translates one logical write intent into one websocket frame.
type WriteEncoder interface {
	// EncodeWrite builds one websocket frame from one immutable logical write intent.
	EncodeWrite(intent WriteIntent) (EncodedWrite, error)
}

// WriteEncoderFunc adapts one plain function into the WriteEncoder interface.
type WriteEncoderFunc func(intent WriteIntent) (EncodedWrite, error)

// FatalEvent carries the deterministic failure cause that triggered mux self-shutdown.
//
// The mux publishes exactly one FatalEvent before draining and exiting its run loop.
// The client side fatal watcher consumes it and reuses the normal Stop path so that
// further caller submissions are rejected through the same code path as a user Stop.
type FatalEvent struct {
	// Err stores the underlying failure, typically an *EncodeWriteError.
	Err error
}

// userCommandKind classifies one caller-originated mux command.
type userCommandKind uint8

const (
	// userCommandKindSubscribe records one logical desired subscribe intent.
	userCommandKindSubscribe userCommandKind = iota + 1
	// userCommandKindUnsubscribe removes one logical desired subscribe intent.
	userCommandKindUnsubscribe
)

// userCommand stores one immutable caller-visible mux command.
type userCommand struct {
	// Kind identifies which logical mux operation should run.
	Kind userCommandKind
	// Key stores the logical subscription key affected by this command.
	Key string
	// Subscription stores the caller-visible completion handle for this command.
	Subscription *Subscription
}

// ioCommandType identifies one serialized io action handled by the worker loop.
type ioCommandType string

const (
	// ioCommandTypeConnect asks the worker to perform one initial dial attempt.
	ioCommandTypeConnect ioCommandType = "connect"
	// ioCommandTypeReconnect asks the worker to keep retrying until one dial succeeds or the upper-layer context ends.
	ioCommandTypeReconnect ioCommandType = "reconnect"
	// ioCommandTypeWrite asks the worker to encode one write intent and send one websocket frame through the current live connection.
	ioCommandTypeWrite ioCommandType = "write"
	// ioCommandTypeReplayWrite asks the worker to replay one desired subscribe intent without any caller-visible completion handle.
	ioCommandTypeReplayWrite ioCommandType = "replay_write"
	// ioCommandTypePing asks the worker to emit one heartbeat ping frame without any caller-visible completion handle.
	ioCommandTypePing ioCommandType = "ping"
)

// ioCommand stores one immutable io action handed to the single worker loop.
type ioCommand struct {
	// Type identifies which worker action should run.
	Type ioCommandType
	// Intent stores the logical outbound websocket instruction when Type is one write command.
	Intent *WriteIntent
	// Subscription stores the asynchronous completion handle for one live caller-visible write command.
	//
	// Replay writes do not use this field and instead travel through ioCommandTypeReplayWrite.
	Subscription *Subscription
	// DialCtx stores the upper-layer cancellation policy for one connect or reconnect command.
	DialCtx context.Context
	// PublishWriteFailure controls whether one failed write should be forwarded into the mux event sink.
	PublishWriteFailure bool
}

// ioWorkerEventKind classifies one internal worker lifecycle event published back to the mux loop.
type ioWorkerEventKind uint8

const (
	// ioWorkerEventKindConnected reports that one worker completed its initial dial.
	ioWorkerEventKindConnected ioWorkerEventKind = iota + 1
	// ioWorkerEventKindConnectFailed reports that one worker failed its initial dial.
	ioWorkerEventKindConnectFailed
	// ioWorkerEventKindReconnected reports that one worker completed its reconnect loop.
	ioWorkerEventKindReconnected
	// ioWorkerEventKindWriteFailed reports that one worker encode failed after command acceptance.
	ioWorkerEventKindWriteFailed
	// ioWorkerEventKindDisconnected reports that the read loop detected a connection drop.
	ioWorkerEventKindDisconnected
	// ioWorkerEventKindPingTick reports that the heartbeat ticker fired one ping tick.
	//
	// The mux run loop applies state and epoch checks before forwarding the actual ping
	// write to the worker mailbox so stale ticks generated by a previous connection
	// generation never reach the io worker.
	ioWorkerEventKindPingTick
)

// ioWorkerEvent stores one immutable internal worker event routed through the mux loop.
type ioWorkerEvent struct {
	// Kind classifies the worker lifecycle event semantics.
	Kind ioWorkerEventKind
	// WorkerIndex identifies which owned worker produced this event.
	WorkerIndex int
	// Intent stores the logical write intent related to one write failure event.
	Intent WriteIntent
	// Err stores the worker failure payload when Kind reports one failure.
	Err error
	// Epoch carries the heartbeat generation that produced this event when Kind is ioWorkerEventKindPingTick.
	Epoch uint64
}

// MuxWorkerState classifies one owned worker connectivity phase exposed to placement policy code.
type MuxWorkerState uint8

const (
	// MuxWorkerStateConnecting reports that the worker is dialing for the first time.
	MuxWorkerStateConnecting MuxWorkerState = iota + 1
	// MuxWorkerStateReconnecting reports that the worker is retrying after one failure.
	MuxWorkerStateReconnecting
	// MuxWorkerStateConnected reports that the worker currently accepts live writes.
	MuxWorkerStateConnected
	// MuxWorkerStateStopped reports that the worker is permanently stopped.
	MuxWorkerStateStopped
)

// MuxWorkerSnapshot stores one caller-visible worker placement snapshot.
type MuxWorkerSnapshot struct {
	// Index stores the stable mux-local worker slot.
	Index int
	// State stores the current worker connectivity phase.
	State MuxWorkerState
	// SubscriptionCount stores the number of desired subscriptions assigned to this worker.
	SubscriptionCount int
}

// PlacementPolicy chooses which worker should own one newly requested logical subscription key.
type PlacementPolicy func(key string, workers []MuxWorkerSnapshot) (int, error)

// dialFactory dials one fresh raw websocket connection for the worker runtime.
type dialFactory struct {
	// Dialer stores the gorilla websocket dialer selected during worker construction.
	Dialer *websocket.Dialer
	// URL stores the websocket endpoint URL. It must not be empty.
	URL string
	// Header stores optional websocket handshake headers.
	Header http.Header
}

// ioWorkerConfig stores one worker-owned io runtime configuration.
type ioWorkerConfig struct {
	// WorkerIndex stores the mux-local worker slot published with internal events.
	WorkerIndex int
	// CommandCh stores the serialized mailbox consumed by the worker loop.
	CommandCh <-chan ioCommand
	// EventSink stores the mux-owned internal worker event channel.
	EventSink chan<- ioWorkerEvent
	// WriteEncoder stores the shared logical-to-wire encoder used by every write command.
	WriteEncoder WriteEncoder
	// OnConnected observes each successful live websocket connection.
	OnConnected func(workerIndex int, conn *websocket.Conn)
	// OnMessage is called for every inbound websocket frame received by the internal read loop.
	OnMessage func(workerIndex int, msgType int, data []byte)
	// Dialer stores the gorilla websocket dialer used for dial attempts.
	Dialer *websocket.Dialer
	// URL stores the websocket endpoint used for dial attempts.
	URL string
	// Header stores the optional websocket handshake headers used for dial attempts.
	Header http.Header
	// DefaultWriteTimeout stores the fallback websocket write timeout used for outbound frames.
	DefaultWriteTimeout time.Duration
	// DialTimeout caps one dial attempt when greater than zero.
	DialTimeout time.Duration
	// ReconnectMin stores the minimum reconnect delay duration. Zero retries immediately.
	ReconnectMin time.Duration
	// ReconnectMax stores the maximum reconnect delay duration. Zero leaves reconnect delay uncapped.
	ReconnectMax time.Duration
	// PingInterval stores the heartbeat period between automatic ping writes. Zero disables heartbeat.
	PingInterval time.Duration
}

// MuxWorkerConfig stores the configuration used to build one owned io worker.
type MuxWorkerConfig struct {
	// WriteEncoder stores the logical write encoder shared by this worker.
	WriteEncoder WriteEncoder
	// OnConnected observes each successful live websocket connection.
	OnConnected func(workerIndex int, conn *websocket.Conn)
	// OnMessage is called for every inbound websocket frame received by the internal read loop.
	OnMessage func(workerIndex int, msgType int, data []byte)
	// Dialer stores the gorilla websocket dialer used for dial attempts.
	Dialer *websocket.Dialer
	// URL stores the websocket endpoint URL used for dial attempts.
	URL string
	// Header stores the optional websocket handshake headers.
	Header http.Header
	// QueueSize stores the worker command buffer capacity.
	//
	// Replay enqueues one write per desired subscription back-to-back without waiting,
	// so QueueSize should be at least the expected maximum desired subscription count
	// for this worker plus a small headroom for concurrent live writes. Otherwise the
	// blocking replay send pauses the mux loop until the worker drains.
	QueueSize int
	// DefaultWriteTimeout stores the fallback websocket write timeout for live writes.
	DefaultWriteTimeout time.Duration
	// DialTimeout stores the dial timeout applied to each attempt.
	DialTimeout time.Duration
	// ReconnectMin stores the minimum reconnect delay duration.
	ReconnectMin time.Duration
	// ReconnectMax stores the maximum reconnect delay duration.
	ReconnectMax time.Duration
	// PingInterval stores the heartbeat period between automatic ping writes. Zero disables heartbeat.
	PingInterval time.Duration
}

// MuxConfig stores the top-level mux runtime configuration.
type MuxConfig struct {
	// Workers stores the owned worker configurations managed by this mux.
	Workers []MuxWorkerConfig
	// QueueSize stores the mux command and internal event buffer capacity.
	QueueSize int
	// PlacementPolicy stores the optional worker-selection strategy for new subscriptions.
	PlacementPolicy PlacementPolicy
}

// muxRunState classifies the top-level mux lifecycle.
type muxRunState uint32

const (
	// muxRunStateReady reports that the mux has been constructed but Run has not started yet.
	muxRunStateReady muxRunState = iota + 1
	// muxRunStateRunning reports that the mux loop is actively processing commands.
	muxRunStateRunning
	// muxRunStateStopping reports that the mux is no longer accepting logical work and is waiting for commandCh drain.
	muxRunStateStopping
	// muxRunStateStopped reports that the mux loop has fully exited.
	muxRunStateStopped
)

// muxSubscription stores one logical desired subscription assignment owned by the mux.
type muxSubscription struct {
	// WorkerIndex stores the worker slot chosen for this logical subscription.
	WorkerIndex int
	// Intent stores the replayable subscribe intent kept in desired state.
	Intent WriteIntent
}

// muxWorkerRuntime stores one fully constructed worker owned by the mux.
type muxWorkerRuntime struct {
	// CommandCh stores the worker mailbox consumed by the io worker loop.
	CommandCh chan ioCommand
	// Worker stores the owned serialized websocket io executor.
	Worker *ioWorker
	// State stores the current worker connectivity phase.
	State MuxWorkerState
	// DesiredKeys stores stable replay order for desired logical subscriptions assigned to this worker.
	DesiredKeys []string
	// DialCancel stores the cancellation handle for one in-flight connect or reconnect program.
	DialCancel context.CancelFunc
	// PingInterval stores the heartbeat period for this worker. Zero disables heartbeat.
	PingInterval time.Duration
	// PingCancel stores the cancellation handle for the heartbeat ticker goroutine that
	// is currently active for this worker. Set when a connection enters Connected and
	// cleared whenever the worker leaves Connected.
	PingCancel context.CancelFunc
	// HeartbeatEpoch identifies the current heartbeat generation. The mux increments it
	// every time the worker enters Connected so stale ping ticks generated by a previous
	// generation can be deterministically dropped before reaching the io worker.
	HeartbeatEpoch uint64
}
