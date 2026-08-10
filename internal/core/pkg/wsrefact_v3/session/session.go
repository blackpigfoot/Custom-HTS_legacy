package session

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	// ErrURLRequired reports that the websocket endpoint URL was empty.
	ErrURLRequired = errors.New("session: URL is required")
	// ErrNotConnected reports that the session is shutting down or already closed.
	ErrNotConnected = errors.New("session: not connected")
)

// MessageHandler handles one inbound websocket payload synchronously.
type MessageHandler func(data []byte)

// Handlers contains synchronous websocket callbacks owned by the session layer.
type Handlers struct {
	// OnMessage handles one inbound websocket payload.
	//
	// OnMessage is called synchronously by the read loop and must return
	// promptly. The data slice is valid only until OnMessage returns; copy it if
	// the handler stores it or passes it to another goroutine.
	OnMessage MessageHandler
}

var noopHandlers = Handlers{
	// OnMessage returns immediately when no message handler is configured.
	OnMessage: func([]byte) {},
}

// NormalizeHandlers replaces nil callbacks with fast no-op handlers.
func NormalizeHandlers(handlers Handlers) Handlers {
	if handlers.OnMessage == nil {
		handlers.OnMessage = noopHandlers.OnMessage
	}
	return handlers
}

// Config configures one live websocket session runtime.
type Config struct {
	// PingInterval controls ping cadence for this session. Zero disables pings.
	PingInterval time.Duration
	// PongTimeout extends the pong read deadline after each pong.
	PongTimeout time.Duration
	// WriteTimeout is the default websocket write timeout.
	WriteTimeout time.Duration
	// Handlers receives synchronous inbound message callbacks.
	Handlers Handlers
}

// ApplyDefaults normalizes zero-valued runtime settings.
func (config *Config) ApplyDefaults() {
	if config.PingInterval < 0 {
		config.PingInterval = 0
	}
	if config.PingInterval > 0 && config.PongTimeout <= 0 {
		config.PongTimeout = 10 * time.Second
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
	}
	config.Handlers = NormalizeHandlers(config.Handlers)
}

// Request is one websocket write.
//
// The caller is responsible for producing the final payload bytes; Session
// does not transform, encode, or generate payloads. If freshness (nonce,
// timestamp) matters, the caller produces the payload immediately before
// calling Send.
type Request struct {
	// Payload is the raw bytes written to the connection.
	Payload []byte
	// MessageType is the websocket opcode for this request.
	// Defaults to TextMessage when zero or negative.
	MessageType int
	// WriteTimeout overrides the default websocket write timeout when non-zero.
	WriteTimeout time.Duration
}

// Session owns one live websocket connection runtime.
//
// Single-use: dial a connection, run Serve, then discard. Reconnection,
// replay, and ordering across or within sessions are upper-layer
// responsibilities; Session only guarantees that one Send call atomically
// writes one frame to the connection.
//
// Ordering: Session does not reorder, queue, or sequence requests.
// Concurrent Send calls from multiple goroutines are serialized by the
// write lock, but the order in which contending callers acquire the lock
// is not guaranteed (Go's sync.Mutex is unfair). Callers requiring a
// specific ordering must coordinate at a higher layer, typically by
// holding their own lock around the dependent Send calls.
//
// Lifecycle:
//
//   - Serve runs the read loop and returns when the connection fails or
//     Stop is called. By the time Serve returns, the connection is
//     closed and the ping goroutine has exited.
//   - Stop signals external termination. Idempotent and non-blocking;
//     the caller of Serve observes complete shutdown via Serve's return.
type Session struct {
	// conn is the owned live websocket connection.
	conn *websocket.Conn
	// config stores runtime behavior for this live connection.
	config Config

	// writeLock serializes conn writes. Held across SetWriteDeadline +
	// WriteMessage as a single critical section. Send and sendPing
	// share this lock since both write frames to the same connection.
	writeLock sync.Mutex

	// closed is set under writeLock when the session is shutting down.
	// Senders that acquire writeLock check this flag before writing.
	closed atomic.Bool

	// stopCh signals shutdown to the ping goroutine.
	// Closed exactly once by initiateShutdown.
	stopCh chan struct{}
	// pingDone closes when the ping goroutine exits. Serve waits on
	// this before returning so no goroutine outlives Serve.
	pingDone chan struct{}

	// stopOnce guarantees shutdown side effects run only once.
	stopOnce sync.Once
}

// New creates one live websocket session around an established connection.
// The connection is owned by the session from this point on; the caller
// must not read, write, or close it directly.
//
// Most callers should use Factory.Dial instead, which wraps connection
// setup and session creation.
func New(conn *websocket.Conn, config Config) *Session {
	config.ApplyDefaults()

	// sessionInstance owns the connection and all runtime goroutines.
	sessionInstance := &Session{
		conn:     conn,
		config:   config,
		stopCh:   make(chan struct{}),
		pingDone: make(chan struct{}),
	}

	go sessionInstance.pingLoop()
	return sessionInstance
}

// Send writes one frame to the connection.
//
// Send is synchronous: when it returns nil, the frame has been handed to
// the OS write buffer. This does not imply server receipt or
// acknowledgement; application-level acknowledgements (subscription
// confirmations, error replies) arrive later through the inbound read
// loop and must be correlated by the upper layer.
//
// Send blocks while another goroutine holds the write lock. Acquisition
// order among contending callers is not guaranteed, so callers requiring
// "first call wins" semantics must coordinate at a higher layer.
//
// Returns ErrNotConnected when the session is shutting down or has
// already been stopped. Returns an OperationError wrapping the
// underlying failure for write errors.
func (session *Session) Send(request Request) error {
	session.writeLock.Lock()
	defer session.writeLock.Unlock()

	// Re-check closed under the lock: Stop may have run while this
	// caller was waiting for the lock. The truthful answer to a write
	// against a torn-down session is ErrNotConnected, not a stale
	// success or a low-level write error.
	if session.closed.Load() {
		return ErrNotConnected
	}

	// messageType is the effective websocket opcode for this write.
	messageType := request.MessageType
	if messageType <= 0 {
		messageType = websocket.TextMessage
	}
	return session.writeLocked(request.Payload, messageType, request.WriteTimeout)
}

// Serve owns the read loop until the connection fails or Stop is called.
// On return, all session goroutines have exited and the connection is
// closed. Must be called exactly once per session.
func (session *Session) Serve() error {
	defer func() {
		session.initiateShutdown()
		<-session.pingDone
	}()

	for {
		// payload is the inbound websocket message body for this iteration.
		_, payload, err := session.conn.ReadMessage()
		if err != nil {
			return &OperationError{Op: "read message", Err: err}
		}
		session.config.Handlers.OnMessage(payload)
	}
}

// Stop signals external termination. Idempotent, non-blocking, and safe
// to call from any goroutine including before Serve has started.
//
// Stop wakes the read loop (via conn.Close) and the ping loop (via
// stopCh). Send callers waiting on writeLock are not woken directly,
// but acquire the lock in turn, observe closed=true, and return
// ErrNotConnected microseconds later (no I/O occurs under the lock once
// closed is set).
//
// The ping goroutine exits asynchronously after stopCh closes. Callers
// that ran Serve will see the ping goroutine joined inside Serve's
// defer; callers that skip Serve do not need to wait for it because
// the goroutine self-terminates and holds no resources beyond its
// stack.
func (session *Session) Stop() {
	session.initiateShutdown()
}

// initiateShutdown publishes the closed flag and closes the connection
// exactly once.
//
// closed is published via atomic.Store, which provides the memory
// ordering needed for subsequent writeLock acquirers to observe it.
// In-flight writes already inside writeLocked are not interrupted by
// this flag; they are interrupted by conn.Close, which causes their
// pending WriteMessage to fail. Callers receive an OperationError in
// that case, which accurately reflects "the connection died while my
// write was in flight".
//
// Send callers that acquire writeLock after this Store see closed=true
// and return ErrNotConnected without attempting any I/O.
func (session *Session) initiateShutdown() {
	session.stopOnce.Do(func() {
		session.closed.Store(true)
		close(session.stopCh)
		_ = session.conn.Close()
	})
}

// pingLoop sends periodic pings while the session is alive. Exits when
// stopCh is closed. If PingInterval is zero, the goroutine returns
// immediately and Serve's wait on pingDone resolves instantly.
func (session *Session) pingLoop() {
	defer close(session.pingDone)

	if session.config.PingInterval <= 0 {
		return
	}

	// ticker drives ping cadence for the live session.
	ticker := time.NewTicker(session.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-session.stopCh:
			return
		case <-ticker.C:
			session.sendPing()
		}
	}
}

// sendPing writes one ping frame under the same write lock as Send,
// so ping frames never interleave with regular frames at the byte level.
func (session *Session) sendPing() {
	session.writeLock.Lock()
	defer session.writeLock.Unlock()

	if session.closed.Load() {
		return
	}
	_ = session.writeLocked(nil, websocket.PingMessage, session.config.WriteTimeout)
}

// writeLocked performs the frame write. Caller MUST hold writeLock.
// Splitting this out lets Send and sendPing share the SetWriteDeadline +
// WriteMessage critical section without duplicating the locking discipline.
func (session *Session) writeLocked(payload []byte, messageType int, writeTimeout time.Duration) error {
	// timeout is the effective write deadline for this frame.
	timeout := writeTimeout
	if timeout <= 0 {
		timeout = session.config.WriteTimeout
	}
	_ = session.conn.SetWriteDeadline(time.Now().Add(timeout))
	if err := session.conn.WriteMessage(messageType, payload); err != nil {
		return &OperationError{Op: "write message", Err: err}
	}
	return nil
}
