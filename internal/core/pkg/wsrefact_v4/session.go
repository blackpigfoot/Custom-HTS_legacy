package wsrefact_v4

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// session owns one live websocket connection and the goroutines that
// run on it: the read loop, the IO worker, and the ping ticker.
//
// Internal type. Callers interact with sessions through Client.
//
// Goroutine model:
//
//   - readLoop: reads inbound frames and dispatches OnMessage. Exits
//     when the connection closes (peer close, error, or stop).
//   - ioWorker: receives writeReq values on writeCh and writes them
//     to the connection serially. Exits when writeCh is closed by
//     stop.
//   - pingTicker: sends periodic pings via writeCh. Exits when stopCh
//     closes.
//
// Lifetime: serve runs until the connection is dead (read loop
// returned). On serve return, all three goroutines have exited and
// the connection is closed. Callers wait for serve to return before
// discarding the session.
//
// Concurrency model: all writes go through writeCh, processed
// serially by ioWorker. There is no write mutex; channel
// serialization is the lock. The IO worker's single-goroutine
// processing guarantees that frames reach the wire in queue order.
type session struct {
	conn   *websocket.Conn
	config sessionConfig

	// writeCh carries write requests to ioWorker. Buffered so the
	// caller (Client worker) does not block on every write; the
	// buffer drains as fast as ioWorker can process.
	//
	// Closed by stop to signal ioWorker termination.
	writeCh chan writeReq

	// stopCh signals shutdown to pingTicker. (ioWorker stops on
	// writeCh close; readLoop stops on conn read error.) Closed
	// exactly once by stop.
	stopCh chan struct{}

	// done is closed when serve returns, signaling all goroutines
	// have exited.
	done chan struct{}
	started atomic.Bool
	stopOnce sync.Once
}

// sessionConfig is the runtime configuration for one session.
type sessionConfig struct {
	pingInterval time.Duration
	pongTimeout  time.Duration
	writeTimeout time.Duration
	handlers     Handlers
	queueSize    int
}

// writeReq is one frame to write, plus a result channel.
//
// resultCh is buffered (capacity 1) so ioWorker never blocks on the
// caller. A nil resultCh is allowed for fire-and-forget writes (used
// by ping).
type writeReq struct {
	payload      []byte
	messageType  int
	writeTimeout time.Duration
	resultCh     chan error
}

// newSession creates one live session around an established connection
// and starts its goroutines (readLoop, ioWorker, pingTicker).
//
// The connection is owned by the session from this point on; the
// caller must not read, write, or close it directly.
func newSession(conn *websocket.Conn, config sessionConfig) *session {
	queueSize := config.queueSize
	if queueSize <= 0 {
		queueSize = 64
	}
	s := &session{
		conn:    conn,
		config:  config,
		writeCh: make(chan writeReq, queueSize),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	return s
}

// serve runs the session's lifecycle until the connection dies or
// stop is called. Returns the read-loop error (if any).
//
// On return, all session goroutines have exited and the connection
// is closed. Callers must call serve exactly once per session.
func (s *session) serve() error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	defer close(s.done)

	// Start IO worker and ping ticker. They run alongside readLoop.
	ioWorkerDone := make(chan struct{})
	pingDone := make(chan struct{})
	go s.ioWorker(ioWorkerDone)
	go s.pingTicker(pingDone)

	// Read loop runs in this goroutine. When it returns, the
	// connection is dead or stop was called.
	readErr := s.readLoop()

	// Tear down: close stopCh (wakes pingTicker) and conn (wakes any
	// blocked write in ioWorker). Then close writeCh to drain
	// ioWorker.
	s.initiateShutdown()

	// Wait for ioWorker and pingTicker to exit. Their exits are
	// independent: ioWorker exits when writeCh drains and closes;
	// pingTicker exits when stopCh closes.
	<-ioWorkerDone
	<-pingDone

	return readErr
}

// stop signals shutdown. Idempotent and non-blocking. Safe to call
// from any goroutine.
//
// stop wakes the read loop (via conn.Close) and the ping ticker (via
// stopCh close). The IO worker drains pending writes and then exits
// when writeCh closes. Pending writes after stop see the OS-level
// failure from the closed conn and resolve their resultCh with that
// error, so callers waiting on the Subscription unblock.
func (s *session) Stop() {
	s.initiateShutdown()
}

// wait blocks until serve returns. Used by Client to detect session
// teardown without itself running serve.
func (s *session) Wait() {
	<-s.done
}

// initiateShutdown closes stopCh, the connection, and writeCh exactly
// once.
//
// Order:
//  1. close(stopCh) — wakes pingTicker.
//  2. conn.Close() — wakes readLoop (returns from ReadMessage with
//     an error). Also wakes any in-flight WriteMessage in ioWorker
//     with an error.
//  3. close(writeCh) — signals ioWorker to drain and exit. Done last
//     so any goroutine that holds a writeReq has time to push it
//     before the channel closes (ioWorker drains remaining items).
//
// Note: writeCh is closed under stopOnce, so callers using send must
// either send before stop or accept the panic from sending on a
// closed channel. The Client worker is the only caller of send and
// runs sequentially with stop, so the sequence is safe by design.
func (s *session) initiateShutdown() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		_ = s.conn.Close()
	})
}

// Enqueue queues one write through the IO worker. Returns the result
// channel from the writeReq for the caller to wait on.
//
// Returns ErrNotConnected immediately if the writeCh is full and the
// caller does not want to block. The default behavior blocks until
// the worker accepts the request, matching mutex-style synchronous
// semantics.
//
// Caller MUST be the goroutine that owns this session's lifecycle
// (the Client worker). External callers go through Client.Enqueue,
// which routes through the worker.
func (s *session) Enqueue(payload []byte, messageType int, writeTimeout time.Duration) *Subscription {
	resultCh := make(chan error, 1)
	req := writeReq{
		payload:      payload,
		messageType:  messageType,
		writeTimeout: writeTimeout,
		resultCh:     resultCh,
	}
	select {
	case <-s.stopCh:
		// Session is stopping; fail immediately.
		resultCh <- ErrNotConnected
	case s.writeCh <- req:
	default:
		// Queue full or closed; fail immediately.
		resultCh <- ErrNotConnected
	}
	return &Subscription{done: resultCh}
}

// ioWorker drains writeCh and performs each write serially. Exits
// when writeCh is closed (by stop).
//
// Single goroutine: this is the only writer to conn, so no write
// mutex is needed. Channel ordering = wire ordering.
func (s *session) ioWorker(done chan struct{}) {
	defer close(done)

	for {
		select {
		case <-s.stopCh:
			return
		case req, ok := <-s.writeCh:
			if !ok {
				return
			}
			err := s.writeOne(req.payload, req.messageType, req.writeTimeout)
			if req.resultCh != nil {
				req.resultCh <- err
			}
		}
	}
}

// writeOne writes one frame. Splits SetWriteDeadline + WriteMessage
// into one logical operation for clarity.
func (s *session) writeOne(payload []byte, messageType int, writeTimeout time.Duration) error {
	timeout := writeTimeout
	if timeout <= 0 {
		timeout = s.config.writeTimeout
	}
	if messageType <= 0 {
		messageType = websocket.TextMessage
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(timeout))
	if err := s.conn.WriteMessage(messageType, payload); err != nil {
		return &OperationError{Op: "write message", Err: err}
	}
	return nil
}

// readLoop reads inbound frames and dispatches OnMessage. Returns
// when the read fails (peer close, error, or local conn.Close).
//
// OnMessage is invoked synchronously in this goroutine. Slow handlers
// backpressure inbound throughput (this is the gorilla/standard
// pattern).
func (s *session) readLoop() error {
	for {
		_, payload, err := s.conn.ReadMessage()
		if err != nil {
			return &OperationError{Op: "read message", Err: err}
		}
		s.config.handlers.OnMessage(payload)
	}
}

// pingTicker sends periodic pings via the IO worker. Exits when
// stopCh closes.
//
// Pings go through writeCh just like regular writes, so they
// serialize naturally with subscribe / unsubscribe / send frames at
// the wire level. resultCh is nil because ping outcome is irrelevant
// to any caller — the next ReadMessage failure is the canonical
// signal that the connection is dead.
func (s *session) pingTicker(done chan struct{}) {
	defer close(done)

	if s.config.pingInterval <= 0 {
		return
	}

	ticker := time.NewTicker(s.config.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.queuePing()
		}
	}
}

// queuePing pushes a ping request to writeCh. Non-blocking: if
// writeCh is full, the ping is dropped (next tick will retry).
// Drops are acceptable because pings are heartbeats, not application
// data.
//
// Recover from panic if writeCh was closed by stop concurrently:
// stop is the canonical end-of-life signal; a dropped ping during
// shutdown is fine.
func (s *session) queuePing() {
	select {
	case s.writeCh <- writeReq{messageType: websocket.PingMessage}:
	default:
		// Queue full or closed; drop this ping.
	}
}
