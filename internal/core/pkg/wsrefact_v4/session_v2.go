package wsrefact_v4

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// sessionV2 keeps the websocket connection lifetime and the IO worker
// lifetime aligned while removing the internal buffered write queue.
//
// Design goal:
//
//   - The upper layer owns request buffering and retry policy.
//   - The session owns only one live connection plus the goroutines
//     that directly operate on that connection.
//   - When the connection dies, the whole session dies with it, and
//     the upper layer swaps in a brand-new session and worker.
//
// The key difference from session is that requestCh is unbuffered.
// There is no "request was accepted into a queue but not yet owned by
// ioWorker" middle state.
//
// Ownership model:
//
//   - Before `requestCh <- req` completes, the caller still owns the
//     request. If stop happens first, the caller resolves the result
//     with ErrNotConnected.
//   - After `requestCh <- req` completes, ioWorker owns the request
//     and MUST resolve it exactly once.
//
// That ownership handoff removes the result-loss window without
// requiring a drain phase.
type sessionV2 struct {
	// conn is the owned live websocket connection.
	conn *websocket.Conn
	// config stores runtime behavior for this session.
	config sessionConfig

	// requestCh is the synchronous handoff channel from Enqueue to
	// ioWorker. Unbuffered by design: a send completes only when
	// ioWorker has accepted ownership of the request.
	requestCh chan writeReqV2

	// stopCh signals shutdown to ioWorker and pingTicker.
	stopCh chan struct{}
	// done closes when serve returns and all session goroutines have exited.
	done chan struct{}

	// started guards against duplicate serve invocations.
	started atomic.Bool
	// stopOnce guarantees shutdown side effects run only once.
	stopOnce sync.Once
}

// writeReqV2 is one frame to write plus the completion handle that
// must be resolved exactly once by the request owner.
type writeReqV2 struct {
	// payload is the outbound websocket frame body.
	payload []byte
	// messageType is the websocket opcode for this write.
	messageType int
	// writeTimeout overrides the default session write timeout.
	writeTimeout time.Duration
	// subscription is the completion handle returned to the caller.
	subscription Subscription
}

// newSessionV2 creates one live session around an established
// connection.
//
// The connection is owned by the session from this point on; the
// caller must not read, write, or close it directly.
func newSessionV2(conn *websocket.Conn, config sessionConfig) *sessionV2 {
	// sessionInstance owns the connection and all runtime goroutines.
	sessionInstance := &sessionV2{
		conn:      conn,
		config:    config,
		requestCh: make(chan writeReqV2),
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
	return sessionInstance
}

// serve runs the session lifecycle until the connection dies or Stop
// is called. Returns the read-loop error, if any.
//
// On return, all session goroutines have exited and the connection is
// closed. Callers must call serve exactly once per session.
func (session *sessionV2) serve() error {
	if !session.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	defer close(session.done)

	// ioWorkerDone closes when the single writer goroutine exits.
	ioWorkerDone := make(chan struct{})
	// pingDone closes when the ping ticker goroutine exits.
	pingDone := make(chan struct{})

	go session.ioWorker(ioWorkerDone)
	go session.pingTicker(pingDone)

	// readErr is the terminal error from the read loop.
	readErr := session.readLoop()
	session.initiateShutdown()

	<-ioWorkerDone
	<-pingDone
	return readErr
}

// Stop signals shutdown. Idempotent and non-blocking. Safe to call
// from any goroutine.
func (session *sessionV2) Stop() {
	session.initiateShutdown()
}

// Wait blocks until serve returns.
func (session *sessionV2) Wait() {
	<-session.done
}

// initiateShutdown closes stopCh and the connection exactly once.
//
// There is intentionally no request-channel close and no drain phase.
// Any request not yet handed off to ioWorker is still owned by the
// caller and fails in Enqueue when stopCh wins the select.
func (session *sessionV2) initiateShutdown() {
	session.stopOnce.Do(func() {
		close(session.stopCh)
		_ = session.conn.Close()
	})
}

// Enqueue hands one write to ioWorker and returns the completion
// handle for the caller to wait on.
//
// Important: sessionV2 does not buffer writes internally. The upper
// layer owns buffering, sequencing, and retry. This session layer only
// performs the ownership handoff to ioWorker.
//
// If stop happens before ioWorker accepts the request, Enqueue
// resolves the Subscription with ErrNotConnected itself. If the send
// succeeds, ownership has moved to ioWorker, which must resolve the
// Subscription exactly once.
func (session *sessionV2) Enqueue(payload []byte, messageType int, writeTimeout time.Duration) *Subscription {
	// subscription is the completion handle returned to the caller.
	subscription := newSubscription()
	// request is the outbound frame plus its completion handle.
	request := writeReqV2{
		payload:      payload,
		messageType:  messageType,
		writeTimeout: writeTimeout,
		subscription: subscription,
	}

	select {
	case <-session.stopCh:
		subscription.resolve(ErrNotConnected)
	case session.requestCh <- request:
		// Ownership moved to ioWorker. It now owns completion.
	}
	return &subscription
}

// ioWorker performs every connection write serially. It exits when
// stopCh closes.
//
// Because requestCh is unbuffered, every request received here is
// already owned by ioWorker. It must therefore always resolve the
// request before looping again.
func (session *sessionV2) ioWorker(done chan struct{}) {
	defer close(done)

	for {
		select {
		case <-session.stopCh:
			return
		case request := <-session.requestCh:
			// writeErr is the terminal result for this accepted request.
			writeErr := session.writeOne(request.payload, request.messageType, request.writeTimeout)
			request.subscription.resolve(writeErr)
		}
	}
}

// writeOne writes one frame to the live websocket connection.
func (session *sessionV2) writeOne(payload []byte, messageType int, writeTimeout time.Duration) error {
	// timeout is the effective deadline for this write.
	timeout := writeTimeout
	if timeout <= 0 {
		timeout = session.config.writeTimeout
	}
	if messageType <= 0 {
		messageType = websocket.TextMessage
	}
	_ = session.conn.SetWriteDeadline(time.Now().Add(timeout))
	if err := session.conn.WriteMessage(messageType, payload); err != nil {
		return &OperationError{Op: "write message", Err: err}
	}
	return nil
}

// readLoop reads inbound frames and dispatches OnMessage. Returns
// when the read fails.
func (session *sessionV2) readLoop() error {
	for {
		// payload is the inbound websocket message body for this iteration.
		_, payload, err := session.conn.ReadMessage()
		if err != nil {
			return &OperationError{Op: "read message", Err: err}
		}
		session.config.handlers.OnMessage(payload)
	}
}

// pingTicker sends periodic pings while the session is alive. Exits
// when stopCh closes.
//
// Pings use the same ioWorker so they preserve wire ordering relative
// to application writes. Because pings are heartbeats, queuePing is
// allowed to drop one when ioWorker is busy.
func (session *sessionV2) pingTicker(done chan struct{}) {
	defer close(done)

	if session.config.pingInterval <= 0 {
		return
	}

	// ticker drives periodic ping attempts for this session.
	ticker := time.NewTicker(session.config.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-session.stopCh:
			return
		case <-ticker.C:
			session.queuePing()
		}
	}
}

// queuePing attempts one non-blocking ping handoff to ioWorker.
//
// Since requestCh is unbuffered, this send succeeds only if ioWorker
// is idle and ready to accept the ping immediately. Otherwise the ping
// is dropped, which is acceptable for a heartbeat.
func (session *sessionV2) queuePing() {
	// pingRequest is the heartbeat frame handed to ioWorker.
	pingRequest := writeReqV2{
		messageType: websocket.PingMessage,
	}

	select {
	case <-session.stopCh:
		return
	case session.requestCh <- pingRequest:
	default:
		// ioWorker is busy; drop this heartbeat.
	}
}
