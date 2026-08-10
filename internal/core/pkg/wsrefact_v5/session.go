package wsrefact_v5

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// sessionConfig stores immutable runtime behavior for one live session.
type sessionConfig struct {
	// handlers stores the synchronous inbound callback set.
	handlers Handlers
	// writeTimeout stores the default websocket write timeout.
	writeTimeout time.Duration
}

// session owns one live websocket connection and its read-loop lifetime.
type session struct {
	// conn is the owned live websocket connection.
	conn *websocket.Conn
	// config stores immutable runtime behavior for this session.
	config sessionConfig
	// done closes when the read loop fully exits.
	done chan struct{}
	// started guards against duplicate read-loop starts.
	started atomic.Bool
	// stopOnce guarantees one shutdown side effect sequence.
	stopOnce sync.Once
}

// sessionReadEvent reports one read-loop outcome back to the single-thread client loop.
type sessionReadEvent struct {
	// session identifies the originating live session.
	session *session
	// readErr stores one terminal websocket read failure.
	readErr error
	// handlerErr stores one synchronous handler failure observed after a successful read.
	handlerErr error
}

// newSession wraps one established websocket connection in a thin session runtime.
func newSession(conn *websocket.Conn, config sessionConfig) *session {
	// liveSession owns the connection and one read-loop lifecycle.
	liveSession := &session{
		conn:   conn,
		config: config,
		done:   make(chan struct{}),
	}
	return liveSession
}

// Start launches the session read loop.
func (session *session) Start(eventSink chan<- muxEvent, shutdownCh <-chan struct{}) error {
	if !session.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	go session.runReadLoop(eventSink, shutdownCh)
	return nil
}

// Stop closes the owned websocket connection exactly once.
func (session *session) Stop() {
	if session == nil {
		return
	}

	session.stopOnce.Do(func() {
		_ = session.conn.Close()
	})
}

// Wait blocks until the session read loop exits.
func (session *session) Wait() {
	<-session.done
}

// writeOne writes one frame to the owned websocket connection.
func (session *session) writeOne(payload []byte, messageType int, writeTimeout time.Duration) error {
	// timeout stores the effective websocket write deadline for this operation.
	timeout := writeTimeout
	if timeout <= 0 {
		timeout = session.config.writeTimeout
	}
	if messageType <= 0 {
		messageType = websocket.TextMessage
	}

	_ = session.conn.SetWriteDeadline(time.Now().Add(timeout))
	if err := session.conn.WriteMessage(messageType, payload); err != nil {
		return &OperationError{
			Op:  "write message",
			Err: err,
		}
	}
	return nil
}

func (session *session) runReadLoop(eventSink chan<- muxEvent, shutdownCh <-chan struct{}) {
	defer close(session.done)

	for {
		// payload stores one inbound websocket message body.
		_, payload, err := session.conn.ReadMessage()
		if err != nil {
			session.publishReadEvent(eventSink, shutdownCh, sessionReadEvent{
				session: session,
				readErr: &OperationError{
					Op:  "read message",
					Err: err,
				},
			})
			return
		}

		// handlerErr stores the user callback result for this inbound frame.
		handlerErr := session.config.handlers.OnMessage(payload)
		if handlerErr == nil {
			continue
		}

		session.publishReadEvent(eventSink, shutdownCh, sessionReadEvent{
			session:    session,
			handlerErr: handlerErr,
		})
		if errors.Is(handlerErr, ErrHandlerContinue) {
			continue
		}
		return
	}
}

func (session *session) publishReadEvent(eventSink chan<- muxEvent, shutdownCh <-chan struct{}, event sessionReadEvent) {
	select {
	case eventSink <- event:
	case <-shutdownCh:
	}
}
