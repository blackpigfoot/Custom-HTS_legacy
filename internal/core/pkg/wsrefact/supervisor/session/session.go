package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact/common"

	"github.com/gorilla/websocket"
)

// Config configures one live websocket session runtime.
type Config struct {
	// MaxRequestQueue limits queued outbound requests for this session.
	MaxRequestQueue int
	// PingInterval controls ping cadence for this session. Zero disables pings.
	PingInterval time.Duration
	// PongTimeout extends the pong read deadline after each pong.
	PongTimeout time.Duration
	// WriteTimeout is the default websocket write timeout.
	WriteTimeout time.Duration
	// ResponseTimeout is the default request completion timeout.
	ResponseTimeout time.Duration
	// Handlers receives synchronous inbound messages and lifecycle events.
	Handlers common.Handlers
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
	if config.MaxRequestQueue <= 0 {
		config.MaxRequestQueue = 256
	}
	if config.ResponseTimeout <= 0 {
		config.ResponseTimeout = 10 * time.Second
	}
	config.Handlers = common.NormalizeHandlers(config.Handlers)
}

// Request is one queued websocket write owned by the session send loop.
type Request struct {
	// Payload is the static payload written when PayloadGen is nil.
	Payload []byte
	// PayloadGen builds the payload at send time when non-nil.
	PayloadGen common.GenFunc
	// MessageType is the websocket opcode for this request.
	MessageType int
	// WriteTimeout overrides the default websocket write timeout when non-zero.
	WriteTimeout time.Duration
	// ResultCh receives the final request result from the send loop.
	ResultCh chan error
}

// Session owns one live websocket connection runtime.
type Session struct {
	// conn is the underlying live websocket connection.
	conn *websocket.Conn
	// ctx scopes one live session lifetime.
	ctx context.Context
	// cancel stops session-local goroutines.
	cancel context.CancelFunc
	// config stores immutable session runtime settings.
	config Config
	// reqCh serializes outbound websocket requests through the send loop.
	reqCh chan Request
	// done closes when the send loop fully exits.
	done chan struct{}
	// stopOnce guarantees one shutdown sequence.
	stopOnce sync.Once
	// sendLoopStarted reports that the send loop was launched for this session.
	sendLoopStarted atomic.Bool
}

// New creates one live websocket session runtime around an established websocket connection.
func New(parent context.Context, conn *websocket.Conn, config Config) *Session {
	config.ApplyDefaults()

	// sessionCtx scopes one live session lifecycle beneath the parent run context.
	sessionCtx, cancel := context.WithCancel(parent)
	// liveSession owns one live websocket connection runtime.
	liveSession := &Session{
		conn:   conn,
		ctx:    sessionCtx,
		cancel: cancel,
		config: config,
		reqCh:  make(chan Request, config.MaxRequestQueue),
		done:   make(chan struct{}),
	}
	liveSession.startSendLoop()
	go liveSession.watchParentCancellation(parent)
	return liveSession
}

// Enqueue puts one request onto the session request queue and waits for completion.
func (session *Session) Enqueue(ctx context.Context, request Request) error {
	if session == nil {
		return common.ErrNotConnected
	}
	if request.ResultCh == nil {
		request.ResultCh = make(chan error, 1)
	}

	select {
	case <-session.ctx.Done():
		return common.ErrNotConnected
	case <-ctx.Done():
		return ctx.Err()
	case session.reqCh <- request:
	default:
		return common.ErrQueueFull
	}

	return session.waitResponse(ctx, request.ResultCh)
}

// Serve owns the synchronous websocket read loop for this live session.
func (session *Session) Serve() error {
	for {
		// payload is the next inbound websocket payload from the live connection.
		_, payload, err := session.readMessage()
		if err != nil {
			return err
		}
		session.config.Handlers.OnMessage(payload)
	}
}

func (session *Session) startSendLoop() {
	if !session.sendLoopStarted.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer close(session.done)
		session.runSendLoop()
	}()
}

func (session *Session) runSendLoop() {
	// pingCh wakes the send loop for periodic ping frames when enabled.
	var pingCh <-chan time.Time
	if session.config.PingInterval > 0 {
		// ticker emits periodic ping wake-ups for this session.
		ticker := time.NewTicker(session.config.PingInterval)
		defer ticker.Stop()
		pingCh = ticker.C
	}

	for {
		select {
		case <-session.ctx.Done():
			return
		case request := <-session.reqCh:
			session.handleRequest(request)
		case <-pingCh:
			session.handlePing()
		}
	}
}

func (session *Session) handleRequest(request Request) {
	// payload is the concrete bytes written for this queued request.
	payload, err := session.resolvePayload(request)
	if err != nil {
		request.ResultCh <- err
		return
	}

	// messageType is the effective websocket opcode for this request.
	messageType := request.MessageType
	if messageType <= 0 {
		messageType = websocket.TextMessage
	}

	if err := session.writePayload(payload, messageType, request.WriteTimeout); err != nil {
		request.ResultCh <- err
		return
	}
	request.ResultCh <- nil
}

func (session *Session) resolvePayload(request Request) ([]byte, error) {
	if request.PayloadGen == nil {
		return request.Payload, nil
	}

	// payload is the request body generated at live-session send time.
	payload, err := request.PayloadGen(session.ctx)
	if err != nil {
		return nil, &common.OperationError{
			Op:  "generate payload",
			Err: err,
		}
	}
	return payload, nil
}

func (session *Session) readMessage() (messageType int, payload []byte, err error) {
	// messageType and payload come directly from the underlying websocket connection.
	messageType, payload, readErr := session.conn.ReadMessage()
	if readErr != nil {
		if session.ctx.Err() != nil {
			return 0, nil, session.ctx.Err()
		}
		return 0, nil, &common.OperationError{
			Op:  "read message",
			Err: readErr,
		}
	}
	return messageType, payload, nil
}

func (session *Session) waitResponse(ctx context.Context, resultCh chan error) error {
	// waitCtx bounds how long the caller waits for the queued request result.
	var waitCtx context.Context
	// cancel releases the temporary wait context resources.
	var cancel context.CancelFunc

	if _, ok := ctx.Deadline(); ok {
		waitCtx, cancel = context.WithCancel(ctx)
	} else if session.config.ResponseTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, session.config.ResponseTimeout)
	} else {
		waitCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	select {
	case err := <-resultCh:
		return err
	case <-waitCtx.Done():
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			return common.ErrResponseTimeout
		}
		return waitCtx.Err()
	case <-session.ctx.Done():
		return common.ErrNotConnected
	}
}

func (session *Session) writePayload(payload []byte, messageType int, writeTimeout time.Duration) error {
	// timeout is the effective write timeout for this payload.
	timeout := writeTimeout
	if timeout <= 0 {
		timeout = session.config.WriteTimeout
	}
	_ = session.conn.SetWriteDeadline(time.Now().Add(timeout))
	if err := session.conn.WriteMessage(messageType, payload); err != nil {
		return &common.OperationError{
			Op:  "write message",
			Err: err,
		}
	}
	return nil
}

func (session *Session) handlePing() {
	_ = session.writePayload(nil, websocket.PingMessage, session.config.WriteTimeout)
}

func (session *Session) watchParentCancellation(parent context.Context) {
	if session == nil || parent == nil {
		return
	}

	select {
	case <-parent.Done():
		_ = session.conn.Close()
	case <-session.done:
	}
}

// Stop tears down the session and waits for the send loop to exit.
func (session *Session) Stop() {
	if session == nil {
		return
	}
	session.stopOnce.Do(func() {
		session.cancel()
		_ = session.conn.Close()
		if session.sendLoopStarted.Load() {
			<-session.done
		}
	})
}
