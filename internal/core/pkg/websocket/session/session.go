package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"Custom-HTS/internal/core/pkg/websocket/common"

	"github.com/gorilla/websocket"
)

// Config configures one live websocket session runtime.
type Config struct {
	// MaxSendQueue limits direct outbound send requests for this session.
	MaxSendQueue int
	// PingInterval controls ping cadence for this session. Zero disables pings.
	PingInterval time.Duration
	// PongTimeout extends the pong read deadline after each pong.
	PongTimeout time.Duration
	// WriteTimeout is the default websocket write timeout.
	WriteTimeout time.Duration
	// ResponseTimeout is the default direct-send response timeout.
	ResponseTimeout time.Duration
	// Handlers receives synchronous inbound messages and lifecycle events.
	Handlers common.WsHandlers
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
	if config.MaxSendQueue <= 0 {
		config.MaxSendQueue = 256
	}
	if config.ResponseTimeout <= 0 {
		config.ResponseTimeout = 10 * time.Second
	}
	config.Handlers = common.NormalizeHandlers(config.Handlers)
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
	// sendCh serializes direct outbound send requests.
	sendCh chan sendRequest
	// writeSignal wakes the replay writer to drain pending intents.
	writeSignal chan struct{}
	// done closes when the write loop fully exits.
	done chan struct{}
	// writerStarted reports whether the write loop has been launched.
	writerStarted atomic.Bool
	// stopOnce guarantees one shutdown sequence.
	stopOnce sync.Once
}

// sendRequest represents one direct Send or SendBinary write request.
type sendRequest struct {
	// payload is the direct message body to write.
	payload []byte
	// messageType is the websocket opcode used for this direct write.
	messageType int
	// writeTimeout limits this direct send write.
	writeTimeout time.Duration
	// errCh returns the direct send result to the caller.
	errCh chan error
}

// New creates one live websocket session runtime around an established websocket connection.
func New(parent context.Context, conn *websocket.Conn, config Config) *Session {
	config.ApplyDefaults()

	// sessionCtx scopes one live session lifecycle beneath the parent run context.
	sessionCtx, cancel := context.WithCancel(parent)
	// session owns one live websocket connection runtime.
	session := &Session{
		conn:        conn,
		ctx:         sessionCtx,
		cancel:      cancel,
		config:      config,
		sendCh:      make(chan sendRequest, config.MaxSendQueue),
		writeSignal: make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	go session.watchParentCancellation(parent)
	return session
}

// Context returns the live session lifecycle context.
func (session *Session) Context() context.Context {
	if session == nil {
		return nil
	}
	return session.ctx
}

// Serve owns the synchronous websocket read loop for this live session.
func (session *Session) Serve() error {
	for {
		_, payload, err := session.readMessage()
		if err != nil {
			return err
		}
		session.config.Handlers.OnMessage(payload)
	}
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

// StartWriter launches the replay-aware write loop under session ownership.
func (session *Session) StartWriter(drain func()) {
	if !session.writerStarted.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer close(session.done)
		session.runWriter(drain)
	}()
	session.WakeWriter()
}

func (session *Session) runWriter(drain func()) {
	// pingCh wakes the write loop for periodic ping frames when enabled.
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
		case <-session.writeSignal:
			if drain != nil {
				drain()
			}
		case request := <-session.sendCh:
			session.handleSendRequest(request)
		case <-pingCh:
			session.handlePing()
		}
	}
}

// WakeWriter nudges the replay-aware write loop to drain pending intents.
func (session *Session) WakeWriter() {
	if session == nil {
		return
	}
	select {
	case session.writeSignal <- struct{}{}:
	default:
	}
}

// Send writes one direct websocket message through the live session.
func (session *Session) Send(ctx context.Context, data []byte, messageType int) error {
	if session == nil {
		return common.ErrNotConnected
	}

	// request carries one direct outbound websocket write.
	request := sendRequest{
		payload:      data,
		messageType:  messageType,
		writeTimeout: session.config.WriteTimeout,
		errCh:        make(chan error, 1),
	}

	select {
	case session.sendCh <- request:
	default:
		return common.ErrQueueFull
	}

	return session.waitResponse(ctx, request.errCh)
}

func (session *Session) waitResponse(ctx context.Context, errCh chan error) error {
	// waitCtx bounds how long the caller waits for the direct send result.
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
	case err := <-errCh:
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

// WritePayload writes one replay payload through the live websocket session.
func (session *Session) WritePayload(payload []byte, messageType int, writeTimeout time.Duration) error {
	// timeout is the effective write timeout for this payload.
	timeout := writeTimeout
	if timeout <= 0 {
		timeout = session.config.WriteTimeout
	}
	_ = session.conn.SetWriteDeadline(time.Now().Add(timeout))
	return session.conn.WriteMessage(messageType, payload)
}

func (session *Session) handleSendRequest(request sendRequest) {
	if err := session.WritePayload(request.payload, request.messageType, request.writeTimeout); err != nil {
		request.errCh <- &common.OperationError{
			Op:  "write message",
			Err: err,
		}
		return
	}
	request.errCh <- nil
}

func (session *Session) handlePing() {
	_ = session.WritePayload(nil, websocket.PingMessage, session.config.WriteTimeout)
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

// Stop tears down the session and waits for the write loop to exit.
func (session *Session) Stop() {
	if session == nil {
		return
	}
	session.stopOnce.Do(func() {
		session.cancel()
		_ = session.conn.Close()
		if session.writerStarted.Load() {
			<-session.done
		}
	})
}
