package supervisor

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact/common"
	sessionsvc "Custom-HTS/_legacy/internal/core/pkg/wsrefact/supervisor/session"

	"github.com/gorilla/websocket"
)

// Hooks connects the reconnect supervisor to optional higher-level observers.
type Hooks struct {
	// OnStateChange receives transport lifecycle state transitions.
	OnStateChange func(common.ConnState)
	// OnEvent receives transport lifecycle events.
	OnEvent func(common.ConnEvent)
	// OnReady runs immediately after a new live session becomes available.
	OnReady func(attempt int)
}

// Config configures the reconnect supervisor.
type Config struct {
	// URL is the websocket endpoint URL.
	URL string
	// Headers stores HTTP headers used during websocket dial.
	Headers map[string]string
	// Dialer creates websocket connections.
	Dialer *websocket.Dialer
	// ReconnectMin is the minimum reconnect backoff.
	ReconnectMin time.Duration
	// ReconnectMax is the maximum reconnect backoff.
	ReconnectMax time.Duration
	// MaxMessageSize limits inbound websocket message size.
	MaxMessageSize int64
	// Session configures one live websocket session runtime.
	Session sessionsvc.Config
	// Hooks receives lifecycle state and reconnect-ready notifications.
	Hooks Hooks
}

// Supervisor owns reconnect, backoff, and live-session lifecycle.
type Supervisor struct {
	// config stores dial and reconnect runtime settings.
	config Config
	// active stores the current live session reference.
	active atomic.Pointer[sessionsvc.Session]
	// state stores the public transport lifecycle state.
	state atomic.Uint32
	// started prevents duplicate Run invocations.
	started atomic.Bool
}

// New creates a reconnecting websocket supervisor.
func New(config Config) (*Supervisor, error) {
	if err := config.applyDefaults(); err != nil {
		return nil, err
	}

	// svc owns reconnect and live-session lifecycle for this websocket transport.
	svc := &Supervisor{
		config: config,
	}
	svc.state.Store(uint32(common.ConnStateStopped))
	return svc, nil
}

// State returns the current reconnect supervisor lifecycle state.
func (supervisor *Supervisor) State() common.ConnState {
	if supervisor == nil {
		return common.ConnStateStopped
	}
	return common.ConnState(supervisor.state.Load())
}

// IsConnected reports whether the supervisor currently owns a live session.
func (supervisor *Supervisor) IsConnected() bool {
	return supervisor.State() == common.ConnStateConnected
}

// Run owns the reconnect lifecycle until ctx is canceled.
func (supervisor *Supervisor) Run(ctx context.Context) error {
	if supervisor == nil {
		return common.ErrNotConnected
	}
	if !supervisor.started.CompareAndSwap(false, true) {
		return common.ErrAlreadyRunning
	}

	supervisor.setState(common.ConnStateReconnecting)
	defer supervisor.shutdown()

	// attempt counts reconnect attempts after the initial dial.
	attempt := 0
	for {
		if !supervisor.waitNextAttempt(ctx, attempt) {
			return nil
		}

		// liveSession is the freshly dialed websocket session.
		liveSession, err := supervisor.connectSession(ctx)
		if err != nil {
			if supervisor.isRunContextDone(ctx, err) {
				return nil
			}
			attempt++
			continue
		}

		supervisor.attachSession(liveSession, attempt)
		readErr := liveSession.Serve()
		supervisor.detachSession(liveSession, readErr)

		if supervisor.isRunContextDone(ctx, readErr) {
			return nil
		}
		supervisor.setState(common.ConnStateReconnecting)
		attempt = 1
	}
}

// Enqueue places one write request onto the current live session queue and waits for completion.
func (supervisor *Supervisor) Enqueue(ctx context.Context, request WriteRequest) error {
	// liveSession is the current active websocket session.
	liveSession := supervisor.active.Load()
	if liveSession == nil {
		return common.ErrNotConnected
	}

	return liveSession.Enqueue(ctx, sessionsvc.Request{
		Payload:      request.Payload,
		PayloadGen:   request.PayloadGen,
		MessageType:  request.MessageType,
		WriteTimeout: request.WriteTimeout,
		ResultCh:     make(chan error, 1),
	})
}

// Send writes one direct text websocket message through the active live session.
func (supervisor *Supervisor) Send(ctx context.Context, data []byte) error {
	return supervisor.Enqueue(ctx, WriteRequest{
		Payload:     data,
		MessageType: websocket.TextMessage,
	})
}

// SendBinary writes one direct binary websocket message through the active live session.
func (supervisor *Supervisor) SendBinary(ctx context.Context, data []byte) error {
	return supervisor.Enqueue(ctx, WriteRequest{
		Payload:     data,
		MessageType: websocket.BinaryMessage,
	})
}

func (config *Config) applyDefaults() error {
	if config.URL == "" {
		return common.ErrURLRequired
	}
	if config.MaxMessageSize <= 0 {
		config.MaxMessageSize = 65536
	}
	if config.ReconnectMin <= 0 {
		config.ReconnectMin = 1 * time.Second
	}
	if config.ReconnectMax <= 0 {
		config.ReconnectMax = 60 * time.Second
	}
	if config.Dialer == nil {
		config.Dialer = &websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	}
	config.Session.ApplyDefaults()
	config.Hooks = normalizeHooks(config.Hooks)
	return nil
}

func (supervisor *Supervisor) connectSession(ctx context.Context) (*sessionsvc.Session, error) {
	// headers contains the websocket handshake headers for this dial attempt.
	headers := http.Header{}
	for key, value := range supervisor.config.Headers {
		headers.Set(key, value)
	}

	//TODO: session??conn???ш린???앹꽦?섏? 留먭퀬
	// session?앹꽦湲곕? 留뚮뱾怨?嫄곌린???ㅼ젙???명똿?대넃?쇰㈃
	// ?먮룞?쇰줈 session??戮묒븷?닿쾶 罹≪뒓??
	// 吏湲덉? conn ?앹꽦, session ?앹꽦???곕줈??conn?앸챸二쇨린 = session?앸챸二쇨린媛 ?덈쭪??

	// rawConn is the freshly established underlying websocket connection.
	rawConn, _, err := supervisor.config.Dialer.DialContext(ctx, supervisor.config.URL, headers)
	if err != nil {
		if supervisor.isRunContextDone(ctx, err) {
			return nil, ctx.Err()
		}
		return nil, &common.OperationError{
			Op:  "dial connection",
			Err: err,
		}
	}

	rawConn.SetReadLimit(supervisor.config.MaxMessageSize)
	if supervisor.config.Session.PingInterval > 0 {
		// pongWait bounds how long reads can wait between pongs.
		pongWait := supervisor.config.Session.PingInterval + supervisor.config.Session.PongTimeout
		_ = rawConn.SetReadDeadline(time.Now().Add(pongWait))
		rawConn.SetPongHandler(func(string) error {
			return rawConn.SetReadDeadline(time.Now().Add(pongWait))
		})
	}

	// liveSession wraps the live websocket connection in a session runtime.
	liveSession := sessionsvc.New(ctx, rawConn, supervisor.config.Session)
	if ctx.Err() != nil {
		liveSession.Stop()
		return nil, ctx.Err()
	}
	return liveSession, nil
}

func (supervisor *Supervisor) attachSession(liveSession *sessionsvc.Session, attempt int) {
	supervisor.active.Store(liveSession)
	supervisor.setState(common.ConnStateConnected)
	/// TODO: emit?쒓굅 ??onready濡??듯빀.
	// 洹??몄쓽 ?대깽?몃뒗 onevemt濡??듯빀.
	supervisor.emitConnected(attempt)
	// TODO: onready, onstatechange ?듯빀.
	supervisor.config.Hooks.OnReady(attempt)
}

func (supervisor *Supervisor) detachSession(liveSession *sessionsvc.Session, readErr error) {
	supervisor.active.CompareAndSwap(liveSession, nil)
	liveSession.Stop()
	supervisor.emitDisconnected(readErr)
}

func (supervisor *Supervisor) shutdown() {
	if liveSession := supervisor.active.Swap(nil); liveSession != nil {
		liveSession.Stop()
	}
	supervisor.setState(common.ConnStateStopped)
}

func (supervisor *Supervisor) setState(state common.ConnState) {
	supervisor.state.Store(uint32(state))
	supervisor.config.Hooks.OnStateChange(state)
}

func (supervisor *Supervisor) waitNextAttempt(ctx context.Context, attempt int) bool {
	if attempt == 0 {
		return true
	}

	supervisor.emitReconnecting(attempt)
	return supervisor.waitBackoff(ctx, attempt)
}

func (supervisor *Supervisor) waitBackoff(ctx context.Context, attempt int) bool {
	// backoff is the current reconnect wait duration with jitter.
	backoff := supervisor.calcBackoff(attempt)
	// timer waits for the next reconnect attempt.
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (supervisor *Supervisor) calcBackoff(attempt int) time.Duration {
	// minSeconds is the lower reconnect backoff bound in seconds.
	minSeconds := supervisor.config.ReconnectMin.Seconds()
	// maxSeconds is the upper reconnect backoff bound in seconds.
	maxSeconds := supervisor.config.ReconnectMax.Seconds()

	// backoffSeconds is the exponential reconnect delay before jitter.
	backoffSeconds := minSeconds * math.Pow(2, float64(attempt-1))
	if backoffSeconds > maxSeconds {
		backoffSeconds = maxSeconds
	}

	// jitter adds a small random spread to reconnect attempts.
	jitter := backoffSeconds * 0.25 * rand.Float64()
	return time.Duration((backoffSeconds + jitter) * float64(time.Second))
}

func (supervisor *Supervisor) isRunContextDone(ctx context.Context, err error) bool {
	if ctx == nil {
		return false
	}
	if ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (supervisor *Supervisor) emitConnected(attempt int) {
	// eventType is connected on the first session and reconnected afterwards.
	eventType := common.Connected
	if attempt > 0 {
		eventType = common.Reconnected
	}
	supervisor.emitEvent(common.ConnEvent{
		Type:    eventType,
		Attempt: attempt,
		At:      time.Now(),
	})
}

func (supervisor *Supervisor) emitDisconnected(err error) {
	supervisor.emitEvent(common.ConnEvent{
		Type: common.Disconnected,
		Err:  err,
		At:   time.Now(),
	})
}

func (supervisor *Supervisor) emitReconnecting(attempt int) {
	supervisor.emitEvent(common.ConnEvent{
		Type:    common.Reconnecting,
		Attempt: attempt,
		At:      time.Now(),
	})
}

func (supervisor *Supervisor) emitEvent(event common.ConnEvent) {
	supervisor.config.Session.Handlers.OnStatus(event)
	supervisor.config.Hooks.OnEvent(event)
}

func normalizeHooks(hooks Hooks) Hooks {
	if hooks.OnStateChange == nil {
		hooks.OnStateChange = func(common.ConnState) {}
	}
	if hooks.OnEvent == nil {
		hooks.OnEvent = func(common.ConnEvent) {}
	}
	if hooks.OnReady == nil {
		hooks.OnReady = func(int) {}
	}
	return hooks
}
