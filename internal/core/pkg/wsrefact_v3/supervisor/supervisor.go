package supervisor

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	sess "Custom-HTS/internal/core/pkg/wsrefact_v3/session"
)

var (
	// ErrURLRequired reports that the websocket endpoint URL was empty.
	ErrURLRequired = errors.New("supervisor: URL is required")
	// ErrAlreadyRunning reports that Run was invoked more than once.
	ErrAlreadyRunning = errors.New("supervisor: already running")
	// ErrNotConnected reports that no live websocket session is exposed.
	ErrNotConnected = errors.New("supervisor: not connected")
)

// Supervisor owns reconnect, backoff, and session lifecycle for one
// websocket transport.
//
// Lifecycle:
//
//   - Run owns the reconnect loop and blocks until Stop is called.
//     Single-shot: must be invoked exactly once per supervisor.
//   - Stop signals shutdown. Non-blocking and idempotent; the caller
//     observes shutdown completion via Run's return.
//
// Server-side coordination: Stop guarantees client-side cleanup (no
// live goroutines, no open sockets) by the time Run returns.
// Server-side cleanup of the previous connection is not coordinated.
// When a caller reuses the same broker endpoint, the upper layer is
// responsible for any grace period or single-active-instance invariant
// required by the broker's connection policy.
//
// Replay and ordering: the supervisor does not replay subscriptions or
// any other domain state on reconnect. Upper layers register an
// OnReady hook to perform replay against the new session. The session
// is exposed to external Enqueue callers AFTER OnReady completes
// successfully, so replay traffic always reaches the wire before any
// external frame. While OnReady runs, external Enqueue callers see
// ErrNotConnected; they handle this like any transient disconnect.
type Supervisor struct {
	// config stores reconnect policy, flattened dial settings, and hooks.
	config Config
	// sessionFactory builds one configured session per dial attempt.
	sessionFactory sess.Factory
	// active points at the current live session. Nil between sessions
	// AND while OnReady is running; only the post-OnReady-success
	// session is exposed. This makes Connected/Reconnected events
	// mean "fully ready" so callers can treat them as the signal to
	// resume external traffic.
	active atomic.Pointer[sess.Session]

	// started guards against duplicate Run invocations.
	started atomic.Bool

	// stopCtx is the supervisor-wide cancellation root. All dial
	// contexts derive from it so Stop cancels in-flight dials directly.
	stopCtx context.Context
	// stopCancel cancels stopCtx and every child dial context.
	stopCancel context.CancelFunc
	// stopOnce guarantees Stop side effects run only once.
	stopOnce sync.Once
}

// New creates a reconnecting websocket supervisor.
func New(config Config) (*Supervisor, error) {
	if err := config.applyDefaults(); err != nil {
		return nil, err
	}

	// sessionFactory owns lower-layer dial and session construction details.
	sessionFactory := sess.Factory{
		Dialer:      config.Dialer,
		URL:         config.URL,
		Header:      config.Header,
		ReadLimit:   config.ReadLimit,
		PongTimeout: config.PongTimeout,
		SessionConfig: sess.Config{
			PingInterval: config.PingInterval,
			PongTimeout:  config.PongTimeout,
			WriteTimeout: config.WriteTimeout,
			Handlers:     config.Handlers,
		},
	}

	// stopCtx is the root cancellation context for the supervisor lifetime.
	stopCtx, stopCancel := context.WithCancel(context.Background())

	// supervisorInstance owns reconnect lifecycle for one transport.
	supervisorInstance := &Supervisor{
		config:         config,
		sessionFactory: sessionFactory,
		stopCtx:        stopCtx,
		stopCancel:     stopCancel,
	}
	return supervisorInstance, nil
}

// State returns the current transport lifecycle state. Derived from
// supervisor flags so the reported state cannot drift out of sync:
//
//   - Stopped: Run has not started, or Stop has been called.
//   - Connected: a live session is currently exposed (post-OnReady).
//   - Reconnecting: Run is active but no session is currently exposed
//     (between sessions, during a dial, or while OnReady is running).
func (supervisor *Supervisor) State() ConnState {
	if !supervisor.started.Load() || supervisor.stopCtx.Err() != nil {
		return ConnStateStopped
	}
	if supervisor.active.Load() != nil {
		return ConnStateConnected
	}
	return ConnStateReconnecting
}

// IsConnected reports whether the supervisor currently owns a live session.
func (supervisor *Supervisor) IsConnected() bool {
	return supervisor.State() == ConnStateConnected
}

// Run owns the reconnect lifecycle until Stop is called.
//
// Single-shot: a second invocation returns ErrAlreadyRunning. Returns
// nil on Stop-driven shutdown. By the time Run returns, no goroutine
// spawned by the supervisor is alive and no underlying connection is
// open.
func (supervisor *Supervisor) Run() error {
	if !supervisor.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	// attempt counts dial attempts. The first attempt is 0 (no
	// backoff); every subsequent attempt is 1+, regardless of whether
	// the previous session connected successfully and later
	// disconnected, or the previous dial / OnReady itself failed. The
	// hooks receive this value as the attempt index for Connected,
	// Reconnected, and Reconnecting events.
	attempt := 0
	for {
		if supervisor.stopCtx.Err() != nil {
			return nil
		}

		if attempt > 0 {
			supervisor.emit(ConnEvent{
				Type:    Reconnecting,
				Attempt: attempt,
				At:      time.Now(),
			})
			if !supervisor.waitBackoff(attempt) {
				return nil
			}
		}

		// liveSession is the newly dialed session candidate for this attempt.
		liveSession, err := supervisor.dial()
		if err != nil {
			if supervisor.stopCtx.Err() != nil {
				return nil
			}
			attempt++
			continue
		}

		supervisor.runSession(liveSession, attempt)
		attempt++
	}
}

// Stop signals shutdown. Non-blocking and idempotent. Safe to call from
// any goroutine, including before Run has started.
func (supervisor *Supervisor) Stop() {
	supervisor.stopOnce.Do(func() {
		// Cancel stopCtx first so any in-flight dial sees cancellation
		// immediately. Then stop the active session, if any, to wake
		// its read loop. Order matters: a dial that was about to start
		// after Stop runs sees ctx.Err() and bails without connecting.
		supervisor.stopCancel()
		if liveSession := supervisor.active.Load(); liveSession != nil {
			liveSession.Stop()
		}
	})
}

// Enqueue forwards a write request to the current active session.
//
// Returns ErrNotConnected when no live session is exposed: between
// sessions, during a dial attempt, while OnReady is running, and
// after Stop. The OnReady window is intentional: replay frames
// reach the wire through the Replayer passed to OnReady, not through
// Enqueue, which guarantees replay frames precede any external frame
// after a reconnect.
//
// Callers needing delivery guarantees across reconnects must
// implement their own retry logic, typically keyed off the
// Connected/Reconnected events delivered to Hooks.OnEvent (which
// fire only after OnReady succeeds).
func (supervisor *Supervisor) Enqueue(request WriteRequest) error {
	// liveSession is the externally visible connected session, if any.
	liveSession := supervisor.active.Load()
	if liveSession == nil {
		return ErrNotConnected
	}

	// sendErr is the lower-layer send result for this request.
	sendErr := liveSession.Send(sess.Request{
		Payload:      request.Payload,
		MessageType:  request.MessageType,
		WriteTimeout: request.WriteTimeout,
	})
	if errors.Is(sendErr, sess.ErrNotConnected) {
		return ErrNotConnected
	}
	return sendErr
}

// dial performs one dial attempt under the supervisor's DialTimeout.
//
// The dial context derives from stopCtx so Stop cancels any in-flight
// dial directly through ctx propagation.
func (supervisor *Supervisor) dial() (*sess.Session, error) {
	// dialCtx scopes one websocket dial attempt.
	dialCtx, cancel := context.WithTimeout(supervisor.stopCtx, supervisor.config.DialTimeout)
	defer cancel()
	return supervisor.sessionFactory.Dial(dialCtx)
}

// runSession owns one live session lifecycle: run OnReady, expose the
// session, emit Connected / Reconnected, serve until death, then emit
// Disconnected.
//
// Active session exposure ordering: the session is exposed AFTER
// OnReady completes successfully. While OnReady runs, external
// Enqueue callers see ErrNotConnected (active.Load() == nil). This
// makes Connected/Reconnected events mean "fully ready": replay
// done, broker state restored, and safe for external traffic.
//
// If OnReady fails, the session is torn down without ever being
// exposed; the caller never sees Connected/Reconnected for this
// attempt and the supervisor proceeds to the next reconnect.
//
// Returns when the session has been fully torn down. Serve guarantees
// self-cleanup on return; the OnReady failure path also stops the
// session before returning.
func (supervisor *Supervisor) runSession(liveSession *sess.Session, attempt int) {
	// replayer writes replay frames before the session becomes externally visible.
	replayer := sessionReplayer{session: liveSession}
	if err := supervisor.config.Hooks.OnReady(replayer, attempt); err != nil {
		// OnReady decided this session is unusable. Tear it down
		// without exposing it; loop back into reconnect. The session
		// was never observable by external Enqueue callers, so no
		// active.Store(nil) is needed. OnEvent receives a Disconnected
		// event so callers tracking lifecycle see this attempt's
		// failure.
		liveSession.Stop()
		supervisor.emit(ConnEvent{
			Type: Disconnected,
			Err:  err,
			At:   time.Now(),
		})
		return
	}

	// eventType is the ready signal emitted after OnReady succeeds.
	eventType := Connected
	if attempt > 0 {
		eventType = Reconnected
	}

	supervisor.active.Store(liveSession)
	supervisor.emit(ConnEvent{
		Type:    eventType,
		Attempt: attempt,
		At:      time.Now(),
	})

	// serveErr is the terminal error returned by the live session loop.
	serveErr := liveSession.Serve()
	supervisor.active.Store(nil)

	supervisor.emit(ConnEvent{
		Type: Disconnected,
		Err:  serveErr,
		At:   time.Now(),
	})
}

// waitBackoff sleeps for the computed backoff, returning false if Stop
// fires during the wait.
func (supervisor *Supervisor) waitBackoff(attempt int) bool {
	// timer gates the reconnect delay for this attempt.
	timer := time.NewTimer(supervisor.calcBackoff(attempt))
	defer timer.Stop()

	select {
	case <-supervisor.stopCtx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// calcBackoff returns the exponential backoff with 25% jitter for the
// given attempt count, clamped to [ReconnectMin, ReconnectMax].
func (supervisor *Supervisor) calcBackoff(attempt int) time.Duration {
	// minSeconds is the configured minimum reconnect interval in seconds.
	minSeconds := supervisor.config.ReconnectMin.Seconds()
	// maxSeconds is the configured maximum reconnect interval in seconds.
	maxSeconds := supervisor.config.ReconnectMax.Seconds()

	// backoffSeconds is the unclamped exponential backoff for this attempt.
	backoffSeconds := minSeconds * math.Pow(2, float64(attempt-1))
	if backoffSeconds > maxSeconds {
		backoffSeconds = maxSeconds
	}

	// jitter randomizes reconnect timing to avoid synchronized retries.
	jitter := backoffSeconds * 0.25 * rand.Float64()
	return time.Duration((backoffSeconds + jitter) * float64(time.Second))
}

// emit forwards one lifecycle event to the configured observer hook.
func (supervisor *Supervisor) emit(event ConnEvent) {
	supervisor.config.Hooks.OnEvent(event)
}
