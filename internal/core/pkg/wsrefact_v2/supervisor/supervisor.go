package supervisor

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/common"
	sess "Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/session"
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
// Subscription replay: the supervisor does not replay subscriptions or
// any other domain state on reconnect. Upper layers register an OnReady
// hook to perform replay against the new session. OnReady runs before
// the session is exposed to external Enqueue callers, so replay frames
// always reach the wire before any frame from a concurrent Enqueue.
// Connected / Reconnected events are emitted only after OnReady
// succeeds, so observers can treat them as "fully ready" signals.
type Supervisor struct {
	config Config
	// active points at the current live session. Nil between sessions
	// AND while OnReady is running (replay frames go through the
	// Replayer passed to OnReady, not through active).
	active atomic.Pointer[sess.Session]

	// started guards against duplicate Run invocations.
	started atomic.Bool

	// stopCtx is the supervisor-wide cancellation root. All dial
	// contexts derive from it so Stop cancels in-flight dials directly.
	stopCtx    context.Context
	stopCancel context.CancelFunc
	stopOnce   sync.Once
}

// New creates a reconnecting websocket supervisor.
func New(config Config) (*Supervisor, error) {
	if err := config.applyDefaults(); err != nil {
		return nil, err
	}
	stopCtx, stopCancel := context.WithCancel(context.Background())
	return &Supervisor{
		config:     config,
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}, nil
}

// State returns the current transport lifecycle state. Derived from
// supervisor flags so the reported state cannot drift out of sync:
//
//   - Stopped: Run has not started, or Stop has been called.
//   - Connected: a live session is currently exposed (post-OnReady).
//   - Reconnecting: Run is active but no session is currently exposed
//     (between sessions, or during OnReady).
func (supervisor *Supervisor) State() common.ConnState {
	if !supervisor.started.Load() || supervisor.stopCtx.Err() != nil {
		return common.ConnStateStopped
	}
	if supervisor.active.Load() != nil {
		return common.ConnStateConnected
	}
	return common.ConnStateReconnecting
}

// IsConnected reports whether the supervisor currently owns a live session.
func (supervisor *Supervisor) IsConnected() bool {
	return supervisor.State() == common.ConnStateConnected
}

// Run owns the reconnect lifecycle until Stop is called.
//
// Single-shot: a second invocation returns ErrAlreadyRunning. Returns
// nil on Stop-driven shutdown. By the time Run returns, no goroutine
// spawned by the supervisor is alive and no underlying connection is
// open.
func (supervisor *Supervisor) Run() error {
	if !supervisor.started.CompareAndSwap(false, true) {
		return common.ErrAlreadyRunning
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
			supervisor.emit(common.ConnEvent{
				Type:    common.Reconnecting,
				Attempt: attempt,
				At:      time.Now(),
			})
			if !supervisor.waitBackoff(attempt) {
				return nil
			}
		}

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
// sessions, during OnReady (replay phase), and after Stop. Callers
// needing delivery guarantees across reconnects must implement their
// own retry logic, typically keyed off Connected / Reconnected events
// delivered after OnReady succeeds.
func (supervisor *Supervisor) Enqueue(request WriteRequest) error {
	liveSession := supervisor.active.Load()
	if liveSession == nil {
		return common.ErrNotConnected
	}
	return liveSession.Send(sess.Request{
		Payload:      request.Payload,
		MessageType:  request.MessageType,
		WriteTimeout: request.WriteTimeout,
	})
}

// dial performs one dial attempt under the supervisor's DialTimeout.
//
// The dial context derives from stopCtx so Stop cancels any in-flight
// dial directly through ctx propagation.
func (supervisor *Supervisor) dial() (*sess.Session, error) {
	dialCtx, cancel := context.WithTimeout(supervisor.stopCtx, supervisor.config.DialTimeout)
	defer cancel()
	return supervisor.config.Factory.Dial(dialCtx)
}

// runSession owns one live session lifecycle: run OnReady, expose the
// session, emit Connected / Reconnected, serve until death, then emit
// Disconnected.
//
// Returns when the session has been fully torn down. Serve guarantees
// self-cleanup on return; OnReady failure path also stops the session
// before returning.
func (supervisor *Supervisor) runSession(liveSession *sess.Session, attempt int) {
	// OnReady runs before the session is exposed via active. Replay
	// frames go through the dedicated Replayer and reach the wire
	// before any external Enqueue can be observed.
	replayer := sessionReplayer{session: liveSession}
	if err := supervisor.config.Hooks.OnReady(replayer, attempt); err != nil {
		// OnReady decided this session is unusable. Tear it down
		// without exposing it; loop back into reconnect. The session's
		// ping goroutine self-terminates on stopCh close; no explicit
		// join is needed before discarding the reference.
		liveSession.Stop()
		supervisor.emit(common.ConnEvent{
			Type: common.Disconnected,
			Err:  err,
			At:   time.Now(),
		})
		return
	}

	supervisor.active.Store(liveSession)

	// Connected / Reconnected is emitted only after OnReady succeeds,
	// so observers can treat the event as "fully ready for external traffic".
	eventType := common.Connected
	if attempt > 0 {
		eventType = common.Reconnected
	}
	supervisor.emit(common.ConnEvent{
		Type:    eventType,
		Attempt: attempt,
		At:      time.Now(),
	})

	serveErr := liveSession.Serve()
	supervisor.active.Store(nil)

	supervisor.emit(common.ConnEvent{
		Type: common.Disconnected,
		Err:  serveErr,
		At:   time.Now(),
	})
}

// waitBackoff sleeps for the computed backoff, returning false if Stop
// fires during the wait.
func (supervisor *Supervisor) waitBackoff(attempt int) bool {
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
	minSeconds := supervisor.config.ReconnectMin.Seconds()
	maxSeconds := supervisor.config.ReconnectMax.Seconds()

	backoffSeconds := minSeconds * math.Pow(2, float64(attempt-1))
	if backoffSeconds > maxSeconds {
		backoffSeconds = maxSeconds
	}

	jitter := backoffSeconds * 0.25 * rand.Float64()
	return time.Duration((backoffSeconds + jitter) * float64(time.Second))
}

func (supervisor *Supervisor) emit(event common.ConnEvent) {
	supervisor.config.Hooks.OnEvent(event)
}
