package replay

import (
	"net/http"
	"time"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/common"
	sess "Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/session"
	supervisorpkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/supervisor"

	"github.com/gorilla/websocket"
)

// Config configures a complete replay+supervisor+session stack.
//
// All lower-layer config is flattened into this single struct. The
// caller fills it once; NewWithSupervisor splits it back into the
// per-layer configs needed by supervisor.New and session.Factory.
//
// Field ownership by layer:
//
//   - Dial / connection: Dialer, URL, Header, ReadLimit, PongTimeout
//   - Session runtime:   PingInterval, WriteTimeout, Handlers
//   - Reconnect policy:  DialTimeout, ReconnectMin, ReconnectMax
//   - Lifecycle hook:    OnEvent (OnReady is wired internally to
//     invoke ReplayAll)
//
// PongTimeout serves both the connection-tuning role (read-deadline
// extension on each pong) and the session config role (paired with
// PingInterval); a single field covers both because they must agree.
type Config struct {
	// Dialer is the gorilla websocket dialer. Nil falls back to
	// websocket.DefaultDialer.
	Dialer *websocket.Dialer
	// URL is the websocket endpoint URL. Required.
	URL string
	// Header carries optional handshake headers (e.g. Authorization).
	Header http.Header
	// ReadLimit is the maximum inbound frame size in bytes. Zero or
	// negative leaves the gorilla default in place.
	ReadLimit int64
	// PongTimeout is the read deadline extension applied on each pong
	// AND the matching pong-wait used when ping cadence is enabled.
	// Zero with PingInterval > 0 falls back to a session default.
	PongTimeout time.Duration

	// PingInterval controls ping cadence. Zero disables pings.
	PingInterval time.Duration
	// WriteTimeout is the default websocket write timeout.
	WriteTimeout time.Duration
	// Handlers receives synchronous inbound messages and lifecycle
	// events at the session layer.
	Handlers common.Handlers

	// DialTimeout caps a single dial attempt. Zero defaults to 10s.
	DialTimeout time.Duration
	// ReconnectMin is the minimum reconnect backoff (defaults to 1s).
	ReconnectMin time.Duration
	// ReconnectMax is the maximum reconnect backoff (defaults to 60s).
	ReconnectMax time.Duration

	// OnEvent receives lifecycle transitions. May be nil.
	//
	// Connected and Reconnected fire only after ReplayAll has completed
	// successfully on the new session, so observing them means the
	// session is fully ready for external traffic.
	OnEvent func(event common.ConnEvent)
}

// NewWithSupervisor builds the full replay+supervisor+session stack.
//
// Internally:
//
//  1. A session.Factory is constructed from the dial / connection /
//     session-runtime fields.
//  2. A supervisor.Config is constructed from the reconnect-policy
//     fields plus the factory and the wired Hooks.
//  3. A *supervisor.Supervisor is created.
//  4. A *Replay is created against the supervisor and the supervisor's
//     OnReady hook is wired to invoke replay.ReplayAll.
//
// Returns the replay and the supervisor as separate values: the caller
// uses the supervisor for lifecycle (Run, Stop) and direct sends
// (Enqueue), and the replay for subscription management (Subscribe,
// Unsubscribe).
func NewWithSupervisor(config Config) (*Replay, *supervisorpkg.Supervisor, error) {
	// replayInstance is assigned after the supervisor is created.
	// The OnReady closure is not invoked until supervisor.Run starts,
	// so the pointer is initialized before the first use.
	var replayInstance *Replay

	// sessionFactory carries dial configuration and per-session runtime defaults.
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

	// supervisorConfig wires the replay hook into the reconnect lifecycle.
	supervisorConfig := supervisorpkg.Config{
		Factory:      sessionFactory,
		DialTimeout:  config.DialTimeout,
		ReconnectMin: config.ReconnectMin,
		ReconnectMax: config.ReconnectMax,
		Hooks: supervisorpkg.Hooks{
			OnReady: func(replayer supervisorpkg.Replayer, attempt int) error {
				return replayInstance.ReplayAll(replayer)
			},
			OnEvent: config.OnEvent,
		},
	}

	// transportSupervisor owns reconnect lifecycle and active-session writes.
	transportSupervisor, err := supervisorpkg.New(supervisorConfig)
	if err != nil {
		return nil, nil, err
	}

	replayInstance = New(transportSupervisor)
	return replayInstance, transportSupervisor, nil
}
