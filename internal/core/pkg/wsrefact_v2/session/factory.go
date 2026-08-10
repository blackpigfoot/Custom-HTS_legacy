package session

import (
	"context"
	"net/http"
	"time"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact_v2/common"

	"github.com/gorilla/websocket"
)

// Factory builds one configured Session per Dial call.
//
// The factory captures everything that is identical across reconnects:
// dial parameters, connection-level tuning (read limit, pong handler), and
// the session runtime config. The supervisor decides when to call Dial;
// the factory decides how to set up each connection.
type Factory struct {
	// Dialer is the gorilla websocket dialer. A nil Dialer falls back to
	// websocket.DefaultDialer.
	Dialer *websocket.Dialer
	// URL is the websocket endpoint URL. Required.
	URL string
	// Header carries optional handshake headers (e.g. Authorization).
	Header http.Header
	// ReadLimit is the maximum inbound frame size in bytes. Zero or
	// negative leaves the gorilla default in place.
	ReadLimit int64
	// PongTimeout is the read deadline extension applied on each pong.
	// Zero disables pong-driven deadline refresh; pair with
	// SessionConfig.PingInterval > 0 for a working keepalive.
	PongTimeout time.Duration
	// SessionConfig is applied to every session produced by this factory.
	SessionConfig Config
}

// Dial opens a new websocket connection, applies connection-level
// configuration, and returns a Session ready to Serve.
//
// dialCtx scopes the dial operation only; it has no relationship to the
// returned session's lifetime. Once Dial returns, the dial context can be
// canceled or expire without affecting the session.
//
// On any failure, no session is returned and any partially established
// connection is closed.
func (factory *Factory) Dial(dialCtx context.Context) (*Session, error) {
	if factory.URL == "" {
		return nil, common.ErrURLRequired
	}

	dialer := factory.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	conn, _, err := dialer.DialContext(dialCtx, factory.URL, factory.Header)
	if err != nil {
		return nil, &common.OperationError{
			Op:  "dial",
			Err: err,
		}
	}

	factory.applyConnectionTuning(conn)

	return New(conn, factory.SessionConfig), nil
}

// applyConnectionTuning installs read limit and pong handler on the raw
// connection before it is handed to the session. Done here rather than
// inside Session so Session stays focused on the runtime loops.
func (factory *Factory) applyConnectionTuning(conn *websocket.Conn) {
	if factory.ReadLimit > 0 {
		conn.SetReadLimit(factory.ReadLimit)
	}

	if factory.PongTimeout > 0 {
		pongTimeout := factory.PongTimeout
		// Initial deadline so a stuck peer is detected even before the
		// first pong arrives.
		_ = conn.SetReadDeadline(time.Now().Add(pongTimeout))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongTimeout))
		})
	}
}
