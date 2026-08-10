package wsrefact

import (
	"time"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact/common"
	replaypkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact/replay"
	supervisorpkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact/supervisor"
	sessionsvc "Custom-HTS/_legacy/internal/core/pkg/wsrefact/supervisor/session"

	"github.com/gorilla/websocket"
)

// Connection is the root reconnecting websocket transport facade.
type Connection = replaypkg.Replay

// Config configures the root reconnecting websocket transport facade.
type Config struct {
	// URL is the websocket endpoint URL.
	URL string
	// PingInterval controls websocket ping cadence. Zero disables pings.
	PingInterval time.Duration
	// PongTimeout is added to PingInterval to build the pong read deadline.
	PongTimeout time.Duration
	// ReconnectMin is the minimum reconnect backoff.
	ReconnectMin time.Duration
	// ReconnectMax is the maximum reconnect backoff.
	ReconnectMax time.Duration
	// MaxMessageSize limits inbound websocket message size.
	MaxMessageSize int64
	// MaxRequestQueue limits queued outbound requests for one live session.
	MaxRequestQueue int
	// Headers stores HTTP headers used during websocket dial.
	Headers map[string]string
	// WriteTimeout limits one websocket write.
	WriteTimeout time.Duration
	// ResponseTimeout limits how long queued writes wait for completion.
	ResponseTimeout time.Duration
	// Dialer creates websocket connections.
	Dialer *websocket.Dialer
	// Handlers receives synchronous inbound message and lifecycle callbacks.
	Handlers Handlers
}

// New creates a reconnecting websocket transport facade.
func New(config Config) (*Connection, error) {
	config.applyDefaults()

	return replaypkg.New(replaypkg.Config{
		Supervisor: supervisorpkg.Config{
			URL:            config.URL,
			Headers:        config.Headers,
			Dialer:         config.Dialer,
			ReconnectMin:   config.ReconnectMin,
			ReconnectMax:   config.ReconnectMax,
			MaxMessageSize: config.MaxMessageSize,
			Session: sessionsvc.Config{
				MaxRequestQueue: config.MaxRequestQueue,
				PingInterval:    config.PingInterval,
				PongTimeout:     config.PongTimeout,
				WriteTimeout:    config.WriteTimeout,
				ResponseTimeout: config.ResponseTimeout,
				Handlers:        common.NormalizeHandlers(config.Handlers),
			},
		},
	})
}

func (config *Config) applyDefaults() {
	if config.PingInterval < 0 {
		config.PingInterval = 0
	}
	if config.PingInterval > 0 && config.PongTimeout <= 0 {
		config.PongTimeout = 10 * time.Second
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 10 * time.Second
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
	if config.MaxRequestQueue <= 0 {
		config.MaxRequestQueue = 256
	}
	if config.ResponseTimeout <= 0 {
		config.ResponseTimeout = 10 * time.Second
	}
	if config.Dialer == nil {
		config.Dialer = &websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	}
	config.Handlers = common.NormalizeHandlers(config.Handlers)
}
