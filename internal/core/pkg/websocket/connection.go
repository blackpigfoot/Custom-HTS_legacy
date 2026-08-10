package websocket

import (
	"time"

	"Custom-HTS/internal/core/pkg/websocket/common"
	"Custom-HTS/internal/core/pkg/websocket/replay"
	"Custom-HTS/internal/core/pkg/websocket/session"
	"Custom-HTS/internal/core/pkg/websocket/supervisor"

	"github.com/gorilla/websocket"
)

// WsConnection is the root reconnecting websocket transport facade.
type WsConnection = replay.Replay

// WsConfig configures the root reconnecting websocket transport facade.
type WsConfig struct {
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
	// MaxSendQueue limits direct outbound send requests for one live session.
	MaxSendQueue int
	// Headers stores HTTP headers used during websocket dial.
	Headers map[string]string
	// WriteTimeout limits one websocket write.
	WriteTimeout time.Duration
	// ResponseTimeout limits how long Send waits for completion.
	ResponseTimeout time.Duration
	// Dialer creates websocket connections.
	Dialer *websocket.Dialer
	// Handlers receives synchronous inbound message and lifecycle callbacks.
	Handlers WsHandlers
}

// New creates a reconnecting websocket transport facade.
func New(config WsConfig) (*WsConnection, error) {
	config.applyDefaults()

	return replay.New(replay.Config{
		Supervisor: supervisor.Config{
			URL:            config.URL,
			Headers:        config.Headers,
			Dialer:         config.Dialer,
			ReconnectMin:   config.ReconnectMin,
			ReconnectMax:   config.ReconnectMax,
			MaxMessageSize: config.MaxMessageSize,
			Session: session.Config{
				MaxSendQueue:    config.MaxSendQueue,
				PingInterval:    config.PingInterval,
				PongTimeout:     config.PongTimeout,
				WriteTimeout:    config.WriteTimeout,
				ResponseTimeout: config.ResponseTimeout,
				Handlers:        common.NormalizeHandlers(config.Handlers),
			},
		},
	})
}

func (config *WsConfig) applyDefaults() {
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
	if config.MaxSendQueue <= 0 {
		config.MaxSendQueue = 256
	}
	if config.ResponseTimeout <= 0 {
		config.ResponseTimeout = 10 * time.Second
	}
	if config.Dialer == nil {
		config.Dialer = &websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	}
	config.Handlers = common.NormalizeHandlers(config.Handlers)
}
