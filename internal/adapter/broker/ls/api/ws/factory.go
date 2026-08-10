package ws

import (
	"fmt"
	"net/http"
	"time"

	authsvc "Custom-HTS/internal/adapter/broker/ls/api/auth"
	corews "Custom-HTS/internal/core/pkg/websocket"
	"Custom-HTS/internal/core/pkg/wsrefact_v6"

	"github.com/gorilla/websocket"
)

// connFactory creates wsrefact_v6 websocket clients for connection slots.
type connFactory struct {
	// auth provides bearer tokens for replayed subscribe and unsubscribe writes.
	auth *authsvc.Auth
	// config is the normalized reusable websocket transport configuration.
	config corews.WsConfig
	// nextID is the next numeric connection slot identifier.
	nextID int
}

func newConnFactory(auth *authsvc.Auth, config corews.WsConfig) *connFactory {
	// normalizedConfig stores the slot transport settings after local defaults are applied.
	normalizedConfig := normalizeConnConfig(config)
	return &connFactory{
		auth:   auth,
		config: normalizedConfig,
		nextID: 1,
	}
}

func (factory *connFactory) newSlot() (*connSlot, error) {
	// id is stable for the slot lifetime and appears in messages and events.
	id := formatConnSlotID(factory.nextID)
	factory.nextID++

	// slot is the connection slot shell that owns one wsrefact_v6 runtime.
	slot := newConnSlot(id, factory.auth, factory.config)
	// workerConfig stores the one-worker runtime used by this slot-owned websocket client.
	workerConfig := wsrefact_v6.MuxWorkerConfig{
		WriteEncoder:        slot,
		OnConnected:         slot.handleConnected,
		Dialer:              factory.config.Dialer,
		URL:                 factory.config.URL,
		Header:              buildHandshakeHeader(factory.config.Headers),
		QueueSize:           factory.config.MaxSendQueue,
		DefaultWriteTimeout: factory.config.WriteTimeout,
		DialTimeout:         factory.config.Dialer.HandshakeTimeout,
		ReconnectMin:        factory.config.ReconnectMin,
		ReconnectMax:        factory.config.ReconnectMax,
	}
	// muxConfig stores the single-slot mux runtime that backs this connection slot.
	muxConfig := wsrefact_v6.MuxConfig{
		Workers:   []wsrefact_v6.MuxWorkerConfig{workerConfig},
		QueueSize: factory.config.MaxSendQueue,
	}
	// client is the caller-facing wsrefact_v6 runtime owned by this slot.
	client, err := wsrefact_v6.NewClient(muxConfig)
	if err != nil {
		return nil, err
	}
	slot.client = client
	return slot, nil
}

func buildHandshakeHeader(headers map[string]string) http.Header {
	if len(headers) == 0 {
		return nil
	}

	// handshakeHeader stores the websocket dial headers converted to the gorilla format.
	handshakeHeader := make(http.Header, len(headers))
	for key, value := range headers {
		handshakeHeader.Set(key, value)
	}
	return handshakeHeader
}

func normalizeConnConfig(config corews.WsConfig) corews.WsConfig {
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
	return config
}

func formatConnSlotID(seq int) string {
	if seq <= 1 {
		return defaultConnSlotID
	}
	return fmt.Sprintf("conn-%03d", seq)
}
