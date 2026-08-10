package ws

import (
	"context"
	"sync"
	"time"

	authsvc "Custom-HTS/internal/adapter/broker/ls/api/auth"
	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
	corews "Custom-HTS/internal/core/pkg/websocket"
	"Custom-HTS/internal/core/pkg/wsrefact_v6"
)

const defaultRealtimeStreamBuf = 256
const defaultRealtimeEventBuf = 256
const defaultConnSlotID = "primary"
const defaultMaxConnSlots = 20
const defaultMaxRoutesPerSlot = 200
const defaultRouteFillStep = 50
const defaultIdleSlotTTL = 60 * time.Second

// Dependencies contains the low-level primitives required by the realtime websocket service.
type Dependencies struct {
	// Auth provides bearer tokens for websocket subscription payloads.
	Auth *authsvc.Auth
	// ConnConfig configures every reconnecting websocket transport created by the service.
	ConnConfig corews.WsConfig
	// MaxConnSlots limits how many websocket connections the service may create.
	MaxConnSlots int
	// MaxRoutesPerSlot limits how many realtime routes one connection may own.
	MaxRoutesPerSlot int
	// RouteFillStep controls the round-robin fill target before opening the next slot.
	RouteFillStep int
	// IdleSlotTTL keeps an empty connection slot alive for fast reuse before closing it.
	IdleSlotTTL time.Duration
}

// Realtime is the low-level LS realtime websocket service.
//
// Realtime exposes shared trade, quote, and execution streams while owning
// websocket connection placement internally. It does not create per-caller
// local handles or fan messages out by TR key.
type Realtime struct {
	// auth provides bearer tokens for websocket subscription payloads.
	auth *authsvc.Auth
	// registry owns websocket connection slot placement and route ownership.
	registry *connRegistry
	// tradeCh is the shared low-level stream for LS trade packets.
	tradeCh chan TradeMessage
	// quoteCh is the shared low-level stream for LS quote packets.
	quoteCh chan QuoteMessage
	// executionCh is the shared low-level stream for LS execution packets.
	executionCh chan ExecutionMessage
	// eventCh reports asynchronous control responses and websocket-side failures.
	eventCh chan Event
	// startOnce keeps the service consumers and websocket run loop single-owner.
	startOnce sync.Once
}

// WS keeps the previous websocket service name as a compatibility alias.
type WS = Realtime

// New creates a low-level LS realtime websocket service from explicit dependencies.
func New(deps Dependencies) (*Realtime, error) {
	if deps.Auth == nil {
		return nil, apierr.ErrNilAuth
	}

	// settings stores normalized routing and lifecycle limits for the service.
	settings := normalizeDependencies(deps)
	// registry owns route placement and websocket connection slot lifecycle.
	registry, err := newConnRegistry(settings)
	if err != nil {
		return nil, err
	}

	return &Realtime{
		auth:        deps.Auth,
		registry:    registry,
		tradeCh:     make(chan TradeMessage, defaultRealtimeStreamBuf),
		quoteCh:     make(chan QuoteMessage, defaultRealtimeStreamBuf),
		executionCh: make(chan ExecutionMessage, defaultRealtimeStreamBuf),
		eventCh:     make(chan Event, defaultRealtimeEventBuf),
	}, nil
}

// Start launches the reconnecting websocket run loop once.
func (svc *Realtime) Start(ctx context.Context) {
	svc.startOnce.Do(func() {
		ctx = normalizeContext(ctx)
		svc.registry.start(ctx, connHandlers{
			OnMessage: svc.handleTransportMessage,
			OnStatus:  svc.emitTransportStatusEvent,
		})
	})
}

// Conn returns the first currently managed wsrefact_v6 websocket client.
func (svc *Realtime) Conn() *wsrefact_v6.Client {
	return svc.registry.firstConn()
}

// Trades returns the shared trade stream owned by the low-level websocket service.
func (svc *Realtime) Trades() <-chan TradeMessage {
	return svc.tradeCh
}

// Quotes returns the shared quote stream owned by the low-level websocket service.
func (svc *Realtime) Quotes() <-chan QuoteMessage {
	return svc.quoteCh
}

// Executions returns the shared execution stream owned by the low-level websocket service.
func (svc *Realtime) Executions() <-chan ExecutionMessage {
	return svc.executionCh
}

// Events returns the asynchronous websocket event stream.
func (svc *Realtime) Events() <-chan Event {
	return svc.eventCh
}

func (svc *Realtime) emitEvent(event Event) {
	if svc.eventCh == nil {
		panic("ls ws event channel is nil")
	}
	if event.Layer == "" {
		event.Layer = "ws"
	}
	if event.At.IsZero() {
		event.At = time.Now()
	}
	select {
	case svc.eventCh <- event:
	default:
	}
}

func (svc *Realtime) emitTransportStatusEvent(connID string, status corews.ConnEvent) {
	event := Event{
		ConnID:  connID,
		Attempt: status.Attempt,
		Err:     status.Err,
		At:      status.At,
	}
	switch status.Type {
	case corews.Connected:
		event.Kind = EventWSConnected
	case corews.Reconnected:
		event.Kind = EventWSReconnected
	case corews.Disconnected:
		event.Kind = EventWSDisconnected
	case corews.Reconnecting:
		event.Kind = EventWSReconnecting
	case corews.Fatal:
		event.Kind = EventWSFatal
	default:
		return
	}
	svc.emitEvent(event)
}

func normalizeDependencies(deps Dependencies) Dependencies {
	if deps.MaxConnSlots <= 0 {
		deps.MaxConnSlots = defaultMaxConnSlots
	}
	if deps.MaxRoutesPerSlot <= 0 {
		deps.MaxRoutesPerSlot = defaultMaxRoutesPerSlot
	}
	if deps.RouteFillStep <= 0 {
		deps.RouteFillStep = defaultRouteFillStep
	}
	if deps.RouteFillStep > deps.MaxRoutesPerSlot {
		deps.RouteFillStep = deps.MaxRoutesPerSlot
	}
	if deps.IdleSlotTTL == 0 {
		deps.IdleSlotTTL = defaultIdleSlotTTL
	}
	return deps
}
