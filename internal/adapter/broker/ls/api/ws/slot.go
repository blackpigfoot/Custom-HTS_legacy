package ws

import (
	"context"
	"errors"
	"sync"
	"time"

	authsvc "Custom-HTS/internal/adapter/broker/ls/api/auth"
	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
	corews "Custom-HTS/internal/core/pkg/websocket"
	"Custom-HTS/internal/core/pkg/wsrefact_v6"

	"github.com/gorilla/websocket"
)

// connSlot owns one wsrefact_v6 websocket client and the routes placed on it.
type connSlot struct {
	// id identifies this connection slot in events and future placement policy.
	id string
	// client is the slot-local wsrefact_v6 websocket runtime.
	client *wsrefact_v6.Client
	// auth provides bearer tokens for replayed subscribe and unsubscribe writes.
	auth *authsvc.Auth
	// maxMessageSize limits inbound websocket payload size for this slot.
	maxMessageSize int64
	// pingInterval stores the optional heartbeat cadence for this slot.
	pingInterval time.Duration
	// pongTimeout extends the read deadline after each pong frame.
	pongTimeout time.Duration
	// handlerMu guards handlers updates before and during the slot lifetime.
	handlerMu sync.RWMutex
	// handlers receives slot-scoped message and status forwarding callbacks.
	handlers connHandlers
	// routes stores locally owned route keys assigned to this slot.
	routes map[string]struct{}
	// ctx is the slot-local lifecycle context guarded by connRegistry.mu.
	ctx context.Context
	// cancel stops the slot-local lifecycle context guarded by connRegistry.mu.
	cancel context.CancelFunc
	// idleTimer closes this slot if it stays empty past the idle TTL.
	idleTimer *time.Timer
	// stateMu guards connection-generation and stop state shared by read loops and connect callbacks.
	stateMu sync.Mutex
	// connectCount stores how many live websocket generations this slot has observed.
	connectCount int
	// stopping reports that slot shutdown started and reconnect lifecycle events should be suppressed.
	stopping bool
}

func newConnSlot(id string, auth *authsvc.Auth, config corews.WsConfig) *connSlot {
	return &connSlot{
		id:             id,
		auth:           auth,
		maxMessageSize: config.MaxMessageSize,
		pingInterval:   config.PingInterval,
		pongTimeout:    config.PongTimeout,
		handlers:       connHandlers{},
		routes:         make(map[string]struct{}),
	}
}

func (slot *connSlot) stop() {
	slot.stateMu.Lock()
	slot.stopping = true
	slot.stateMu.Unlock()
	if slot.cancel != nil {
		slot.cancel()
	}
	if slot.client != nil {
		slot.client.Stop()
	}
	slot.ctx = nil
	slot.cancel = nil
	slot.cancelIdleTimer()
}

func (slot *connSlot) run(ctx context.Context) {
	if slot.client == nil {
		panic(ErrNilConnection)
	}

	// stopOnContextDone forwards slot context cancellation into the wsrefact_v6 runtime.
	go func() {
		<-ctx.Done()
		slot.client.Stop()
	}()
	if slot.pingInterval > 0 {
		go slot.runPingLoop(ctx)
	}
	slot.client.Run()
}

func (slot *connSlot) subscribe(ctx context.Context, key string) error {
	_ = ctx
	if slot.client == nil {
		return ErrNilConnection
	}

	// subscription stores the asynchronous command completion reported by the wsrefact_v6 client.
	subscription, err := slot.client.Subscribe(key)
	if err != nil {
		return err
	}
	return slot.observeCommandResult(subscription)
}

func (slot *connSlot) unsubscribe(ctx context.Context, key string) error {
	_ = ctx
	if slot.client == nil {
		return ErrNilConnection
	}

	// subscription stores the asynchronous command completion reported by the wsrefact_v6 client.
	subscription, err := slot.client.Unsubscribe(key)
	if err != nil {
		return err
	}
	return slot.observeCommandResult(subscription)
}

func (slot *connSlot) addRoute(key string) {
	slot.routes[key] = struct{}{}
}

func (slot *connSlot) removeRoute(key string) {
	delete(slot.routes, key)
}

func (slot *connSlot) routeCount() int {
	return len(slot.routes)
}

func (slot *connSlot) cancelIdleTimer() {
	if slot.idleTimer == nil {
		return
	}
	slot.idleTimer.Stop()
	slot.idleTimer = nil
}

func (slot *connSlot) setHandlers(handlers connHandlers) {
	slot.handlerMu.Lock()
	defer slot.handlerMu.Unlock()
	slot.handlers = handlers
}

func (slot *connSlot) observeCommandResult(subscription *wsrefact_v6.Subscription) error {
	if subscription == nil {
		return nil
	}
	select {
	case err, ok := <-subscription.Done():
		if !ok {
			return nil
		}
		return slot.translateCommandError(err)
	default:
		return nil
	}
}

func (slot *connSlot) translateCommandError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, wsrefact_v6.ErrReconnecting):
		return nil
	case errors.Is(err, wsrefact_v6.ErrSubscriptionNotFound):
		return nil
	default:
		return &apierr.OperationError{
			Op:  "ls websocket command",
			Err: err,
		}
	}
}

// EncodeWrite translates one logical slot route key into one outbound LS websocket frame.
func (slot *connSlot) EncodeWrite(intent wsrefact_v6.WriteIntent) (wsrefact_v6.EncodedWrite, error) {
	if intent.Action == wsrefact_v6.WriteIntentActionPing {
		// encoded stores the slot-local heartbeat ping frame returned to the wsrefact_v6 worker.
		encoded := wsrefact_v6.EncodedWrite{
			MessageType: websocket.PingMessage,
		}
		return encoded, nil
	}
	if slot.auth == nil {
		return wsrefact_v6.EncodedWrite{}, apierr.ErrNilAuth
	}

	// route stores the decoded logical routing metadata carried in the mux key.
	route, err := parseTransportKey(intent.Key)
	if err != nil {
		return wsrefact_v6.EncodedWrite{}, err
	}
	// trType stores the LS register or unregister action code required for this logical intent.
	trType, err := resolveTRType(route.scope, intent.Action)
	if err != nil {
		return wsrefact_v6.EncodedWrite{}, err
	}
	// token stores the current LS bearer token used to build this outbound control frame.
	token, err := slot.auth.GetToken(context.Background())
	if err != nil {
		return wsrefact_v6.EncodedWrite{}, err
	}
	// payload stores the fully encoded LS websocket control request body.
	payload, err := buildWSPayload(token, trType, route.trCode, route.trKey)
	if err != nil {
		return wsrefact_v6.EncodedWrite{}, err
	}
	// encoded stores the final websocket text frame returned to the slot worker.
	encoded := wsrefact_v6.EncodedWrite{
		Payload:     payload,
		MessageType: websocket.TextMessage,
	}
	return encoded, nil
}

func (slot *connSlot) handleMessage(data []byte) {
	slot.handlerMu.RLock()
	defer slot.handlerMu.RUnlock()
	if slot.handlers.OnMessage != nil {
		slot.handlers.OnMessage(slot.id, data)
	}
}

func (slot *connSlot) handleStatus(event corews.ConnEvent) {
	slot.handlerMu.RLock()
	defer slot.handlerMu.RUnlock()
	if slot.handlers.OnStatus != nil {
		slot.handlers.OnStatus(slot.id, event)
	}
}

func (slot *connSlot) handleConnected(_ int, conn *websocket.Conn) {
	if conn == nil {
		return
	}
	slot.configureLiveConn(conn)

	slot.stateMu.Lock()
	if slot.stopping {
		slot.stateMu.Unlock()
		return
	}
	slot.connectCount++
	// generation stores the stable live websocket generation identifier observed by this callback.
	generation := slot.connectCount
	slot.stateMu.Unlock()

	// event stores the mapped lifecycle notification forwarded to the existing realtime event pipeline.
	event := corews.ConnEvent{
		Attempt: generation - 1,
		At:      time.Now(),
	}
	if generation == 1 {
		event.Type = corews.Connected
	} else {
		event.Type = corews.Reconnected
	}
	slot.handleStatus(event)
	go slot.readLoop(conn, generation)
}

func (slot *connSlot) configureLiveConn(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	if slot.maxMessageSize > 0 {
		conn.SetReadLimit(slot.maxMessageSize)
	}
	if slot.pingInterval <= 0 {
		return
	}

	// deadline stores the rolling pong deadline applied to the live websocket read loop.
	deadline := slot.pingInterval + slot.pongTimeout
	if deadline <= 0 {
		return
	}
	// initialDeadline stores the first read deadline applied immediately after connect.
	initialDeadline := time.Now().Add(deadline)
	_ = conn.SetReadDeadline(initialDeadline)
	conn.SetPongHandler(func(string) error {
		// nextDeadline stores the next read deadline applied after a pong arrives.
		nextDeadline := time.Now().Add(deadline)
		return conn.SetReadDeadline(nextDeadline)
	})
}

func (slot *connSlot) readLoop(conn *websocket.Conn, generation int) {
	for {
		// messageType stores the websocket opcode returned by the live read.
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			slot.handleReadFailure(generation, err)
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		slot.handleMessage(data)
	}
}

func (slot *connSlot) handleReadFailure(generation int, err error) {
	slot.stateMu.Lock()
	// stopping stores whether slot shutdown already started when this read failure surfaced.
	stopping := slot.stopping
	// currentGeneration stores the most recent live websocket generation owned by the slot.
	currentGeneration := slot.connectCount
	slot.stateMu.Unlock()
	if stopping || generation != currentGeneration {
		return
	}

	// disconnectedEvent reports that the active live websocket read loop ended.
	disconnectedEvent := corews.ConnEvent{
		Type:    corews.Disconnected,
		Err:     err,
		Attempt: generation - 1,
		At:      time.Now(),
	}
	slot.handleStatus(disconnectedEvent)
	// reconnectingEvent reports that the slot will now wait for the next wsrefact_v6 reconnect cycle.
	reconnectingEvent := corews.ConnEvent{
		Type:    corews.Reconnecting,
		Err:     err,
		Attempt: generation,
		At:      time.Now(),
	}
	slot.handleStatus(reconnectingEvent)
}

func (slot *connSlot) runPingLoop(ctx context.Context) {
	// ticker emits the configured slot heartbeat cadence.
	ticker := time.NewTicker(slot.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if slot.client == nil {
				return
			}
			// subscription stores the asynchronous wsrefact_v6 heartbeat completion handle.
			subscription, err := slot.client.Ping()
			if err != nil {
				continue
			}
			_ = slot.observeCommandResult(subscription)
		}
	}
}
