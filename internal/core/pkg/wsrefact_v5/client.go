package wsrefact_v5

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// muxEvent is the closed set of events consumed by the single-thread client loop.
type muxEvent interface {
	isMuxEvent()
}

// sendEvent stores one direct outbound websocket send request.
type sendEvent struct {
	// payload stores the detached outbound websocket frame body.
	payload []byte
	// options stores the per-call websocket write overrides.
	options sendOptions
	// subscription stores the caller-visible completion handle.
	subscription Subscription
}

func (sendEvent) isMuxEvent() {}

// subscribeEvent stores one logical subscribe request.
type subscribeEvent struct {
	// key stores the logical subscription identifier.
	key string
	// options stores the subscribe and unsubscribe payload generators.
	options SubscribeOptions
	// subscription stores the caller-visible completion handle.
	subscription Subscription
}

func (subscribeEvent) isMuxEvent() {}

// unsubscribeEvent stores one logical unsubscribe request.
type unsubscribeEvent struct {
	// key stores the logical subscription identifier.
	key string
	// subscription stores the caller-visible completion handle.
	subscription Subscription
}

func (unsubscribeEvent) isMuxEvent() {}

// snapshotEvent stores one synchronous desired-state key snapshot request.
type snapshotEvent struct {
	// responseCh stores the synchronous response sink for the detached key snapshot.
	responseCh chan []string
}

func (snapshotEvent) isMuxEvent() {}

// dialResultEvent stores one completed dial attempt.
type dialResultEvent struct {
	// attempt stores the logical reconnect attempt count for this dial.
	attempt int
	// conn stores the newly established websocket connection when dialing succeeded.
	conn *websocket.Conn
	// err stores the dial failure when dialing failed.
	err error
}

func (dialResultEvent) isMuxEvent() {}

// reconnectTickEvent stores one expired backoff timer notification.
type reconnectTickEvent struct {
	// attempt stores the reconnect attempt count that should be dialed next.
	attempt int
}

func (reconnectTickEvent) isMuxEvent() {}

func (sessionReadEvent) isMuxEvent()   {}
func (workerFailureEvent) isMuxEvent() {}

// Client owns the public websocket transport API and the single-thread multiplexer loop.
type Client struct {
	// config stores immutable transport runtime settings.
	config Config
	// eventCh stores public requests plus internal worker/session events.
	eventCh chan muxEvent
	// runCtx scopes the full client lifetime for background helpers.
	runCtx context.Context
	// runCancel stops the client lifetime context exactly once.
	runCancel context.CancelFunc
	// done closes when Run fully exits.
	done chan struct{}
	// state stores the public lifecycle state for lock-free readers.
	state atomic.Uint32
	// started guards against duplicate Run invocations.
	started atomic.Bool
	// stopOnce guarantees one public stop signal.
	stopOnce sync.Once
}

// clientLoop stores the mutable single-thread transport state.
type clientLoop struct {
	// client stores the outer public transport wrapper.
	client *Client
	// store stores desired replayable subscription intent in caller order.
	store *intentStore
	// liveTransport stores the current active connection generation, even when writes are blocked.
	liveTransport *transport
	// retiredTransports stores previously detached connection generations awaiting shutdown join.
	retiredTransports []*transport
	// routingOpen reports whether new writes may still be handed to the live worker.
	routingOpen bool
	// dialing reports whether one background dial attempt is currently in flight.
	dialing bool
	// waitingReconnect reports whether one reconnect timer is currently armed.
	waitingReconnect bool
	// readyCount stores how many live sessions have completed activation successfully.
	readyCount int
}

// workerReplayer exposes one synchronous replayer over the current live ioWorker.
type workerReplayer struct {
	// transport stores the current live connection generation used for replay and OnReady writes.
	transport *transport
}

// New creates one reconnecting websocket Client.
func New(config Config) (*Client, error) {
	if err := config.applyDefaults(); err != nil {
		return nil, err
	}

	// runCtx scopes the full client lifetime for background helpers.
	runCtx, runCancel := context.WithCancel(context.Background())
	// client owns the public API and the single-thread transport loop.
	client := &Client{
		config:    config,
		eventCh:   make(chan muxEvent, config.EventQueueSize),
		runCtx:    runCtx,
		runCancel: runCancel,
		done:      make(chan struct{}),
	}
	client.state.Store(uint32(ConnStateStopped))
	return client, nil
}

// Run owns the reconnect lifecycle until Stop is called or a fatal handler-stop error occurs.
func (client *Client) Run() error {
	if client == nil {
		return ErrClientStopped
	}
	if !client.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	defer close(client.done)
	client.setState(ConnStateReconnecting)

	// loop stores the mutable single-thread runtime state for this Run call.
	loop := clientLoop{
		client: client,
		store:  newIntentStore(),
	}
	loop.startDial(0)

	for {
		select {
		case <-client.runCtx.Done():
			return loop.shutdown(nil)
		case event := <-client.eventCh:
			// stepErr stores one fatal loop-processing error.
			stepErr := loop.handleEvent(event)
			if stepErr == nil {
				continue
			}
			return loop.shutdown(stepErr)
		}
	}
}

// Stop requests client shutdown. It is safe to call from any goroutine.
func (client *Client) Stop() {
	if client == nil {
		return
	}

	client.stopOnce.Do(func() {
		client.runCancel()
	})
}

// State returns the current public websocket lifecycle state.
func (client *Client) State() ConnState {
	if client == nil {
		return ConnStateStopped
	}
	return ConnState(client.state.Load())
}

// IsConnected reports whether the client currently exposes a writable live session.
func (client *Client) IsConnected() bool {
	return client.State() == ConnStateConnected
}

// Send queues one direct websocket write and returns its asynchronous completion handle.
func (client *Client) Send(payload []byte, options ...SendOption) Subscription {
	if client == nil {
		return newResolvedSubscription(ErrClientStopped)
	}

	// writeOptions stores the normalized per-call websocket write overrides.
	writeOptions := sendOptions{
		messageType: websocket.TextMessage,
	}
	for _, option := range options {
		option(&writeOptions)
	}

	// detachedPayload stores the caller payload detached from future caller mutation.
	detachedPayload := append([]byte(nil), payload...)
	// subscription stores the caller-visible completion handle.
	subscription := newSubscription()
	// event stores the queued logical send request for the single-thread loop.
	event := sendEvent{
		payload:      detachedPayload,
		options:      writeOptions,
		subscription: subscription,
	}
	if err := client.publishPublic(event); err != nil {
		subscription.resolve(err)
	}
	return subscription
}

// Subscribe records one replayable subscription intent and attempts an immediate live write when possible.
func (client *Client) Subscribe(key string, options SubscribeOptions) Subscription {
	if client == nil {
		return newResolvedSubscription(ErrClientStopped)
	}

	// subscription stores the caller-visible completion handle.
	subscription := newSubscription()
	// event stores the queued logical subscribe request for the single-thread loop.
	event := subscribeEvent{
		key:          key,
		options:      options,
		subscription: subscription,
	}
	if err := client.publishPublic(event); err != nil {
		subscription.resolve(err)
	}
	return subscription
}

// Unsubscribe removes one replayable subscription intent and attempts an immediate live write when possible.
func (client *Client) Unsubscribe(key string) Subscription {
	if client == nil {
		return newResolvedSubscription(ErrClientStopped)
	}

	// subscription stores the caller-visible completion handle.
	subscription := newSubscription()
	// event stores the queued logical unsubscribe request for the single-thread loop.
	event := unsubscribeEvent{
		key:          key,
		subscription: subscription,
	}
	if err := client.publishPublic(event); err != nil {
		subscription.resolve(err)
	}
	return subscription
}

// Subscriptions returns the current desired-state subscription keys in replay order.
func (client *Client) Subscriptions() []string {
	if client == nil || !client.started.Load() || client.runCtx.Err() != nil {
		return nil
	}

	// responseCh stores the synchronous response sink for this snapshot request.
	responseCh := make(chan []string, 1)
	// event stores the queued synchronous snapshot request.
	event := snapshotEvent{responseCh: responseCh}
	if err := client.publishPublic(event); err != nil {
		return nil
	}

	select {
	case keys := <-responseCh:
		return keys
	case <-client.runCtx.Done():
		return nil
	}
}

// Send writes one frame synchronously through the current live ioWorker.
func (replayer workerReplayer) Send(payload []byte, options ...SendOption) error {
	if replayer.transport == nil || replayer.transport.worker == nil {
		return ErrNotConnected
	}

	// writeOptions stores the normalized per-call websocket write overrides.
	writeOptions := sendOptions{
		messageType: websocket.TextMessage,
	}
	for _, option := range options {
		option(&writeOptions)
	}

	return replayer.transport.worker.SendSync(payload, resolveMessageType(writeOptions.messageType), writeOptions.writeTimeout)
}

func (client *Client) publishPublic(event muxEvent) error {
	if !client.started.Load() {
		return ErrClientStopped
	}
	if client.runCtx.Err() != nil {
		return ErrClientStopped
	}

	select {
	case client.eventCh <- event:
		return nil
	default:
		return ErrQueueFull
	}
}

func (client *Client) publishInternal(event muxEvent) {
	if client == nil {
		return
	}

	select {
	case client.eventCh <- event:
	case <-client.runCtx.Done():
	}
}

func (client *Client) setState(state ConnState) {
	client.state.Store(uint32(state))
}

func (client *Client) emit(event ConnEvent) {
	event.At = time.Now()
	client.config.Hooks.OnEvent(event)
}

func (client *Client) dial() (*websocket.Conn, error) {
	// dialCtx bounds one websocket dial attempt beneath the client lifetime.
	dialCtx, cancel := context.WithTimeout(client.runCtx, client.config.DialTimeout)
	defer cancel()

	// header stores the detached HTTP handshake headers for this dial.
	header := cloneHeader(client.config.Header)
	// rawConn stores the newly established websocket connection when dialing succeeds.
	rawConn, _, err := client.config.Dialer.DialContext(dialCtx, client.config.URL, header)
	if err != nil {
		if errors.Is(dialCtx.Err(), context.Canceled) || errors.Is(dialCtx.Err(), context.DeadlineExceeded) {
			return nil, dialCtx.Err()
		}
		return nil, &OperationError{
			Op:  "dial connection",
			Err: err,
		}
	}

	if client.config.ReadLimit > 0 {
		rawConn.SetReadLimit(client.config.ReadLimit)
	}
	if client.config.PingInterval > 0 && client.config.PongTimeout > 0 {
		// pongWait stores the effective read deadline extension applied after each pong.
		pongWait := client.config.PingInterval + client.config.PongTimeout
		_ = rawConn.SetReadDeadline(time.Now().Add(pongWait))
		rawConn.SetPongHandler(func(string) error {
			return rawConn.SetReadDeadline(time.Now().Add(pongWait))
		})
	}
	return rawConn, nil
}

func (loop *clientLoop) handleEvent(event muxEvent) error {
	switch typedEvent := event.(type) {
	case sendEvent:
		loop.handleSend(typedEvent)
		return nil
	case subscribeEvent:
		loop.handleSubscribe(typedEvent)
		return nil
	case unsubscribeEvent:
		loop.handleUnsubscribe(typedEvent)
		return nil
	case snapshotEvent:
		typedEvent.responseCh <- loop.store.keys()
		close(typedEvent.responseCh)
		return nil
	case dialResultEvent:
		loop.handleDialResult(typedEvent)
		return nil
	case reconnectTickEvent:
		loop.waitingReconnect = false
		loop.startDial(typedEvent.attempt)
		return nil
	case sessionReadEvent:
		return loop.handleSessionRead(typedEvent)
	case workerFailureEvent:
		loop.handleWorkerFailure(typedEvent)
		return nil
	default:
		return nil
	}
}

func (loop *clientLoop) handleSend(event sendEvent) {
	if !loop.canRouteWrites() {
		loop.client.emit(ConnEvent{
			Type: SendDuringBackoff,
		})
		event.subscription.resolve(ErrBackoff)
		return
	}

	// enqueued reports whether the live worker accepted this direct send.
	enqueued := loop.liveTransport.worker.TryEnqueue(
		event.payload,
		resolveMessageType(event.options.messageType),
		event.options.writeTimeout,
		&event.subscription,
	)
	if !enqueued {
		event.subscription.resolve(ErrQueueFull)
	}
}

func (loop *clientLoop) handleSubscribe(event subscribeEvent) {
	if event.options.SubGen == nil || event.options.UnsubGen == nil {
		event.subscription.resolve(errInvalidOptions)
		return
	}

	if err := loop.store.put(event.key, event.options); err != nil {
		event.subscription.resolve(err)
		return
	}

	if !loop.canRouteWrites() {
		loop.client.emit(ConnEvent{
			Type: SendDuringBackoff,
			Key:  event.key,
		})
		event.subscription.resolve(ErrBackoff)
		return
	}

	// payload stores the live subscribe frame generated for the current session.
	payload, err := event.options.SubGen()
	if err != nil {
		event.subscription.resolve(err)
		return
	}

	// enqueued reports whether the live worker accepted this subscribe write.
	enqueued := loop.liveTransport.worker.TryEnqueue(
		payload,
		resolveMessageType(event.options.MessageType),
		event.options.WriteTimeout,
		&event.subscription,
	)
	if !enqueued {
		event.subscription.resolve(ErrQueueFull)
	}
}

func (loop *clientLoop) handleUnsubscribe(event unsubscribeEvent) {
	// intent stores the removed logical subscription metadata.
	intent, err := loop.store.remove(event.key)
	if err != nil {
		event.subscription.resolve(err)
		return
	}

	if !loop.canRouteWrites() {
		loop.client.emit(ConnEvent{
			Type: SendDuringBackoff,
			Key:  event.key,
		})
		event.subscription.resolve(ErrBackoff)
		return
	}

	// payload stores the live unsubscribe frame generated for the current session.
	payload, genErr := intent.options.UnsubGen()
	if genErr != nil {
		event.subscription.resolve(genErr)
		return
	}

	// enqueued reports whether the live worker accepted this unsubscribe write.
	enqueued := loop.liveTransport.worker.TryEnqueue(
		payload,
		resolveMessageType(intent.options.MessageType),
		intent.options.WriteTimeout,
		&event.subscription,
	)
	if !enqueued {
		event.subscription.resolve(ErrQueueFull)
	}
}

func (loop *clientLoop) handleDialResult(event dialResultEvent) {
	loop.dialing = false
	if loop.client.runCtx.Err() != nil {
		if event.conn != nil {
			_ = event.conn.Close()
		}
		return
	}

	if event.err != nil {
		loop.scheduleReconnect(nextReconnectAttempt(event.attempt))
		return
	}

	// liveTransport stores the freshly established connection generation runtime.
	liveTransport, err := newTransport(event.conn, transportConfig{
		handlers:       loop.client.config.Handlers,
		writeTimeout:   loop.client.config.WriteTimeout,
		pingInterval:   loop.client.config.PingInterval,
		writeQueueSize: loop.client.config.WriteQueueSize,
	}, loop.client.eventCh, loop.client.runCtx.Done())
	if err != nil {
		loop.scheduleReconnect(nextReconnectAttempt(event.attempt))
		return
	}

	loop.liveTransport = liveTransport
	loop.routingOpen = false

	if err := loop.activate(event.attempt); err != nil {
		loop.client.emit(ConnEvent{
			Type: Disconnected,
			Err:  err,
		})
		loop.releaseLiveTransport()
		loop.client.setState(ConnStateReconnecting)
		loop.scheduleReconnect(nextReconnectAttempt(event.attempt))
		return
	}

	loop.routingOpen = true
	loop.client.setState(ConnStateConnected)
	if loop.readyCount == 0 {
		loop.client.emit(ConnEvent{
			Type: Connected,
		})
	} else {
		loop.client.emit(ConnEvent{
			Type: Reconnected,
		})
	}
	loop.readyCount++
}

func (loop *clientLoop) handleSessionRead(event sessionReadEvent) error {
	if loop.liveTransport == nil || event.session != loop.liveTransport.session {
		return nil
	}

	if event.handlerErr != nil {
		loop.client.emit(ConnEvent{
			Type: HandlerError,
			Err:  event.handlerErr,
		})

		switch {
		case errors.Is(event.handlerErr, ErrHandlerContinue):
			return nil
		case errors.Is(event.handlerErr, ErrHandlerBackoff):
			loop.enterBackoff(event.handlerErr, 1)
			return nil
		case errors.Is(event.handlerErr, ErrHandlerStop):
			return event.handlerErr
		default:
			return event.handlerErr
		}
	}

	if event.readErr != nil {
		loop.enterBackoff(event.readErr, 1)
	}
	return nil
}

func (loop *clientLoop) handleWorkerFailure(event workerFailureEvent) {
	if loop.liveTransport == nil || event.worker != loop.liveTransport.worker {
		return
	}
	if !loop.routingOpen {
		return
	}

	loop.routingOpen = false
	loop.client.emit(ConnEvent{
		Type: TransportWriteFailed,
		Err:  event.err,
	})
}

func (loop *clientLoop) activate(attempt int) error {
	// replayer stores the synchronous live-session writer exposed during activation.
	replayer := workerReplayer{transport: loop.liveTransport}
	// snapshot stores the replayable subscription sequence for this activation.
	snapshot := loop.store.snapshot()
	for _, intent := range snapshot {
		// payload stores the live subscribe frame generated for this activation.
		payload, err := intent.options.SubGen()
		if err != nil {
			return err
		}
		if err := replayer.Send(
			payload,
			WithMessageType(resolveMessageType(intent.options.MessageType)),
			WithWriteTimeout(intent.options.WriteTimeout),
		); err != nil {
			return err
		}
	}

	if loop.client.config.Hooks.OnReady != nil {
		if err := loop.client.config.Hooks.OnReady(replayer, attempt); err != nil {
			return err
		}
	}
	return nil
}

func (loop *clientLoop) enterBackoff(err error, attempt int) {
	loop.client.emit(ConnEvent{
		Type: Disconnected,
		Err:  err,
	})
	loop.releaseLiveTransport()
	loop.client.setState(ConnStateReconnecting)
	loop.scheduleReconnect(attempt)
}

func (loop *clientLoop) releaseLiveTransport() {
	// liveTransport stores the detached current connection generation for teardown.
	liveTransport := loop.liveTransport

	loop.routingOpen = false
	loop.liveTransport = nil

	if liveTransport != nil {
		liveTransport.Dispose()
		loop.retiredTransports = append(loop.retiredTransports, liveTransport)
	}
}

func (loop *clientLoop) scheduleReconnect(attempt int) {
	if loop.waitingReconnect || loop.client.runCtx.Err() != nil {
		return
	}

	loop.waitingReconnect = true
	loop.client.emit(ConnEvent{
		Type:    Reconnecting,
		Attempt: attempt,
	})

	// delay stores the effective reconnect backoff for this attempt.
	delay := calcBackoff(loop.client.config.ReconnectMin, loop.client.config.ReconnectMax, attempt)
	go func() {
		// timer waits for the next reconnect attempt.
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
			loop.client.publishInternal(reconnectTickEvent{attempt: attempt})
		case <-loop.client.runCtx.Done():
		}
	}()
}

func (loop *clientLoop) startDial(attempt int) {
	if loop.dialing || loop.client.runCtx.Err() != nil {
		return
	}

	loop.dialing = true
	go func() {
		// conn stores the newly established websocket connection when dialing succeeds.
		conn, err := loop.client.dial()
		loop.client.publishInternal(dialResultEvent{
			attempt: attempt,
			conn:    conn,
			err:     err,
		})
	}()
}

func (loop *clientLoop) canRouteWrites() bool {
	return loop.client.State() == ConnStateConnected && loop.routingOpen && loop.liveTransport != nil
}

func (loop *clientLoop) shutdown(runErr error) error {
	loop.client.runCancel()

	// liveTransport stores the detached current connection generation for final shutdown.
	liveTransport := loop.liveTransport

	loop.routingOpen = false
	loop.liveTransport = nil

	if liveTransport != nil {
		liveTransport.Dispose()
	}
	for _, retiredTransport := range loop.retiredTransports {
		if retiredTransport == nil {
			continue
		}
		retiredTransport.Wait()
	}
	if liveTransport != nil {
		liveTransport.Wait()
	}

	loop.client.setState(ConnStateStopped)
	loop.client.emit(ConnEvent{
		Type: Stopped,
		Err:  runErr,
	})
	return runErr
}

func resolveMessageType(messageType int) int {
	if messageType > 0 {
		return messageType
	}
	return websocket.TextMessage
}

func nextReconnectAttempt(attempt int) int {
	if attempt <= 0 {
		return 1
	}
	return attempt + 1
}

func calcBackoff(minimum time.Duration, maximum time.Duration, attempt int) time.Duration {
	if attempt <= 1 {
		return minimum
	}

	// backoff stores the exponential reconnect delay before capping.
	backoff := minimum
	for currentAttempt := 1; currentAttempt < attempt; currentAttempt++ {
		if backoff >= maximum {
			return maximum
		}
		backoff *= 2
		if backoff > maximum {
			return maximum
		}
	}
	return backoff
}

func cloneHeader(header http.Header) http.Header {
	if header == nil {
		return nil
	}

	// cloned stores the detached HTTP header copy for this dial attempt.
	cloned := make(http.Header, len(header))
	for key, values := range header {
		// detachedValues stores the detached header slice for one header key.
		detachedValues := append([]string(nil), values...)
		cloned[key] = detachedValues
	}
	return cloned
}
