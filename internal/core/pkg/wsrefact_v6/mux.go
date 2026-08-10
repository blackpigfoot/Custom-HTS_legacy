package wsrefact_v6

import (
	"context"
	stdErrors "errors"
	"sync"
	"time"
)

// Mux owns desired subscription state, worker placement, reconnect replay, and caller command serialization.
//
// Lifecycle gating against new submissions is owned by the wrapping Client. The mux only
// exposes a one-shot Stop and a fatal notification channel; on a fatal worker event the
// mux transitions into stopping state, publishes the cause through fatalCh, and waits for
// the Client to close commandCh through the normal Stop path.
type Mux struct {
	// ioWorkerCh stores internal worker lifecycle events published back into the mux loop.
	ioWorkerCh chan ioWorkerEvent
	// commandCh stores caller-originated logical commands.
	commandCh chan userCommand
	// workerExitCh stores one worker-loop exit notification per owned worker.
	workerExitCh chan int
	// fatalCh stores the one-shot fatal notification published before mux self-shutdown.
	// Buffered with capacity one so the publish path never blocks the mux loop.
	fatalCh chan FatalEvent
	// workers stores all owned worker runtimes.
	workers []muxWorkerRuntime
	// subscriptions stores the desired logical subscription set keyed by logical key.
	subscriptions map[string]muxSubscription
	// placementPolicy stores the worker-selection strategy for new subscriptions.
	placementPolicy PlacementPolicy
	// stopOnce guarantees that Stop closes commandCh exactly once.
	stopOnce sync.Once
	// pingTickerWG joins every heartbeat ticker goroutine so handleShutdown can guarantee
	// that no ping ticker survives past close(ioWorkerCh).
	pingTickerWG sync.WaitGroup
	// shutdownErr stores the terminal error used to resolve commands observed after fatal stop begins.
	shutdownErr error
	// state stores the mux lifecycle owned by the serialized mux loop.
	state muxRunState
}

// NewMux allocates one mux that manages multiple reconnecting websocket io workers.
func NewMux(config MuxConfig) (*Mux, error) {
	if len(config.Workers) == 0 {
		return nil, ErrWorkersRequired
	}
	if config.QueueSize <= 0 {
		return nil, ErrInvalidQueueSize
	}

	// ioWorkerCh stores the shared internal worker event sink for this mux.
	ioWorkerCh := make(chan ioWorkerEvent, config.QueueSize)
	// runtimes stores the fully constructed worker runtimes owned by this mux.
	runtimes := make([]muxWorkerRuntime, 0, len(config.Workers))
	for workerIndex, workerConfig := range config.Workers {
		if workerConfig.QueueSize <= 0 {
			return nil, ErrInvalidQueueSize
		}
		if workerConfig.DefaultWriteTimeout <= 0 {
			return nil, ErrInvalidWriteTimeout
		}

		// commandCh stores the serialized worker mailbox for this worker slot.
		commandCh := make(chan ioCommand, workerConfig.QueueSize)
		// worker stores the fully configured websocket io worker for this slot.
		worker, err := newIOWorker(ioWorkerConfig{
			WorkerIndex:         workerIndex,
			CommandCh:           commandCh,
			EventSink:           ioWorkerCh,
			WriteEncoder:        workerConfig.WriteEncoder,
			OnConnected:         workerConfig.OnConnected,
			OnMessage:           workerConfig.OnMessage,
			Dialer:              workerConfig.Dialer,
			URL:                 workerConfig.URL,
			Header:              workerConfig.Header,
			DefaultWriteTimeout: workerConfig.DefaultWriteTimeout,
			DialTimeout:         workerConfig.DialTimeout,
			ReconnectMin:        workerConfig.ReconnectMin,
			ReconnectMax:        workerConfig.ReconnectMax,
			PingInterval:        workerConfig.PingInterval,
		})
		if err != nil {
			return nil, err
		}

		runtimes = append(runtimes, muxWorkerRuntime{
			CommandCh:    commandCh,
			Worker:       worker,
			State:        MuxWorkerStateConnecting,
			DesiredKeys:  make([]string, 0),
			PingInterval: workerConfig.PingInterval,
		})
	}

	// placementPolicy stores the effective worker placement strategy for this mux.
	placementPolicy := config.PlacementPolicy
	if placementPolicy == nil {
		placementPolicy = defaultPlacementPolicy
	}

	// mux stores the fully constructed reconnecting websocket multiplexer.
	mux := &Mux{
		ioWorkerCh:      ioWorkerCh,
		commandCh:       make(chan userCommand, config.QueueSize),
		workerExitCh:    make(chan int, len(config.Workers)),
		fatalCh:         make(chan FatalEvent, 1),
		workers:         runtimes,
		subscriptions:   make(map[string]muxSubscription),
		placementPolicy: placementPolicy,
		shutdownErr:     ErrMuxStopped,
	}
	mux.state = muxRunStateReady
	return mux, nil
}

// FatalCh returns the read-only fatal notification channel.
//
// The mux publishes at most one FatalEvent before shutdown and then closes the channel.
// On normal Stop the channel is closed without any value being sent.
func (m *Mux) FatalCh() <-chan FatalEvent {
	return m.fatalCh
}

// Run starts every owned worker, drives reconnect, and serializes all mux commands until shutdown.
func (m *Mux) Run() {
	if m.state != muxRunStateReady {
		return
	}
	m.state = muxRunStateRunning

	for workerIndex := range m.workers {
		go m.runWorkerLoop(workerIndex)
		m.startConnect(workerIndex)
	}

	for {
		select {
		case command, ok := <-m.commandCh:
			if !ok {
				m.handleShutdown()
				return
			}
			if m.state != muxRunStateRunning {
				command.Subscription.resolve(m.shutdownResolveError())
				continue
			}
			m.handleCommand(command)
		case event := <-m.ioWorkerCh:
			if fatalErr := m.handleIOWorkerEvent(event); fatalErr != nil {
				m.beginFatalShutdown(fatalErr)
			}
		}
	}
}

// stop requests one external mux shutdown. Idempotent.
func (m *Mux) stop() {
	m.stopOnce.Do(func() {
		close(m.commandCh)
	})
}

// submitCommand enqueues one caller-visible command into the mux mailbox.
//
// Lifecycle gating is owned by the wrapping Client; this method only verifies that the
// backing command channel is not saturated.
func (m *Mux) submitCommand(kind userCommandKind, key string) (*Subscription, error) {
	subscription := newSubscription()
	command := userCommand{
		Kind:         kind,
		Key:          key,
		Subscription: subscription,
	}

	select {
	case m.commandCh <- command:
		return subscription, nil
	default:
		return nil, ErrQueueFull
	}
}

// handleCommand executes one caller-originated mux command.
func (m *Mux) handleCommand(command userCommand) {
	switch command.Kind {
	case userCommandKindSubscribe:
		m.handleSubscribe(command)
	case userCommandKindUnsubscribe:
		m.handleUnsubscribe(command)
	default:
		panic("ws: unrecognized mux command")
	}
}

// handleIOWorkerEvent applies one internal worker lifecycle event and returns a non-nil
// error when the event represents a deterministic fatal failure that should trigger mux
// stop transition.
func (m *Mux) handleIOWorkerEvent(event ioWorkerEvent) error {
	switch event.Kind {
	case ioWorkerEventKindConnected, ioWorkerEventKindReconnected:
		m.handleWorkerReady(event.WorkerIndex)
	case ioWorkerEventKindConnectFailed:
		m.startReconnect(event.WorkerIndex)
	case ioWorkerEventKindDisconnected:
		m.startReconnect(event.WorkerIndex)
	case ioWorkerEventKindPingTick:
		m.handlePingTick(event)
	case ioWorkerEventKindWriteFailed:
		// Only encode failures reach here; transport errors are handled by the read loop.
		// Encode failures are deterministic bugs that repeat on every retry, so surface
		// them as fatal so the client watcher can transition into the normal Stop path.
		var encodeErr *EncodeWriteError
		if stdErrors.As(event.Err, &encodeErr) {
			return event.Err
		}
	}
	return nil
}

// handleSubscribe records one desired subscribe key and emits one live subscribe only when the key was newly added.
func (m *Mux) handleSubscribe(command userCommand) {
	if _, exists := m.subscriptions[command.Key]; exists {
		command.Subscription.resolve(ErrSubscriptionExists)
		return
	}

	workerIndex, placementErr := m.pickWorker(command.Key)
	if placementErr != nil {
		command.Subscription.resolve(placementErr)
		return
	}

	intent := WriteIntent{
		Key:    command.Key,
		Action: WriteIntentActionSubscribe,
	}
	m.subscriptions[command.Key] = muxSubscription{
		WorkerIndex: workerIndex,
		Intent:      intent,
	}

	keys := m.workers[workerIndex].DesiredKeys
	keys = append(keys, command.Key)

	worker := &m.workers[workerIndex]
	if worker.State != MuxWorkerStateConnected {
		// Desired state is recorded; the next reconnect replay will issue the actual
		// subscribe write. Resolving with nil signals registration success.
		command.Subscription.resolve(nil)
		return
	}

	if dispatchErr := m.dispatchLiveWrite(workerIndex, intent, command.Subscription); dispatchErr != nil {
		delete(m.subscriptions, command.Key)
		m.removeDesiredKey(workerIndex, command.Key)
		command.Subscription.resolve(dispatchErr)
	}
}

// handleUnsubscribe removes one desired subscribe key and emits one live unsubscribe only when the key existed.
func (m *Mux) handleUnsubscribe(command userCommand) {
	entry, exists := m.subscriptions[command.Key]
	if !exists {
		command.Subscription.resolve(ErrSubscriptionNotFound)
		return
	}

	delete(m.subscriptions, command.Key)
	removedIndex := m.removeDesiredKey(entry.WorkerIndex, command.Key)

	worker := &m.workers[entry.WorkerIndex]
	if worker.State != MuxWorkerStateConnected {
		command.Subscription.resolve(nil)
		return
	}

	// intent stores the live unsubscribe instruction derived from the retained desired subscribe intent.
	intent := entry.Intent
	intent.Action = WriteIntentActionUnsubscribe
	if dispatchErr := m.dispatchLiveWrite(entry.WorkerIndex, intent, command.Subscription); dispatchErr != nil {
		m.subscriptions[command.Key] = entry
		m.insertDesiredKey(entry.WorkerIndex, command.Key, removedIndex)
		command.Subscription.resolve(dispatchErr)
	}
}

// handlePingTick decides whether one ping tick should be forwarded to the io worker.
//
// State and epoch checks happen inside the mux run loop so the decision is fully
// serialized with reconnect transitions. A tick is forwarded only when the worker is
// currently Connected and the tick was generated by the current heartbeat generation.
// Both conditions together guarantee that pings produced by a previous connection are
// always dropped, even if they raced past the ticker context cancellation.
func (m *Mux) handlePingTick(event ioWorkerEvent) {
	worker := &m.workers[event.WorkerIndex]
	if worker.State != MuxWorkerStateConnected {
		return
	}
	if event.Epoch != worker.HeartbeatEpoch {
		return
	}

	// intent stores the heartbeat ping instruction for the worker encoder.
	intent := WriteIntent{
		Action: WriteIntentActionPing,
	}
	command := ioCommand{
		Type:                ioCommandTypePing,
		Intent:              &intent,
		PublishWriteFailure: true,
	}
	select {
	case worker.CommandCh <- command:
	default:
		// Worker mailbox is saturated; one heartbeat tick is dropped silently.
	}
}

// handleWorkerReady restores desired subscriptions and starts the heartbeat ticker
// after one successful initial connect or reconnect. The replay must finish before the
// ticker starts so the heartbeat generation only advances once the worker is fully
// ready to accept new writes.
func (m *Mux) handleWorkerReady(workerIndex int) {
	m.clearDialCancel(workerIndex)
	m.runReplay(workerIndex)
	m.startPingTicker(workerIndex)
}

// startPingTicker bumps the heartbeat generation and launches one ticker goroutine
// for the worker. Called from the mux run loop so HeartbeatEpoch and PingCancel are
// only mutated under run-loop ownership.
func (m *Mux) startPingTicker(workerIndex int) {
	worker := &m.workers[workerIndex]
	if worker.PingInterval <= 0 {
		return
	}
	if worker.State != MuxWorkerStateConnected {
		return
	}

	worker.HeartbeatEpoch++
	// epoch stamps every tick this generation produces so the run loop can drop ticks
	// from earlier generations even if they race past the ticker context cancellation.
	epoch := worker.HeartbeatEpoch

	tickerCtx, tickerCancel := context.WithCancel(context.Background())
	worker.PingCancel = tickerCancel
	m.pingTickerWG.Add(1)
	go m.runPingTicker(workerIndex, epoch, tickerCtx, worker.PingInterval)
}

// stopPingTicker cancels the worker's heartbeat ticker goroutine and forgets the handle.
// Called whenever the worker leaves Connected.
func (m *Mux) stopPingTicker(workerIndex int) {
	worker := &m.workers[workerIndex]
	if worker.PingCancel == nil {
		return
	}
	worker.PingCancel()
	worker.PingCancel = nil
}

// runPingTicker fires one ioWorkerEventKindPingTick into the mux event sink on every
// heartbeat interval. It owns no state of its own beyond the captured epoch and only
// stops when the heartbeat context is cancelled, so the routing decision stays inside
// the mux run loop.
func (m *Mux) runPingTicker(workerIndex int, epoch uint64, tickerCtx context.Context, interval time.Duration) {
	defer m.pingTickerWG.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-tickerCtx.Done():
			return
		case <-ticker.C:
			event := ioWorkerEvent{
				Kind:        ioWorkerEventKindPingTick,
				WorkerIndex: workerIndex,
				Epoch:       epoch,
			}
			select {
			case m.ioWorkerCh <- event:
			case <-tickerCtx.Done():
				return
			}
		}
	}
}

// runReplay enqueues every desired subscription back into the worker mailbox through the
// dedicated replay-write path and immediately returns control to the mux loop.
//
// Replay is fire-and-forget so the mux can keep accepting user commands while the worker
// drains its mailbox. Replay results are reported asynchronously through the worker event
// sink: deterministic encode failures arrive as ioWorkerEventKindWriteFailed wrapping
// *EncodeWriteError and trigger fatal self-shutdown, while transport failures trigger a
// normal reconnect. Replay therefore uses its own worker command kind instead of reusing
// the live caller-visible write path.
//
// The send into worker.CommandCh is intentionally blocking. Dropping replay writes would
// create split-brain between desired state and the broker, so the mux briefly pauses while
// the worker drains rather than losing any logical subscription. QueueSize on each worker
// must therefore be sized to comfortably hold the maximum desired subscription count.
func (m *Mux) runReplay(workerIndex int) {
	// worker stores the runtime currently replaying its desired subscription set.
	worker := &m.workers[workerIndex]
	if worker.State == MuxWorkerStateStopped {
		return
	}
	if len(worker.DesiredKeys) == 0 {
		worker.State = MuxWorkerStateConnected
		return
	}

	for _, key := range worker.DesiredKeys {
		// entry stores the latest desired subscription state for this key.
		entry, exists := m.subscriptions[key]
		if !exists {
			continue
		}

		// commandIntent stores the immutable replay instruction forwarded into the worker loop.
		commandIntent := entry.Intent
		worker.CommandCh <- ioCommand{
			Type:                ioCommandTypeReplayWrite,
			Intent:              &commandIntent,
			PublishWriteFailure: true,
		}
	}

	worker.State = MuxWorkerStateConnected
}

// dispatchLiveWrite enqueues one live worker write that resolves the caller directly inside the worker loop.
func (m *Mux) dispatchLiveWrite(workerIndex int, intent WriteIntent, callerSubscription *Subscription) error {
	command := ioCommand{
		Type:                ioCommandTypeWrite,
		Intent:              &intent,
		Subscription:        callerSubscription,
		PublishWriteFailure: true,
	}

	select {
	case m.workers[workerIndex].CommandCh <- command:
		return nil
	default:
		return ErrQueueFull
	}
}

// pickWorker chooses one worker for a new logical subscription key using the configured placement policy.
func (m *Mux) pickWorker(key string) (int, error) {
	snapshots := make([]MuxWorkerSnapshot, 0, len(m.workers))
	for workerIndex := range m.workers {
		worker := m.workers[workerIndex]
		snapshots = append(snapshots, MuxWorkerSnapshot{
			Index:             workerIndex,
			State:             worker.State,
			SubscriptionCount: len(worker.DesiredKeys),
		})
	}

	// workerIndex stores the placement-policy result for this logical key.
	workerIndex, placementErr := m.placementPolicy(key, snapshots)
	if placementErr != nil {
		return 0, placementErr
	}
	if workerIndex < 0 || workerIndex >= len(m.workers) {
		return 0, ErrPlacementRejected
	}
	if m.workers[workerIndex].State == MuxWorkerStateStopped {
		return 0, ErrPlacementRejected
	}
	return workerIndex, nil
}

// appendDesiredKey records one logical key in stable replay order for the target worker.
func (m *Mux) appendDesiredKey(workerIndex int, key string) {
	m.workers[workerIndex].DesiredKeys = append(m.workers[workerIndex].DesiredKeys, key)
}

// insertDesiredKey restores one logical key into stable replay order for the target worker.
func (m *Mux) insertDesiredKey(workerIndex int, key string, keyIndex int) {
	// worker stores the target runtime whose replay order is being restored.
	worker := &m.workers[workerIndex]
	if keyIndex < 0 || keyIndex >= len(worker.DesiredKeys) {
		worker.DesiredKeys = append(worker.DesiredKeys, key)
		return
	}

	worker.DesiredKeys = append(worker.DesiredKeys, "")
	copy(worker.DesiredKeys[keyIndex+1:], worker.DesiredKeys[keyIndex:])
	worker.DesiredKeys[keyIndex] = key
}

// removeDesiredKey removes one logical key from the target worker replay order.
func (m *Mux) removeDesiredKey(workerIndex int, key string) int {
	worker := &m.workers[workerIndex]
	for keyIndex, existingKey := range worker.DesiredKeys {
		if existingKey != key {
			continue
		}
		copy(worker.DesiredKeys[keyIndex:], worker.DesiredKeys[keyIndex+1:])
		worker.DesiredKeys[len(worker.DesiredKeys)-1] = ""
		worker.DesiredKeys = worker.DesiredKeys[:len(worker.DesiredKeys)-1]
		return keyIndex
	}
	return -1
}

// clearDialCancel releases one worker dial context after the connect program finishes.
func (m *Mux) clearDialCancel(workerIndex int) {
	// worker stores the runtime whose current dial context should be released.
	worker := &m.workers[workerIndex]
	if worker.DialCancel == nil {
		return
	}
	worker.DialCancel()
	worker.DialCancel = nil
}

// runWorkerLoop executes one worker loop and reports its terminal exit back to the mux.
func (m *Mux) runWorkerLoop(workerIndex int) {
	m.workers[workerIndex].Worker.loop()
	m.workerExitCh <- workerIndex
}

// startConnect launches one initial dial attempt for the selected worker.
func (m *Mux) startConnect(workerIndex int) {
	// worker stores the selected runtime that should perform one fresh initial dial.
	worker := &m.workers[workerIndex]
	worker.State = MuxWorkerStateConnecting
	if worker.DialCancel != nil {
		worker.DialCancel()
	}

	// dialCtx stores the cancellation context for this initial dial program.
	dialCtx, cancel := context.WithCancel(context.Background())
	worker.DialCancel = cancel
	worker.CommandCh <- ioCommand{
		Type:    ioCommandTypeConnect,
		DialCtx: dialCtx,
	}
}

// startReconnect transitions one worker into reconnect mode and launches its reconnect loop when needed.
func (m *Mux) startReconnect(workerIndex int) {
	// worker stores the runtime that should enter reconnect mode.
	worker := &m.workers[workerIndex]
	if worker.State == MuxWorkerStateReconnecting || worker.State == MuxWorkerStateStopped {
		return
	}

	// Flip state first so any ping tick already queued in ioWorkerCh is dropped by
	// handlePingTick, then cancel the heartbeat ticker so no further ticks are produced.
	worker.State = MuxWorkerStateReconnecting
	m.stopPingTicker(workerIndex)
	if worker.DialCancel != nil {
		worker.DialCancel()
	}

	// dialCtx stores the cancellation context for this reconnect program.
	dialCtx, cancel := context.WithCancel(context.Background())
	worker.DialCancel = cancel
	worker.CommandCh <- ioCommand{
		Type:    ioCommandTypeReconnect,
		DialCtx: dialCtx,
	}
}

// handleShutdown cancels heartbeat tickers and dial programs, closes every worker
// mailbox, drains pending events while worker loops and ticker goroutines exit, and
// finally closes the fatal and event channels. Called exactly once from the run loop
// after commandCh has been fully drained and closed.
//
// Ping tickers are cancelled before cmdCh is closed so they cannot enqueue further
// commands into a closed mailbox, and the ticker WaitGroup is joined before closing
// ioWorkerCh so no ticker goroutine ever sends on a closed event channel.
func (m *Mux) handleShutdown() {
	for workerIndex := range m.workers {
		// worker stores the runtime being shut down.
		worker := &m.workers[workerIndex]
		m.stopPingTicker(workerIndex)
		worker.State = MuxWorkerStateStopped
		if worker.DialCancel != nil {
			worker.DialCancel()
			worker.DialCancel = nil
		}
		close(worker.CommandCh)
	}

	// tickersDone fires once every heartbeat ticker goroutine has fully exited.
	tickersDone := make(chan struct{})
	go func() {
		m.pingTickerWG.Wait()
		close(tickersDone)
	}()

	// remainingWorkers stores how many worker loops still need to report their terminal exit.
	remainingWorkers := len(m.workers)
	tickersExited := false
	for remainingWorkers > 0 || !tickersExited {
		select {
		case <-m.ioWorkerCh:
		case <-m.workerExitCh:
			remainingWorkers--
		case <-tickersDone:
			tickersExited = true
		}
	}

	close(m.ioWorkerCh)
	close(m.fatalCh)
	m.state = muxRunStateStopped
}

// beginFatalShutdown transitions the mux into stopping state and publishes the fatal cause once.
func (m *Mux) beginFatalShutdown(fatalErr error) {
	if fatalErr == nil {
		return
	}
	if m.state != muxRunStateRunning {
		return
	}
	m.state = muxRunStateStopping
	m.shutdownErr = fatalErr
	m.publishFatal(fatalErr)
}

// shutdownResolveError returns the best terminal error for commands abandoned during shutdown.
func (m *Mux) shutdownResolveError() error {
	if m.shutdownErr != nil {
		return m.shutdownErr
	}
	return ErrMuxStopped
}

// publishFatal writes one FatalEvent into the buffered fatalCh without ever blocking.
//
// fatalCh has capacity one and the publish path is the only producer, so the buffered
// slot is always free on the first call. The non-blocking select default exists purely
// as a safety net against any future code path that might attempt a second publish.
func (m *Mux) publishFatal(err error) {
	select {
	case m.fatalCh <- FatalEvent{Err: err}:
	default:
	}
}

// defaultPlacementPolicy chooses the least-loaded connected worker and falls back to the least-loaded non-stopped worker.
func defaultPlacementPolicy(key string, workers []MuxWorkerSnapshot) (int, error) {
	_ = key

	// connectedIndex stores the least-loaded connected worker found so far.
	connectedIndex := -1
	// connectedCount stores the load of connectedIndex.
	connectedCount := 0
	// fallbackIndex stores the least-loaded non-stopped worker found so far.
	fallbackIndex := -1
	// fallbackCount stores the load of fallbackIndex.
	fallbackCount := 0
	for _, worker := range workers {
		if worker.State == MuxWorkerStateStopped {
			continue
		}

		if fallbackIndex == -1 || worker.SubscriptionCount < fallbackCount {
			fallbackIndex = worker.Index
			fallbackCount = worker.SubscriptionCount
		}
		if worker.State != MuxWorkerStateConnected {
			continue
		}
		if connectedIndex == -1 || worker.SubscriptionCount < connectedCount {
			connectedIndex = worker.Index
			connectedCount = worker.SubscriptionCount
		}
	}

	if connectedIndex >= 0 {
		return connectedIndex, nil
	}
	if fallbackIndex >= 0 {
		return fallbackIndex, nil
	}
	return 0, ErrMuxCapacityReached
}
