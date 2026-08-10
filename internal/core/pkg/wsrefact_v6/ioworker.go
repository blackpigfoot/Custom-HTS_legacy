package wsrefact_v6

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/gorilla/websocket"
)

// EncodeWrite delegates to the wrapped function value.
func (encoder WriteEncoderFunc) EncodeWrite(intent WriteIntent) (EncodedWrite, error) {
	return encoder(intent)
}

// ioWorker serializes websocket writes and reconnect retries in one goroutine.
//
// Ownership rule:
//   - Upper layers send immutable commands and immutable write intents.
//   - The worker owns one shared encoder that turns intents into websocket frames.
//   - The worker loop alone mutates conn and executes websocket io.
//   - One reconnect command owns the whole reconnect retry mechanism.
//   - Upper layers own the reconnect stop policy through DialCtx.
type ioWorker struct {
	// workerIndex stores the mux-local worker slot published with internal events.
	workerIndex int
	// conn stores the currently owned websocket connection and is mutated only by the loop goroutine.
	conn *websocket.Conn
	// cmdCh stores the serialized mailbox consumed by the loop goroutine.
	cmdCh <-chan ioCommand
	// eventSink stores the mux-owned internal worker event channel.
	eventSink chan<- ioWorkerEvent
	// writeEncoder stores the shared logical-to-wire encoder used by every write command.
	writeEncoder WriteEncoder
	// onConnected observes each successful live websocket connection.
	onConnected func(workerIndex int, conn *websocket.Conn)
	// onMessage is called for every inbound websocket frame received by the internal read loop.
	onMessage func(workerIndex int, msgType int, data []byte)
	// connCancel cancels the context owned by the current read loop goroutine.
	connCancel context.CancelFunc
	// factory stores the websocket dial factory used for connect and reconnect attempts.
	factory dialFactory
	// defaultWriteTimeout stores the fallback websocket write timeout for outbound frames.
	defaultWriteTimeout time.Duration
	// dialTimeout stores the connect timeout applied to each dial attempt.
	dialTimeout time.Duration
	// reconnectMin stores the minimum reconnect delay duration.
	reconnectMin time.Duration
	// reconnectMax stores the maximum reconnect delay duration.
	reconnectMax time.Duration
}

// newIOWorker allocates one serialized websocket io worker and validates all required configuration eagerly.
func newIOWorker(config ioWorkerConfig) (*ioWorker, error) {
	if config.CommandCh == nil {
		return nil, errCommandChannelRequired
	}
	if config.EventSink == nil {
		return nil, errEventSinkRequired
	}
	if config.WriteEncoder == nil {
		return nil, errWriteEncoderRequired
	}
	if config.Dialer == nil {
		return nil, ErrDialerRequired
	}
	if config.URL == "" {
		return nil, ErrURLRequired
	}

	// factory stores the validated dial factory used by this worker.
	factory := dialFactory{
		Dialer: config.Dialer,
		URL:    config.URL,
		Header: config.Header,
	}

	// worker stores the fully configured websocket io executor.
	worker := &ioWorker{
		workerIndex:         config.WorkerIndex,
		cmdCh:               config.CommandCh,
		eventSink:           config.EventSink,
		writeEncoder:        config.WriteEncoder,
		onConnected:         config.OnConnected,
		onMessage:           config.OnMessage,
		factory:             factory,
		defaultWriteTimeout: config.DefaultWriteTimeout,
		dialTimeout:         config.DialTimeout,
		reconnectMin:        config.ReconnectMin,
		reconnectMax:        config.ReconnectMax,
	}
	return worker, nil
}

// loop runs the serialized websocket io command loop until cmdCh closes.
func (worker *ioWorker) loop() {
	for command := range worker.cmdCh {
		switch command.Type {
		case ioCommandTypeConnect:
			worker.handleConnect(command)
		case ioCommandTypeReconnect:
			worker.handleReconnect(command)
		case ioCommandTypeWrite:
			worker.handleWrite(command)
		case ioCommandTypeReplayWrite:
			worker.handleReplayWrite(command)
		case ioCommandTypePing:
			worker.handlePing(command)
		default:
			panic(errUnknownCommand)
		}
	}

	worker.dropConn()
}

// handleConnect performs one dial attempt and publishes exactly one terminal result.
func (worker *ioWorker) handleConnect(command ioCommand) {
	if command.DialCtx == nil {
		panic(errDialContextRequired)
	}

	worker.dropConn()

	conn, err := worker.dialConn(command.DialCtx)
	worker.conn = conn
	
	if err != nil {
		worker.publishEvent(ioWorkerEvent{
			Kind:        ioWorkerEventKindConnectFailed,
			WorkerIndex: worker.workerIndex,
			Err:         err,
		})
		return
	}

	worker.publishEvent(ioWorkerEvent{
		Kind:        ioWorkerEventKindConnected,
		WorkerIndex: worker.workerIndex,
	})
	worker.notifyConnected(conn)
}

// handleReconnect runs one full reconnect retry program and publishes exactly one terminal success event.
func (worker *ioWorker) handleReconnect(command ioCommand) {
	if command.DialCtx == nil {
		panic(errDialContextRequired)
	}

	worker.dropConn()

	conn := worker.runReconnect(command.DialCtx)
	if command.DialCtx.Err() != nil || conn == nil {
		return
	}

	worker.conn = conn
	worker.publishEvent(ioWorkerEvent{
		Kind:        ioWorkerEventKindReconnected,
		WorkerIndex: worker.workerIndex,
	})
	worker.notifyConnected(conn)
}

// handleWrite executes one outbound websocket frame write against the current live connection.
func (worker *ioWorker) handleWrite(command ioCommand) {
	if command.Intent == nil {
		panic(errWriteIntentRequired)
	}
	if command.Subscription == nil {
		panic(errSubscriptionRequired)
	}
	if worker.conn == nil {
		command.Subscription.resolve(ErrNotConnected)
		worker.publishWriteFailure(command, ErrNotConnected)
		return
	}

	encoded, encodeErr := worker.encodeWrite(*command.Intent)
	if encodeErr != nil {
		command.Subscription.resolve(encodeErr)
		worker.publishWriteFailure(command, encodeErr)
		return
	}

	writeErr := worker.writeOne(encoded)
	command.Subscription.resolve(writeErr)
	if writeErr != nil {
		worker.closeConn()
	}
}

// handleReplayWrite executes one fire-and-forget replay write against the current live connection.
//
// Replay writes never resolve a caller-facing subscription because they are driven only by
// retained desired state. Failures still surface through the internal worker event sink.
func (worker *ioWorker) handleReplayWrite(command ioCommand) {
	if command.Intent == nil {
		panic(errWriteIntentRequired)
	}
	if worker.conn == nil {
		worker.publishWriteFailure(command, ErrNotConnected)
		return
	}

	encoded, encodeErr := worker.encodeWrite(*command.Intent)
	if encodeErr != nil {
		worker.publishWriteFailure(command, encodeErr)
		return
	}

	writeErr := worker.writeOne(encoded)
	if writeErr != nil {
		worker.closeConn()
	}
}

// handlePing executes one fire-and-forget heartbeat ping write against the current live connection.
//
// Mux ownership rule: ping commands only arrive when the mux already verified that the
// worker is Connected and that the ping tick belongs to the current heartbeat epoch. If
// the connection has nevertheless been lost in the brief gap between mux dispatch and
// worker pickup, the ping is dropped silently because the read loop will publish the
// disconnect on its own.
func (worker *ioWorker) handlePing(command ioCommand) {
	if command.Intent == nil {
		panic(errWriteIntentRequired)
	}
	if worker.conn == nil {
		return
	}

	encoded, encodeErr := worker.encodeWrite(*command.Intent)
	if encodeErr != nil {
		worker.publishWriteFailure(command, encodeErr)
		return
	}

	writeErr := worker.writeOne(encoded)
	if writeErr != nil {
		worker.closeConn()
	}
}

// notifyConnected starts the internal read loop and invokes the optional lifecycle hook.
func (worker *ioWorker) notifyConnected(conn *websocket.Conn) {
	connCtx, connCancel := context.WithCancel(context.Background())
	worker.connCancel = connCancel
	go worker.readLoop(conn, connCtx)
	if worker.onConnected != nil {
		worker.onConnected(worker.workerIndex, conn)
	}
}

// readLoop drains inbound websocket frames until the connection dies or its context is cancelled.
//
// A cancelled context means the connection was closed intentionally (reconnect or shutdown),
// so the loop exits silently. An uncancelled context means the drop was unexpected, so the
// loop publishes exactly one ioWorkerEventKindDisconnected to trigger a mux reconnect.
func (worker *ioWorker) readLoop(conn *websocket.Conn, connCtx context.Context) {
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			if connCtx.Err() != nil {
				return
			}
			worker.publishEvent(ioWorkerEvent{
				Kind:        ioWorkerEventKindDisconnected,
				WorkerIndex: worker.workerIndex,
				Err:         err,
			})
			return
		}
		if worker.onMessage != nil {
			worker.onMessage(worker.workerIndex, msgType, data)
		}
	}
}

// closeConn closes the current connection without releasing ownership.
//
// Used by failed writes to unblock the read loop so it can detect the disconnect
// and publish ioWorkerEventKindDisconnected exactly once.
func (worker *ioWorker) closeConn() {
	if worker.conn != nil {
		_ = worker.conn.Close()
	}
}

// publishWriteFailure forwards one failed write into the internal event sink when requested.
func (worker *ioWorker) publishWriteFailure(command ioCommand, err error) {
	if err == nil || !command.PublishWriteFailure {
		return
	}
	worker.publishEvent(ioWorkerEvent{
		Kind:        ioWorkerEventKindWriteFailed,
		WorkerIndex: worker.workerIndex,
		Intent:      *command.Intent,
		Err:         err,
	})
}

// publishEvent forwards one internal worker event into the mux-owned event channel.
func (worker *ioWorker) publishEvent(event ioWorkerEvent) {
	worker.eventSink <- event
}

// runReconnect keeps retrying reconnect until one dial succeeds or the upper-layer reconnect context ends.
func (worker *ioWorker) runReconnect(reconnectCtx context.Context) *websocket.Conn {
	for attempt := 1; ; attempt++ {
		// shouldDial reports whether this reconnect program is still allowed to advance into the next dial attempt.
		shouldDial := worker.waitReconnectDelay(reconnectCtx, attempt)
		if !shouldDial {
			return nil
		}

		// nextConn stores the websocket connection returned by this dial attempt when it succeeds.
		nextConn, err := worker.dialConn(reconnectCtx)
		if err == nil {
			return nextConn
		}
		if reconnectCtx.Err() != nil {
			return nil
		}
	}
}

// dropConn cancels the read loop context, closes the connection, and forgets both.
func (worker *ioWorker) dropConn() {
	if worker.connCancel != nil {
		worker.connCancel()
		worker.connCancel = nil
	}
	if worker.conn != nil {
		_ = worker.conn.Close()
	}
	worker.conn = nil
}

// dialConn performs one dial attempt using the configured factory and the upper-layer reconnect context.
func (worker *ioWorker) dialConn(reconnectCtx context.Context) (*websocket.Conn, error) {
	if worker.dialTimeout <= 0 {
		return worker.factory.Dial(reconnectCtx)
	}

	// dialCtx stores the dial context bounded by the effective dial timeout.
	dialCtx, cancel := context.WithTimeout(reconnectCtx, worker.dialTimeout)
	defer cancel()
	return worker.factory.Dial(dialCtx)
}

// encodeWrite translates one logical write intent into one websocket frame through the shared encoder.
func (worker *ioWorker) encodeWrite(intent WriteIntent) (EncodedWrite, error) {
	// encoded stores the websocket frame produced by the shared encoder.
	encoded, encodeErr := worker.writeEncoder.EncodeWrite(intent)
	if encodeErr != nil {
		return EncodedWrite{}, &EncodeWriteError{
			Err: encodeErr,
		}
	}
	if encoded.MessageType == 0 {
		panic(errEncodedMessageTypeRequired)
	}
	return encoded, nil
}

// writeOne performs one websocket frame write through the current live connection.
func (worker *ioWorker) writeOne(encoded EncodedWrite) error {
	if worker.defaultWriteTimeout > 0 {
		// deadline stores the absolute websocket write deadline for this frame.
		deadline := time.Now().Add(worker.defaultWriteTimeout)
		if err := worker.conn.SetWriteDeadline(deadline); err != nil {
			return &OperationError{
				Op:  "set write deadline",
				Err: err,
			}
		}
	}

	if err := worker.conn.WriteMessage(encoded.MessageType, encoded.Payload); err != nil {
		return &OperationError{
			Op:  "write message",
			Err: err,
		}
	}
	return nil
}

// waitReconnectDelay blocks for the computed reconnect delay unless the upper-layer reconnect context ends first.
func (worker *ioWorker) waitReconnectDelay(reconnectCtx context.Context, attempt int) bool {
	// delay stores the reconnect delay duration for this attempt.
	delay := worker.calcReconnectDelay(attempt)
	if delay <= 0 {
		select {
		case <-reconnectCtx.Done():
			return false
		default:
			return true
		}
	}

	// timer gates the reconnect delay window for this attempt.
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-reconnectCtx.Done():
		return false
	}
}

// calcReconnectDelay returns one exponential reconnect delay with 25 percent jitter.
func (worker *ioWorker) calcReconnectDelay(attempt int) time.Duration {
	if attempt <= 0 || worker.reconnectMin <= 0 {
		return 0
	}

	// baseDelay stores the unclamped exponential reconnect delay in nanoseconds.
	baseDelay := float64(worker.reconnectMin) * math.Pow(2, float64(attempt-1))
	if worker.reconnectMax > 0 && baseDelay > float64(worker.reconnectMax) {
		baseDelay = float64(worker.reconnectMax)
	}

	// jitter adds one small random spread so reconnects do not align too tightly.
	jitter := baseDelay * 0.25 * rand.Float64()
	return time.Duration(baseDelay + jitter)
}
