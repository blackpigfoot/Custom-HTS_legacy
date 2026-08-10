package wsrefact_v5

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ioWorkerConfig stores immutable runtime behavior for one conn-bound writer.
type ioWorkerConfig struct {
	// pingInterval controls heartbeat cadence for this writer. Zero disables pings.
	pingInterval time.Duration
	// queueSize is the buffered outbound event capacity.
	queueSize int
}

// ioRequest stores one outbound websocket write and its completion sink.
type ioRequest struct {
	// payload stores the outbound websocket frame body.
	payload []byte
	// messageType stores the websocket opcode for this frame.
	messageType int
	// writeTimeout overrides the default session write timeout for this frame.
	writeTimeout time.Duration
	// subscription stores the caller-visible completion handle for asynchronous requests.
	subscription *Subscription
	// resultCh stores the private synchronous result sink for replay and OnReady writes.
	resultCh chan error
}

// ioWorkerEventType identifies one event consumed by the conn-bound writer loop.
type ioWorkerEventType int

const (
	// ioWorkerEventWrite reports one outbound websocket write request.
	ioWorkerEventWrite ioWorkerEventType = iota
	// ioWorkerEventDispose reports that the worker should finish queued writes and then exit.
	ioWorkerEventDispose
)

// ioWorkerEvent stores one event consumed by the conn-bound writer loop.
type ioWorkerEvent struct {
	// kind identifies the worker event variant.
	kind ioWorkerEventType
	// request stores the outbound write request when kind is ioWorkerEventWrite.
	request *ioRequest
}

// workerFailureEvent reports one ioWorker write failure back to the single-thread client loop.
type workerFailureEvent struct {
	// worker identifies the originating ioWorker instance.
	worker *ioWorker
	// err stores the write failure observed by the worker.
	err error
}

// ioWorker serializes all outbound writes for exactly one live session.
type ioWorker struct {
	// session stores the owned live session written by this worker.
	session *session
	// config stores immutable writer runtime behavior.
	config ioWorkerConfig
	// eventCh stores buffered worker events for this live connection.
	eventCh chan ioWorkerEvent
	// done closes when the worker fully exits after its dispose event is processed.
	done chan struct{}
	// started guards against duplicate worker starts.
	started atomic.Bool
	// disposeOnce guarantees that exactly one dispose event is issued.
	disposeOnce sync.Once
}

// newIOWorker allocates one conn-bound websocket writer.
func newIOWorker(session *session, config ioWorkerConfig) *ioWorker {
	// queueSize stores the normalized buffered event capacity.
	queueSize := config.queueSize
	if queueSize <= 0 {
		queueSize = 64
	}

	// worker owns serialized writes for one live websocket connection.
	worker := &ioWorker{
		session: session,
		config:  config,
		eventCh: make(chan ioWorkerEvent, queueSize),
		done:    make(chan struct{}),
	}
	return worker
}

// Start launches the conn-bound write loop.
func (worker *ioWorker) Start(eventSink chan<- muxEvent, shutdownCh <-chan struct{}) error {
	if !worker.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}

	go worker.run(eventSink, shutdownCh)
	return nil
}

// Dispose asks the worker to finish already-queued writes and then exit.
func (worker *ioWorker) Dispose() {
	if !worker.started.Load() {
		return
	}

	worker.disposeOnce.Do(func() {
		// disposeEvent stores the terminal worker event for this connection generation.
		disposeEvent := ioWorkerEvent{
			kind: ioWorkerEventDispose,
		}
		worker.eventCh <- disposeEvent
	})
}

// Wait blocks until the worker fully exits.
func (worker *ioWorker) Wait() {
	<-worker.done
}

// TryEnqueue attempts one non-blocking asynchronous handoff to the worker.
func (worker *ioWorker) TryEnqueue(payload []byte, messageType int, writeTimeout time.Duration, subscription *Subscription) bool {
	// request stores one asynchronous outbound websocket write.
	request := &ioRequest{
		payload:      payload,
		messageType:  messageType,
		writeTimeout: writeTimeout,
		subscription: subscription,
	}
	// event stores one buffered worker event for this asynchronous write.
	event := ioWorkerEvent{
		kind:    ioWorkerEventWrite,
		request: request,
	}

	select {
	case worker.eventCh <- event:
		return true
	default:
		return false
	}
}

// SendSync writes one frame synchronously through the worker event queue.
func (worker *ioWorker) SendSync(payload []byte, messageType int, writeTimeout time.Duration) error {
	// resultCh stores the synchronous completion sink for this write.
	resultCh := make(chan error, 1)
	// request stores one synchronous outbound websocket write.
	request := &ioRequest{
		payload:      payload,
		messageType:  messageType,
		writeTimeout: writeTimeout,
		resultCh:     resultCh,
	}
	// event stores one buffered worker event for this synchronous write.
	event := ioWorkerEvent{
		kind:    ioWorkerEventWrite,
		request: request,
	}

	worker.eventCh <- event
	return <-resultCh
}

func (worker *ioWorker) run(eventSink chan<- muxEvent, shutdownCh <-chan struct{}) {
	defer close(worker.done)

	// ticker drives periodic heartbeat writes when enabled.
	var ticker *time.Ticker
	// pingCh exposes the active ticker channel to the select loop.
	var pingCh <-chan time.Time
	if worker.config.pingInterval > 0 {
		ticker = time.NewTicker(worker.config.pingInterval)
		pingCh = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case event := <-worker.eventCh:
			if worker.handleEvent(eventSink, shutdownCh, event) {
				return
			}
		case <-pingCh:
			worker.handlePing(eventSink, shutdownCh)
		}
	}
}

func (worker *ioWorker) handleEvent(eventSink chan<- muxEvent, shutdownCh <-chan struct{}, event ioWorkerEvent) (shouldStop bool) {
	switch event.kind {
	case ioWorkerEventDispose:
		return true
	case ioWorkerEventWrite:
		if event.request != nil {
			worker.handleRequest(eventSink, shutdownCh, event.request)
		}
	}
	return false
}

func (worker *ioWorker) handleRequest(eventSink chan<- muxEvent, shutdownCh <-chan struct{}, request *ioRequest) {
	// writeErr stores the terminal result for this outbound frame.
	writeErr := worker.session.writeOne(request.payload, request.messageType, request.writeTimeout)
	if request.subscription != nil {
		request.subscription.resolve(writeErr)
	}
	if request.resultCh != nil {
		request.resultCh <- writeErr
		close(request.resultCh)
	}
	if writeErr == nil {
		return
	}

	worker.publishFailure(eventSink, shutdownCh, workerFailureEvent{
		worker: worker,
		err:    writeErr,
	})
}

func (worker *ioWorker) handlePing(eventSink chan<- muxEvent, shutdownCh <-chan struct{}) {
	// writeErr stores the heartbeat write result.
	writeErr := worker.session.writeOne(nil, websocket.PingMessage, 0)
	if writeErr == nil {
		return
	}

	worker.publishFailure(eventSink, shutdownCh, workerFailureEvent{
		worker: worker,
		err:    writeErr,
	})
}

func (worker *ioWorker) publishFailure(eventSink chan<- muxEvent, shutdownCh <-chan struct{}, event workerFailureEvent) {
	select {
	case eventSink <- event:
	case <-shutdownCh:
	}
}
