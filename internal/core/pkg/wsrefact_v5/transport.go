package wsrefact_v5

import (
	"time"

	"github.com/gorilla/websocket"
)

// transportConfig stores immutable runtime behavior for one websocket connection generation.
type transportConfig struct {
	// handlers stores the synchronous inbound callback set for this generation.
	handlers Handlers
	// writeTimeout stores the default websocket write timeout for this generation.
	writeTimeout time.Duration
	// pingInterval stores the heartbeat cadence for this generation.
	pingInterval time.Duration
	// writeQueueSize stores the buffered ioWorker event capacity for this generation.
	writeQueueSize int
}

// transport groups one live session with its matching conn-bound writer.
type transport struct {
	// session stores the read-loop owner for this connection generation.
	session *session
	// worker stores the serialized write executor for this connection generation.
	worker *ioWorker
}

// newTransport creates one complete connection generation and starts its goroutines.
func newTransport(conn *websocket.Conn, config transportConfig, eventSink chan<- muxEvent, shutdownCh <-chan struct{}) (*transport, error) {
	// liveSession stores the freshly established websocket session wrapper.
	liveSession := newSession(conn, sessionConfig{
		handlers:     config.handlers,
		writeTimeout: config.writeTimeout,
	})
	if err := liveSession.Start(eventSink, shutdownCh); err != nil {
		liveSession.Stop()
		return nil, err
	}

	// liveWorker stores the freshly established conn-bound writer.
	liveWorker := newIOWorker(liveSession, ioWorkerConfig{
		pingInterval: config.pingInterval,
		queueSize:    config.writeQueueSize,
	})
	if err := liveWorker.Start(eventSink, shutdownCh); err != nil {
		liveSession.Stop()
		liveSession.Wait()
		return nil, err
	}

	// liveTransport stores the complete connection generation runtime.
	liveTransport := &transport{
		session: liveSession,
		worker:  liveWorker,
	}
	return liveTransport, nil
}

// Dispose tears down this connection generation without waiting for completion.
func (transport *transport) Dispose() {
	if transport == nil {
		return
	}

	transport.session.Stop()
	transport.worker.Dispose()
}

// Wait blocks until both the session read loop and the conn-bound writer exit.
func (transport *transport) Wait() {
	if transport == nil {
		return
	}

	transport.worker.Wait()
	transport.session.Wait()
}
