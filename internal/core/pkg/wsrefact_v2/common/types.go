package common

import (
	"fmt"
	"time"
)

// ConnState represents the connection lifecycle state of the reconnecting transport.
type ConnState int

const (
	// ConnStateStopped reports that the transport is not running.
	ConnStateStopped ConnState = iota
	// ConnStateReconnecting reports that the transport is trying to establish a session.
	ConnStateReconnecting
	// ConnStateConnected reports that the transport currently owns a live session.
	ConnStateConnected
)

// String formats the lifecycle state for debugging and diagnostics.
func (state ConnState) String() string {
	switch state {
	case ConnStateStopped:
		return "Stopped"
	case ConnStateReconnecting:
		return "Reconnecting"
	case ConnStateConnected:
		return "Connected"
	default:
		return fmt.Sprintf("ConnState(%d)", int(state))
	}
}

// ConnEventType identifies one websocket lifecycle event.
type ConnEventType string

const (
	// Connected reports that the first live session became ready.
	Connected ConnEventType = "connected"
	// Reconnected reports that a later live session became ready after a disconnect.
	Reconnected ConnEventType = "reconnected"
	// Disconnected reports that the live session ended.
	Disconnected ConnEventType = "disconnected"
	// Reconnecting reports that the transport is waiting before the next dial attempt.
	Reconnecting ConnEventType = "reconnecting"
)

// ConnEvent reports one websocket lifecycle transition.
type ConnEvent struct {
	// Type identifies the lifecycle transition.
	Type ConnEventType
	// Err carries the lifecycle failure when applicable.
	Err error
	// Attempt stores the reconnect attempt count when applicable.
	Attempt int
	// At is the local time when the event was emitted.
	At time.Time
}

// GenFunc builds one websocket payload on demand.
//
// GenFunc must be a fast synchronous function. It is invoked by the session
// send loop, which processes one request at a time, so a slow GenFunc
// blocks every other queued request. If freshness, nonce lookup, or
// cancellation is required, the caller must capture them in a closure
// before enqueueing.
type GenFunc func() ([]byte, error)

// Handlers contains synchronous websocket callbacks.
type Handlers struct {
	// OnMessage handles one inbound websocket payload.
	//
	// OnMessage is called synchronously by the read loop and must return
	// promptly. The data slice is valid only until OnMessage returns; copy it if
	// the handler stores it or passes it to another goroutine.
	OnMessage func(data []byte)
	// OnStatus handles one websocket lifecycle event.
	//
	// OnStatus is called synchronously by the transport lifecycle. Slow handlers
	// backpressure reconnect and shutdown progress.
	OnStatus func(event ConnEvent)
}

var noopHandlers = Handlers{
	// OnMessage returns immediately when no message handler is configured.
	OnMessage: func([]byte) {},
	// OnStatus returns immediately when no lifecycle handler is configured.
	OnStatus: func(ConnEvent) {},
}

// NormalizeHandlers replaces nil callbacks with fast no-op handlers.
func NormalizeHandlers(handlers Handlers) Handlers {
	if handlers.OnMessage == nil {
		handlers.OnMessage = noopHandlers.OnMessage
	}
	if handlers.OnStatus == nil {
		handlers.OnStatus = noopHandlers.OnStatus
	}
	return handlers
}
