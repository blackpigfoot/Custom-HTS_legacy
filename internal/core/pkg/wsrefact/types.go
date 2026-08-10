package wsrefact

import (
	"Custom-HTS/_legacy/internal/core/pkg/wsrefact/common"
	replaypkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact/replay"
	supervisorpkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact/supervisor"
)

// ConnState represents the connection lifecycle state of the reconnecting transport.
type ConnState = common.ConnState

const (
	// ConnStateStopped reports that the transport is not running.
	ConnStateStopped = common.ConnStateStopped
	// ConnStateReconnecting reports that the transport is trying to establish a session.
	ConnStateReconnecting = common.ConnStateReconnecting
	// ConnStateConnected reports that the transport currently owns a live session.
	ConnStateConnected = common.ConnStateConnected
)

// ConnEventType identifies one websocket lifecycle event.
type ConnEventType = common.ConnEventType

const (
	// Connected reports that the first live session became ready.
	Connected = common.Connected
	// Reconnected reports that a later live session became ready after a disconnect.
	Reconnected = common.Reconnected
	// Disconnected reports that the live session ended.
	Disconnected = common.Disconnected
	// Reconnecting reports that the transport is waiting before the next dial attempt.
	Reconnecting = common.Reconnecting
)

// ConnEvent reports one websocket lifecycle transition.
type ConnEvent = common.ConnEvent

// GenFunc builds one websocket payload on demand.
type GenFunc = common.GenFunc

// Handlers contains synchronous websocket callbacks.
type Handlers = common.Handlers

// WriteOptions are per-request websocket write overrides.
type WriteOptions = common.WriteOptions

// WriteOption mutates WriteOptions.
type WriteOption = common.WriteOption

// WriteRequest reuses the supervisor-owned queued websocket write request.
type WriteRequest = supervisorpkg.WriteRequest

// ReplayWriteRequest keeps the replay alias explicit for callers that prefer replay naming.
type ReplayWriteRequest = replaypkg.WriteRequest
