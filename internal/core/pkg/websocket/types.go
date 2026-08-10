package websocket

import "Custom-HTS/internal/core/pkg/websocket/common"

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

// GenFunc builds one websocket payload on demand.
type GenFunc = common.GenFunc

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
	// Fatal reports that the transport reached a terminal failure.
	Fatal = common.Fatal
)

// ConnEvent reports one websocket lifecycle transition.
type ConnEvent = common.ConnEvent

// WsHandlers contains synchronous websocket callbacks.
type WsHandlers = common.WsHandlers

// SubOptions are per-subscription write options.
type SubOptions = common.SubOptions

// SubOption mutates SubOptions.
type SubOption = common.SubOption
