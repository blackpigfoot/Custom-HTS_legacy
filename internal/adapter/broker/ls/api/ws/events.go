package ws

import (
	"encoding/json"
	"time"
)

// EventKind identifies an asynchronous LS realtime websocket event.
type EventKind string

const (
	// EventWSConnected reports that the websocket transport connected.
	EventWSConnected EventKind = "ws_connected"
	// EventWSReconnected reports that the websocket transport reconnected after at least one retry.
	EventWSReconnected EventKind = "ws_reconnected"
	// EventWSDisconnected reports that the websocket transport disconnected.
	EventWSDisconnected EventKind = "ws_disconnected"
	// EventWSReconnecting reports that the websocket transport started a reconnect attempt.
	EventWSReconnecting EventKind = "ws_reconnecting"
	// EventWSFatal reports that the websocket transport reached a fatal terminal error.
	EventWSFatal EventKind = "ws_fatal"
	// EventControlResponse reports a websocket subscribe or unsubscribe response.
	EventControlResponse EventKind = "control_response"
	// EventJSONDecodeFailed reports a failure to decode the outer websocket JSON envelope.
	EventJSONDecodeFailed EventKind = "json_decode_failed"
	// EventRealtimeBodyDecodeFailed reports a failure to decode a typed realtime body.
	EventRealtimeBodyDecodeFailed EventKind = "realtime_body_decode_failed"
	// EventMissingRealtimeTR reports a realtime packet without a TR code.
	EventMissingRealtimeTR EventKind = "missing_realtime_tr"
	// EventUnknownRealtimeTR reports a realtime packet with no registered websocket stream.
	EventUnknownRealtimeTR EventKind = "unknown_realtime_tr"
	// EventWSChannelFull reports that a low-level websocket stream channel was full.
	EventWSChannelFull EventKind = "ws_channel_full"
	// EventDriverChannelFull keeps the previous event name as a compatibility alias.
	EventDriverChannelFull EventKind = EventWSChannelFull
	// EventRawRealtimeIgnored reports a non-JSON realtime payload that the websocket service ignored.
	EventRawRealtimeIgnored EventKind = "raw_realtime_ignored"
)

// Event reports asynchronous websocket events without deciding how to log or recover.
//
// Event is intentionally flat for simple channel consumption. Not every field
// is meaningful for every Kind; callers should interpret fields according to
// Kind. A future stricter API can split these fields into typed event payload
// variants.
type Event struct {
	// Kind identifies the event category.
	Kind EventKind
	// Layer identifies the package layer that emitted the event.
	Layer string
	// ConnID identifies the websocket connection slot that emitted the event.
	ConnID string
	// Stream identifies the logical realtime stream such as trade, quote, or execution.
	Stream string
	// TRType is the LS websocket action code when present.
	TRType string
	// TRCode is the vendor-native realtime TR code when present.
	TRCode string
	// TRKey is the vendor-native realtime routing key when present.
	TRKey string
	// RspCd is the LS business response code when present.
	RspCd string
	// RspMsg is the LS business response message when present.
	RspMsg string
	// MsgCd is the optional LS message code when present.
	MsgCd string
	// Msg is the optional LS message text when present.
	Msg string
	// Success reports whether a control response represents an accepted request.
	Success bool
	// Attempt stores the reconnect attempt count when the transport reports it.
	Attempt int
	// Err carries the wrapped failure when the event represents an error.
	Err error
	// Size stores the original websocket payload size when known.
	Size int
	// Payload keeps the raw control or realtime body for callers that need vendor detail.
	Payload json.RawMessage
	// ReceivedAt is the local time when the websocket payload reached the service.
	ReceivedAt time.Time
	// At is the local time when this event was emitted.
	At time.Time
}
