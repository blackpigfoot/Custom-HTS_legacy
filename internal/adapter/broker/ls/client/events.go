package client

import (
	"encoding/json"
	"time"

	lsapi "Custom-HTS/internal/adapter/broker/ls/api"
)

// EventKind identifies an asynchronous LS client event.
type EventKind string

const (
	// EventWSConnected reports that the websocket transport connected.
	EventWSConnected EventKind = EventKind(lsapi.EventWSConnected)
	// EventWSReconnected reports that the websocket transport reconnected after at least one retry.
	EventWSReconnected EventKind = EventKind(lsapi.EventWSReconnected)
	// EventWSDisconnected reports that the websocket transport disconnected.
	EventWSDisconnected EventKind = EventKind(lsapi.EventWSDisconnected)
	// EventWSReconnecting reports that the websocket transport started a reconnect attempt.
	EventWSReconnecting EventKind = EventKind(lsapi.EventWSReconnecting)
	// EventWSFatal reports that the websocket transport reached a fatal terminal error.
	EventWSFatal EventKind = EventKind(lsapi.EventWSFatal)
	// EventControlResponse reports a websocket subscribe or unsubscribe response.
	EventControlResponse EventKind = EventKind(lsapi.EventControlResponse)
	// EventJSONDecodeFailed reports a websocket failure to decode the outer JSON envelope.
	EventJSONDecodeFailed EventKind = EventKind(lsapi.EventJSONDecodeFailed)
	// EventRealtimeBodyDecodeFailed reports a websocket failure to decode a typed realtime body.
	EventRealtimeBodyDecodeFailed EventKind = EventKind(lsapi.EventRealtimeBodyDecodeFailed)
	// EventMissingRealtimeTR reports a websocket realtime packet without a TR code.
	EventMissingRealtimeTR EventKind = EventKind(lsapi.EventMissingRealtimeTR)
	// EventUnknownRealtimeTR reports a websocket realtime packet with no registered stream.
	EventUnknownRealtimeTR EventKind = EventKind(lsapi.EventUnknownRealtimeTR)
	// EventDriverChannelFull keeps the previous channel-full event name as a compatibility alias.
	EventDriverChannelFull EventKind = EventKind(lsapi.EventDriverChannelFull)
	// EventWSChannelFull reports that a low-level websocket stream channel was full.
	EventWSChannelFull EventKind = EventKind(lsapi.EventWSChannelFull)
	// EventRawRealtimeIgnored reports a non-JSON realtime payload ignored by the websocket service.
	EventRawRealtimeIgnored EventKind = EventKind(lsapi.EventRawRealtimeIgnored)
	// EventSubscriberChannelFull reports that a client-local subscriber channel was full.
	EventSubscriberChannelFull EventKind = "subscriber_channel_full"
)

// Event reports asynchronous client-facing LS events.
//
// Event is intentionally flat for simple channel consumption. Not every field
// is meaningful for every Kind; callers should interpret fields according to
// Kind. A future stricter API can split these fields into typed event payload
// variants.
type Event struct {
	// Kind identifies the event category.
	Kind EventKind
	// Layer identifies the package layer that originally emitted the event.
	Layer string
	// ConnID identifies the websocket connection slot that originally emitted the event.
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

func eventFromWS(src lsapi.Event) Event {
	return Event{
		Kind:       EventKind(src.Kind),
		Layer:      src.Layer,
		ConnID:     src.ConnID,
		Stream:     src.Stream,
		TRType:     src.TRType,
		TRCode:     src.TRCode,
		TRKey:      src.TRKey,
		RspCd:      src.RspCd,
		RspMsg:     src.RspMsg,
		MsgCd:      src.MsgCd,
		Msg:        src.Msg,
		Success:    src.Success,
		Attempt:    src.Attempt,
		Err:        src.Err,
		Size:       src.Size,
		Payload:    src.Payload,
		ReceivedAt: src.ReceivedAt,
		At:         src.At,
	}
}
