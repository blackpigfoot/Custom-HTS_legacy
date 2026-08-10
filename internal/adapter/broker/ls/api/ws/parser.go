package ws

import (
	"encoding/json"
	"time"

	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
)

// wsJSONEnvelope is the first-pass websocket payload used for control and realtime routing.
type wsJSONEnvelope struct {
	Header wsCommonHeader  `json:"header"` // Header carries routing and response metadata.
	Body   json.RawMessage `json:"body"`   // Body is deferred until the packet route is known.
}

// wsCommonHeader is the superset header shape shared by LS websocket packets.
type wsCommonHeader struct {
	TRType string `json:"tr_type"` // TRType is the websocket register or unregister action code.
	TRCode string `json:"tr_cd"`   // TRCode is the vendor realtime TR code.
	TRKey  string `json:"tr_key"`  // TRKey is the vendor realtime route key.
	RspCd  string `json:"rsp_cd"`  // RspCd is the vendor control response code.
	RspMsg string `json:"rsp_msg"` // RspMsg is the vendor control response message.
	MsgCd  string `json:"msg_cd"`  // MsgCd is an optional vendor message code.
	Msg    string `json:"msg"`     // Msg is an optional vendor message text.
}

func (svc *Realtime) handleJSONMessage(connID string, data []byte, receivedAt time.Time) {
	envelope, err := parseWSJSONEnvelope(data)
	if err != nil {
		svc.emitEvent(Event{
			Kind:       EventJSONDecodeFailed,
			ConnID:     connID,
			Err:        err,
			Size:       len(data),
			Payload:    cloneRawMessage(data),
			ReceivedAt: receivedAt,
		})
		return
	}

	if isWSControlMessage(&envelope) {
		svc.handleWSControlMessage(connID, &envelope, receivedAt, len(data))
		return
	}

	svc.handleRealtimeJSONMessage(connID, envelope, receivedAt, len(data))
}

func (svc *Realtime) handleWSControlMessage(connID string, msg *wsJSONEnvelope, receivedAt time.Time, messageSize int) {
	if msg == nil {
		return
	}

	// success reports whether LS accepted the websocket control request.
	success := msg.Header.RspCd == "00000" || msg.Header.RspCd == "0000"
	// event carries the control response to the owner layer instead of logging in the websocket service.
	event := Event{
		Kind:       EventControlResponse,
		ConnID:     connID,
		TRType:     msg.Header.TRType,
		TRCode:     msg.Header.TRCode,
		TRKey:      msg.Header.TRKey,
		RspCd:      msg.Header.RspCd,
		RspMsg:     msg.Header.RspMsg,
		MsgCd:      msg.Header.MsgCd,
		Msg:        msg.Header.Msg,
		Success:    success,
		Size:       messageSize,
		Payload:    cloneRawMessage(msg.Body),
		ReceivedAt: receivedAt,
	}
	if !success {
		event.Err = &apierr.LSError{RspCd: msg.Header.RspCd, RspMsg: msg.Header.RspMsg}
	}
	svc.emitEvent(event)
}

func (svc *Realtime) handleRealtimeJSONMessage(connID string, decoded wsJSONEnvelope, receivedAt time.Time, messageSize int) {
	// envelope is the first-pass realtime view that keeps the raw body until route dispatch.
	envelope := RealtimeEnvelope{
		Header: RealtimeHeader{
			TRType: decoded.Header.TRType,
			TRCode: decoded.Header.TRCode,
			TRKey:  decoded.Header.TRKey,
		},
		Body: decoded.Body,
	}

	// header is the first-pass routing metadata from the native realtime envelope.
	header := envelope.Header
	if header.TRCode == "" {
		svc.emitEvent(Event{
			Kind:       EventMissingRealtimeTR,
			ConnID:     connID,
			TRType:     header.TRType,
			TRKey:      header.TRKey,
			Size:       messageSize,
			Payload:    cloneRawMessage(envelope.Body),
			ReceivedAt: receivedAt,
		})
		return
	}

	if svc.publishRealtime(connID, envelope, receivedAt, messageSize) {
		return
	}

	svc.emitEvent(Event{
		Kind:       EventUnknownRealtimeTR,
		ConnID:     connID,
		TRType:     header.TRType,
		TRCode:     header.TRCode,
		TRKey:      header.TRKey,
		Size:       messageSize,
		Payload:    cloneRawMessage(envelope.Body),
		ReceivedAt: receivedAt,
	})
}

func (svc *Realtime) publishRealtime(connID string, envelope RealtimeEnvelope, receivedAt time.Time, messageSize int) bool {
	if len(envelope.Body) == 0 {
		return false
	}

	// header is reused as the single response header after the body type is selected.
	header := envelope.Header
	switch header.TRCode {
	case RealtimeTRTrade:
		var body TradeBody
		if err := decodeRealtimeBody(envelope.Body, &body); err != nil {
			svc.emitRealtimeBodyDecodeFailed(connID, "trade", header, err, envelope.Body, receivedAt, messageSize)
			return true
		}
		// response keeps the final header/body DTO as the channel message source of truth.
		response := RealtimeResponse[TradeBody]{Header: header, Body: body}
		select {
		case svc.tradeCh <- TradeMessage{Response: response, ConnID: connID, ReceivedAt: receivedAt}:
		default:
			svc.emitWSChannelFull(connID, "trade", header, envelope.Body, receivedAt, messageSize)
		}
		return true
	case RealtimeTRQuote:
		var body QuoteBody
		if err := decodeRealtimeBody(envelope.Body, &body); err != nil {
			svc.emitRealtimeBodyDecodeFailed(connID, "quote", header, err, envelope.Body, receivedAt, messageSize)
			return true
		}
		// response keeps the final header/body DTO as the channel message source of truth.
		response := RealtimeResponse[QuoteBody]{Header: header, Body: body}
		select {
		case svc.quoteCh <- QuoteMessage{Response: response, ConnID: connID, ReceivedAt: receivedAt}:
		default:
			svc.emitWSChannelFull(connID, "quote", header, envelope.Body, receivedAt, messageSize)
		}
		return true
	case RealtimeTRExecution:
		var body ExecutionBody
		if err := decodeRealtimeBody(envelope.Body, &body); err != nil {
			svc.emitRealtimeBodyDecodeFailed(connID, "execution", header, err, envelope.Body, receivedAt, messageSize)
			return true
		}
		// response keeps the final header/body DTO as the channel message source of truth.
		response := RealtimeResponse[ExecutionBody]{Header: header, Body: body}
		select {
		case svc.executionCh <- ExecutionMessage{Response: response, ConnID: connID, ReceivedAt: receivedAt}:
		default:
			svc.emitWSChannelFull(connID, "execution", header, envelope.Body, receivedAt, messageSize)
		}
		return true
	default:
		return false
	}
}

func parseWSJSONEnvelope(data []byte) (wsJSONEnvelope, error) {
	var envelope wsJSONEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return wsJSONEnvelope{}, &apierr.DecodePathError{
			Path: "websocket",
			Err:  err,
		}
	}
	return envelope, nil
}

func isWSControlMessage(msg *wsJSONEnvelope) bool {
	if msg == nil {
		return false
	}
	return msg.Header.RspCd != "" || msg.Header.RspMsg != "" || msg.Header.MsgCd != "" || msg.Header.Msg != ""
}

func decodeRealtimeBody(data []byte, out any) error {
	if err := json.Unmarshal(data, out); err != nil {
		return &apierr.DecodePathError{
			Path: "body",
			Err:  err,
		}
	}
	return nil
}

func (svc *Realtime) emitRealtimeBodyDecodeFailed(connID, stream string, header RealtimeHeader, err error, payload json.RawMessage, receivedAt time.Time, size int) {
	svc.emitEvent(Event{
		Kind:       EventRealtimeBodyDecodeFailed,
		ConnID:     connID,
		Stream:     stream,
		TRType:     header.TRType,
		TRCode:     header.TRCode,
		TRKey:      header.TRKey,
		Err:        err,
		Size:       size,
		Payload:    cloneRawMessage(payload),
		ReceivedAt: receivedAt,
	})
}

func (svc *Realtime) emitWSChannelFull(connID, stream string, header RealtimeHeader, payload json.RawMessage, receivedAt time.Time, size int) {
	svc.emitEvent(Event{
		Kind:       EventWSChannelFull,
		ConnID:     connID,
		Stream:     stream,
		TRType:     header.TRType,
		TRCode:     header.TRCode,
		TRKey:      header.TRKey,
		Size:       size,
		Payload:    cloneRawMessage(payload),
		ReceivedAt: receivedAt,
	})
}

func cloneRawMessage(data []byte) json.RawMessage {
	if len(data) == 0 {
		return nil
	}
	// cloned keeps raw payload events valid after the synchronous transport handler returns.
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}
