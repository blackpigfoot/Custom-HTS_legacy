package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
	"Custom-HTS/internal/core/pkg/wsrefact_v6"
)

const (
	wsTypeRegisterAccount    = "1"
	wsTypeUnregisterAccount  = "2"
	wsTypeRegisterRealtime   = "3"
	wsTypeUnregisterRealtime = "4"
)

// wsRequest is the native LS websocket control request DTO.
type wsRequest struct {
	Header wsRequestHeader `json:"header"` // Header contains token and action metadata.
	Body   wsRequestBody   `json:"body"`   // Body contains the vendor route being changed.
}

// wsRequestHeader is the native LS websocket control request header DTO.
type wsRequestHeader struct {
	Token  string `json:"token"`   // Token is the OAuth bearer token without the Bearer prefix.
	TRType string `json:"tr_type"` // TRType is the vendor action code for register or unregister.
}

// wsRequestBody is the native LS websocket control request body DTO.
type wsRequestBody struct {
	TRCd  string `json:"tr_cd"`  // TRCd is the vendor realtime TR code.
	TRKey string `json:"tr_key"` // TRKey is the vendor realtime route key.
}

// handleTransportMessage receives one websocket payload from the reconnecting
// transport and forwards it into the LS-specific control or realtime parsers.
func (svc *Realtime) handleTransportMessage(connID string, data []byte) {
	if len(data) == 0 {
		return
	}
	// receivedAt records the earliest local timestamp after the transport hands the payload to the service.
	receivedAt := time.Now()
	if data[0] != '{' {
		svc.handleRawRealtimeMessage(connID, data, receivedAt)
		return
	}

	svc.handleJSONMessage(connID, data, receivedAt)
}

// handleRawRealtimeMessage keeps a placeholder hook for non-JSON realtime
// packets. The current LS stock integration only routes JSON payloads.
func (svc *Realtime) handleRawRealtimeMessage(connID string, data []byte, receivedAt time.Time) {
	svc.emitEvent(Event{
		Kind:       EventRawRealtimeIgnored,
		ConnID:     connID,
		Size:       len(data),
		ReceivedAt: receivedAt,
	})
}

// SubscribeTrade records one low-level trade-route intent for the normalized
// issue code.
func (svc *Realtime) SubscribeTrade(ctx context.Context, code string) error {
	trKey, err := normalizeRealtimeCode(RealtimeTRTrade, code)
	if err != nil {
		return err
	}
	return svc.subscribeTradeRoute(ctx, trKey)
}

// SubscribeQuote records one low-level quote-route intent for the normalized
// issue code.
func (svc *Realtime) SubscribeQuote(ctx context.Context, code string) error {
	trKey, err := normalizeRealtimeCode(RealtimeTRQuote, code)
	if err != nil {
		return err
	}
	return svc.subscribeQuoteRoute(ctx, trKey)
}

// SubscribeExecution records one low-level execution-route intent for the HTS
// identifier.
func (svc *Realtime) SubscribeExecution(ctx context.Context, htsID string) error {
	trKey, err := normalizeExecutionKey(htsID)
	if err != nil {
		return err
	}
	return svc.subscribeExecutionRoute(ctx, trKey)
}

// UnsubscribeTrade removes one low-level trade-route intent for the normalized
// issue code.
func (svc *Realtime) UnsubscribeTrade(ctx context.Context, code string) error {
	trKey, err := normalizeRealtimeCode(RealtimeTRTrade, code)
	if err != nil {
		return err
	}
	return svc.unsubscribeTradeRoute(ctx, trKey)
}

// UnsubscribeQuote removes one low-level quote-route intent for the normalized
// issue code.
func (svc *Realtime) UnsubscribeQuote(ctx context.Context, code string) error {
	trKey, err := normalizeRealtimeCode(RealtimeTRQuote, code)
	if err != nil {
		return err
	}
	return svc.unsubscribeQuoteRoute(ctx, trKey)
}

// UnsubscribeExecution removes one low-level execution-route intent for the HTS
// identifier.
func (svc *Realtime) UnsubscribeExecution(ctx context.Context, htsID string) error {
	trKey, err := normalizeExecutionKey(htsID)
	if err != nil {
		return err
	}
	return svc.unsubscribeExecutionRoute(ctx, trKey)
}

func (svc *Realtime) subscribeTradeRoute(ctx context.Context, trKey string) error {
	return svc.subscribeRealtimeTransport(ctx, RealtimeTRTrade, trKey)
}

func (svc *Realtime) subscribeQuoteRoute(ctx context.Context, trKey string) error {
	return svc.subscribeRealtimeTransport(ctx, RealtimeTRQuote, trKey)
}

func (svc *Realtime) subscribeExecutionRoute(ctx context.Context, trKey string) error {
	return svc.subscribeAccountTransport(ctx, RealtimeTRExecution, trKey)
}

func (svc *Realtime) unsubscribeTradeRoute(ctx context.Context, trKey string) error {
	return svc.unsubscribeRealtimeTransport(ctx, RealtimeTRTrade, trKey)
}

func (svc *Realtime) unsubscribeQuoteRoute(ctx context.Context, trKey string) error {
	return svc.unsubscribeRealtimeTransport(ctx, RealtimeTRQuote, trKey)
}

func (svc *Realtime) unsubscribeExecutionRoute(ctx context.Context, trKey string) error {
	return svc.unsubscribeAccountTransport(ctx, RealtimeTRExecution, trKey)
}

func (svc *Realtime) subscribeRealtimeTransport(ctx context.Context, trCode, trKey string) error {
	return svc.subscribeTransport(ctx, "rt", trCode, trKey, wsTypeRegisterRealtime)
}

func (svc *Realtime) unsubscribeRealtimeTransport(ctx context.Context, trCode, trKey string) error {
	return svc.subscribeTransport(ctx, "rt", trCode, trKey, wsTypeUnregisterRealtime)
}

func (svc *Realtime) subscribeAccountTransport(ctx context.Context, trCode, trKey string) error {
	return svc.subscribeTransport(ctx, "acct", trCode, trKey, wsTypeRegisterAccount)
}

func (svc *Realtime) unsubscribeAccountTransport(ctx context.Context, trCode, trKey string) error {
	return svc.subscribeTransport(ctx, "acct", trCode, trKey, wsTypeUnregisterAccount)
}

func (svc *Realtime) subscribeTransport(ctx context.Context, scope, trCode, trKey, trType string) error {
	ctx = normalizeContext(ctx)

	// key stores the stable logical route identifier shared across slot placement and wsrefact_v6 replay.
	key := buildTransportKey(scope, trCode, trKey)
	if trType == wsTypeUnregisterRealtime || trType == wsTypeUnregisterAccount {
		slot, ok, err := svc.registry.owner(key)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := slot.unsubscribe(ctx, key); err != nil {
			return err
		}
		svc.registry.release(key, slot)
		return nil
	}

	result, err := svc.registry.acquire(key)
	if err != nil {
		return err
	}
	if err := result.slot.subscribe(ctx, key); err != nil {
		if !result.existed {
			svc.registry.rollback(key, result.slot)
		}
		return err
	}
	return nil
}

func buildWSPayload(token, trType, trCode, trKey string) ([]byte, error) {
	return json.Marshal(wsRequest{
		Header: wsRequestHeader{
			Token:  token,
			TRType: trType,
		},
		Body: wsRequestBody{
			TRCd:  trCode,
			TRKey: trKey,
		},
	})
}

func buildTransportKey(scope, trCode, trKey string) string {
	return scope + ":" + trCode + ":" + trKey
}

// transportRouteKey stores the decoded logical mux key components used by the slot encoder.
type transportRouteKey struct {
	// scope identifies whether the route is account-scoped or market-data scoped.
	scope string
	// trCode stores the vendor-native realtime TR code.
	trCode string
	// trKey stores the vendor-native realtime routing key.
	trKey string
}

func parseTransportKey(key string) (transportRouteKey, error) {
	// first stores everything after the scope separator.
	first := strings.IndexByte(key, ':')
	if first <= 0 {
		return transportRouteKey{}, fmt.Errorf("ls websocket route key is invalid: %q", key)
	}
	// second stores the separator between the TR code and the TR key.
	second := strings.IndexByte(key[first+1:], ':')
	if second < 0 {
		return transportRouteKey{}, fmt.Errorf("ls websocket route key is invalid: %q", key)
	}
	second += first + 1
	// route stores the decoded logical route identifier returned to the slot encoder.
	route := transportRouteKey{
		scope:  key[:first],
		trCode: key[first+1 : second],
		trKey:  key[second+1:],
	}
	if route.scope == "" || route.trCode == "" || route.trKey == "" {
		return transportRouteKey{}, fmt.Errorf("ls websocket route key is invalid: %q", key)
	}
	return route, nil
}

func resolveTRType(scope string, action wsrefact_v6.WriteIntentAction) (string, error) {
	switch {
	case scope == "rt" && action == wsrefact_v6.WriteIntentActionSubscribe:
		return wsTypeRegisterRealtime, nil
	case scope == "rt" && action == wsrefact_v6.WriteIntentActionUnsubscribe:
		return wsTypeUnregisterRealtime, nil
	case scope == "acct" && action == wsrefact_v6.WriteIntentActionSubscribe:
		return wsTypeRegisterAccount, nil
	case scope == "acct" && action == wsrefact_v6.WriteIntentActionUnsubscribe:
		return wsTypeUnregisterAccount, nil
	default:
		return "", fmt.Errorf("ls websocket scope/action pair is unsupported: %s/%s", scope, action)
	}
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func normalizeRealtimeCode(trCode, code string) (string, error) {
	normalized, ok := normalizeWSIssueCode(code)
	if !ok {
		return "", &apierr.InvalidIssueCodeError{
			TRCode: trCode,
			Value:  code,
		}
	}
	return normalized, nil
}

func normalizeExecutionKey(htsID string) (string, error) {
	key := strings.TrimSpace(htsID)
	if key == "" {
		return "", &apierr.MissingValueError{Field: "hts_id"}
	}
	return key, nil
}

func normalizeWSIssueCode(code string) (string, bool) {
	code = strings.TrimSpace(code)
	if code == "" {
		return "", false
	}
	if after, ok := strings.CutPrefix(code, "A"); ok {
		code = after
	}
	if len(code) != 6 {
		return "", false
	}
	for i := 0; i < len(code); i++ {
		if code[i] < '0' || code[i] > '9' {
			return "", false
		}
	}
	return code, true
}
