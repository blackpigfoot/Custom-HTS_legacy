package api

import (
	"time"

	rootls "Custom-HTS/internal/adapter/broker/ls"
	authsvc "Custom-HTS/internal/adapter/broker/ls/api/auth"
	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
	restsvc "Custom-HTS/internal/adapter/broker/ls/api/rest"
	wssvc "Custom-HTS/internal/adapter/broker/ls/api/ws"
	"Custom-HTS/internal/core/pkg/requester"
	corews "Custom-HTS/internal/core/pkg/websocket"
	"Custom-HTS/internal/core/pkg/wsrefact_v6"
)

// Config is the root LS API configuration.
type Config = rootls.Config

// Auth is the LS OAuth token service.
type Auth = authsvc.Auth

// AuthConfig configures the LS OAuth token service.
type AuthConfig = authsvc.Config

// REST is the low-level LS REST service.
type REST = restsvc.REST

// RESTDependencies contains dependencies for the low-level LS REST service.
type RESTDependencies = restsvc.Dependencies

// Realtime is the low-level LS realtime websocket service.
type Realtime = wssvc.Realtime

// WS keeps the previous websocket service name as a compatibility alias.
type WS = wssvc.WS

// WSDependencies contains dependencies for the low-level LS realtime websocket service.
type WSDependencies = wssvc.Dependencies

// RealtimeEnvelope is the first-pass LS realtime JSON envelope.
type RealtimeEnvelope = wssvc.RealtimeEnvelope

// RealtimeHeader is the common LS realtime header DTO.
type RealtimeHeader = wssvc.RealtimeHeader

// RealtimeResponse is the reusable LS realtime JSON envelope.
type RealtimeResponse[T any] = wssvc.RealtimeResponse[T]

// TradeBody is the native LS trade-tick body DTO.
type TradeBody = wssvc.TradeBody

// QuoteBody is the native LS orderbook body DTO.
type QuoteBody = wssvc.QuoteBody

// ExecutionBody is the native LS execution body DTO.
type ExecutionBody = wssvc.ExecutionBody

// TradeMessage is the native LS trade stream message.
type TradeMessage = wssvc.TradeMessage

// QuoteMessage is the native LS quote stream message.
type QuoteMessage = wssvc.QuoteMessage

// ExecutionMessage is the native LS execution stream message.
type ExecutionMessage = wssvc.ExecutionMessage

// EventKind identifies an asynchronous LS realtime websocket event.
type EventKind = wssvc.EventKind

// Event reports asynchronous LS realtime websocket events.
type Event = wssvc.Event

// DecimalValue is the LS decimal string helper type.
type DecimalValue = restsvc.DecimalValue

// T1102Response is the native LS t1102 response DTO.
type T1102Response = restsvc.T1102Response

// T1101Response is the native LS t1101 response DTO.
type T1101Response = restsvc.T1101Response

// T8407Response is the native LS t8407 response DTO.
type T8407Response = restsvc.T8407Response

// T0424Response is the native LS t0424 response DTO.
type T0424Response = restsvc.T0424Response

// CSPAQ12300Response is the native LS CSPAQ12300 response DTO.
type CSPAQ12300Response = restsvc.CSPAQ12300Response

// MissingValueError adds field context to ErrMissingValue.
type MissingValueError = apierr.MissingValueError

// InvalidIssueCodeError reports an issue code normalization failure.
type InvalidIssueCodeError = apierr.InvalidIssueCodeError

// DecodePathError reports a logical JSON path decode failure.
type DecodePathError = apierr.DecodePathError

// FieldParseError reports a field-level parse failure.
type FieldParseError = apierr.FieldParseError

// LSError is the common LS REST/WebSocket business error shape.
type LSError = apierr.LSError

// OperationError wraps a lower-level error with an LS operation label.
type OperationError = apierr.OperationError

// CodeLimitError reports that a request exceeded the TR-specific code limit.
type CodeLimitError = restsvc.CodeLimitError

const (
	// BrokerName is the logical LS broker identifier exposed to callers.
	BrokerName = rootls.BrokerName

	// RealtimeTRTrade is the LS realtime TR code for domestic-stock trade ticks.
	RealtimeTRTrade = wssvc.RealtimeTRTrade
	// RealtimeTRQuote is the LS realtime TR code for domestic-stock orderbook updates.
	RealtimeTRQuote = wssvc.RealtimeTRQuote
	// RealtimeTRExecution is the LS realtime TR code for domestic-stock executions.
	RealtimeTRExecution = wssvc.RealtimeTRExecution

	// EventWSConnected reports that the websocket transport connected.
	EventWSConnected = wssvc.EventWSConnected
	// EventWSReconnected reports that the websocket transport reconnected.
	EventWSReconnected = wssvc.EventWSReconnected
	// EventWSDisconnected reports that the websocket transport disconnected.
	EventWSDisconnected = wssvc.EventWSDisconnected
	// EventWSReconnecting reports that the websocket transport is reconnecting.
	EventWSReconnecting = wssvc.EventWSReconnecting
	// EventWSFatal reports that the websocket transport reached a terminal failure.
	EventWSFatal = wssvc.EventWSFatal
	// EventControlResponse reports a websocket control response.
	EventControlResponse = wssvc.EventControlResponse
	// EventJSONDecodeFailed reports a JSON envelope decode failure.
	EventJSONDecodeFailed = wssvc.EventJSONDecodeFailed
	// EventRealtimeBodyDecodeFailed reports a typed realtime body decode failure.
	EventRealtimeBodyDecodeFailed = wssvc.EventRealtimeBodyDecodeFailed
	// EventMissingRealtimeTR reports a realtime packet without a TR code.
	EventMissingRealtimeTR = wssvc.EventMissingRealtimeTR
	// EventUnknownRealtimeTR reports a realtime packet with no registered stream.
	EventUnknownRealtimeTR = wssvc.EventUnknownRealtimeTR
	// EventDriverChannelFull keeps the previous channel-full event name as a compatibility alias.
	EventDriverChannelFull = wssvc.EventDriverChannelFull
	// EventWSChannelFull reports that a low-level websocket stream channel was full.
	EventWSChannelFull = wssvc.EventWSChannelFull
	// EventRawRealtimeIgnored reports a non-JSON realtime payload ignored by the websocket service.
	EventRawRealtimeIgnored = wssvc.EventRawRealtimeIgnored

	// PathStockMarket is the REST path for market-data TRs.
	PathStockMarket = restsvc.PathStockMarket
	// PathStockAccount is the REST path for account TRs.
	PathStockAccount = restsvc.PathStockAccount
	// TrIDPrice is the LS TR code for the single-price lookup.
	TrIDPrice = restsvc.TrIDPrice
	// TrIDOrderbook is the LS TR code for the orderbook lookup.
	TrIDOrderbook = restsvc.TrIDOrderbook
	// TrIDMultiPrice is the LS TR code for the multi-price lookup.
	TrIDMultiPrice = restsvc.TrIDMultiPrice
	// TrIDBalanceT0424 is the LS TR code for the stock balance lookup.
	TrIDBalanceT0424 = restsvc.TrIDBalanceT0424
	// TrIDBalanceCSPAQ is the LS TR code for the account asset lookup.
	TrIDBalanceCSPAQ = restsvc.TrIDBalanceCSPAQ
	// RateScale is the default fixed-point scale used by decimal parsing.
	RateScale = restsvc.RateScale
)

var (
	// ErrMissingValue reports that a required value was empty.
	ErrMissingValue = apierr.ErrMissingValue
	// ErrInvalidIssueCode reports that a stock code could not be normalized.
	ErrInvalidIssueCode = apierr.ErrInvalidIssueCode
	// ErrNilRequester reports that an API service was built without a requester.
	ErrNilRequester = apierr.ErrNilRequester
	// ErrNilAuth reports that an API service was built without auth.
	ErrNilAuth = apierr.ErrNilAuth
	// ErrNilConnection reports that the websocket service was built without a transport.
	ErrNilConnection = wssvc.ErrNilConnection
	// ErrWSConnectionLimit reports that every websocket connection slot is full.
	ErrWSConnectionLimit = wssvc.ErrWSConnectionLimit
	// ErrRealtimeClosed reports that the realtime websocket service lifecycle has ended.
	ErrRealtimeClosed = wssvc.ErrRealtimeClosed
	// ErrT8407CodesRequired reports that t8407 was called without any codes.
	ErrT8407CodesRequired = restsvc.ErrT8407CodesRequired
	// ErrTooManyCodes reports that the caller provided more codes than a TR allows.
	ErrTooManyCodes = restsvc.ErrTooManyCodes
	// ErrInvalidDecimalScale reports that a decimal scale must be a positive power of 10.
	ErrInvalidDecimalScale = restsvc.ErrInvalidDecimalScale
)

// API is the low-level combined LS API service.
//
// API assembles the shared LS session primitives once and embeds both the
// low-level REST service and the low-level realtime websocket service. It does
// not add local subscription fan-out or per-caller channel ownership.
type API struct {
	// REST exposes low-level LS REST operations.
	*REST
	// Realtime exposes low-level LS realtime websocket operations.
	*Realtime
	// auth is the shared OAuth token service used by the embedded services.
	auth *Auth
}

// Client keeps the previous combined API name as a compatibility alias.
type Client = API

// New creates a combined low-level LS API service.
func New(cfg Config) (*API, error) {
	req, err := requester.New(nil)
	if err != nil {
		return nil, err
	}

	authService, err := authsvc.New(authsvc.Config{
		Requester: req,
		RestURL:   rootls.BaseURLDefault,
		AppKey:    cfg.AppKey,
		AppSecret: cfg.AppSecret,
	})
	if err != nil {
		return nil, err
	}

	realtimeService, err := wssvc.New(wssvc.Dependencies{
		Auth: authService,
		ConnConfig: corews.WsConfig{
			URL:          rootls.ResolveWSURL(cfg.IsPaper),
			ReconnectMin: 1 * time.Second,
			ReconnectMax: 60 * time.Second,
		},
	})
	if err != nil {
		return nil, err
	}

	restService, err := restsvc.New(restsvc.Dependencies{
		Requester: req,
		Auth:      authService,
		RestURL:   rootls.BaseURLDefault,
		AccountNo: cfg.AccountNo,
	})
	if err != nil {
		return nil, err
	}

	return &API{
		REST:     restService,
		Realtime: realtimeService,
		auth:     authService,
	}, nil
}

// WSClient returns the low-level wsrefact_v6 websocket client owned by this API service.
func (a *API) WSClient() *wsrefact_v6.Client {
	if a == nil || a.Realtime == nil {
		return nil
	}
	return a.Realtime.Conn()
}
