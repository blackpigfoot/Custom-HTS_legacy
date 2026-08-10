package client

import (
	"context"
	"sync"
	"time"

	rootls "Custom-HTS/internal/adapter/broker/ls"
	lsapi "Custom-HTS/internal/adapter/broker/ls/api"
	"Custom-HTS/internal/core/pkg/wsrefact_v6"
)

type Config = rootls.Config
type RealtimeEnvelope = lsapi.RealtimeEnvelope
type RealtimeHeader = lsapi.RealtimeHeader
type RealtimeResponse[T any] = lsapi.RealtimeResponse[T]
type TradeBody = lsapi.TradeBody
type QuoteBody = lsapi.QuoteBody
type ExecutionBody = lsapi.ExecutionBody
type TradeMessage = lsapi.TradeMessage
type QuoteMessage = lsapi.QuoteMessage
type ExecutionMessage = lsapi.ExecutionMessage
type MissingValueError = lsapi.MissingValueError
type CodeLimitError = lsapi.CodeLimitError
type InvalidIssueCodeError = lsapi.InvalidIssueCodeError
type DecodePathError = lsapi.DecodePathError
type FieldParseError = lsapi.FieldParseError
type LSError = lsapi.LSError
type OperationError = lsapi.OperationError
type DecimalValue = lsapi.DecimalValue
type T1102Response = lsapi.T1102Response
type T1101Response = lsapi.T1101Response
type T8407Response = lsapi.T8407Response
type T0424Response = lsapi.T0424Response
type CSPAQ12300Response = lsapi.CSPAQ12300Response
type REST = lsapi.REST
type Realtime = lsapi.Realtime
type WS = lsapi.WS

var ErrMissingValue = lsapi.ErrMissingValue
var ErrT8407CodesRequired = lsapi.ErrT8407CodesRequired
var ErrTooManyCodes = lsapi.ErrTooManyCodes
var ErrInvalidIssueCode = lsapi.ErrInvalidIssueCode
var ErrInvalidDecimalScale = lsapi.ErrInvalidDecimalScale
var ErrWSConnectionLimit = lsapi.ErrWSConnectionLimit
var ErrRealtimeClosed = lsapi.ErrRealtimeClosed

const BrokerName = rootls.BrokerName
const RealtimeTRTrade = lsapi.RealtimeTRTrade
const RealtimeTRQuote = lsapi.RealtimeTRQuote
const RealtimeTRExecution = lsapi.RealtimeTRExecution
const PathStockMarket = lsapi.PathStockMarket
const PathStockAccount = lsapi.PathStockAccount
const TrIDPrice = lsapi.TrIDPrice
const TrIDOrderbook = lsapi.TrIDOrderbook
const TrIDMultiPrice = lsapi.TrIDMultiPrice
const TrIDBalanceT0424 = lsapi.TrIDBalanceT0424
const TrIDBalanceCSPAQ = lsapi.TrIDBalanceCSPAQ
const RateScale = lsapi.RateScale

const defaultClientEventBuf = 256

// API is the higher-level LS client built on top of the low-level LS API
// package.
//
// API keeps the existing per-subscription local handle model. It owns one
// private low-level API instance, subscribes that API to vendor routes on
// demand, and fans vendor packets back out to per-caller channels by TR key.
type API struct {
	// api is the private low-level LS API instance owned by this wrapper.
	api *lsapi.API
	// subs keeps the local per-subscription route registry used by the wrapper.
	subs *subscriptionRegistry
	// eventCh reports websocket events and client-local fan-out failures.
	eventCh chan Event
	// dispatchOnce makes the wrapper dispatch loops single-owner.
	dispatchOnce sync.Once
}

// Client keeps the previous high-level client name as a compatibility alias.
type Client = API

// New creates a high-level LS client wrapper.
//
// The returned client keeps the package's existing per-caller subscription
// handle API while internally delegating low-level work to the combined API
// layer.
func New(cfg Config) (*API, error) {
	api, err := lsapi.New(cfg)
	if err != nil {
		return nil, err
	}

	client := &API{
		api:     api,
		eventCh: make(chan Event, defaultClientEventBuf),
	}
	client.subs = newSubscriptionRegistry(client.emitEvent)
	return client, nil
}

// Start launches the client fan-out loops and the underlying websocket service.
func (b *API) Start(ctx context.Context) {
	b.dispatchOnce.Do(func() {
		go b.runTradeDispatch(ctx)
		go b.runQuoteDispatch(ctx)
		go b.runExecutionDispatch(ctx)
		go b.runEventDispatch(ctx)
	})
	b.api.Start(ctx)
}

// API returns the private low-level LS API client used by the wrapper.
func (b *API) API() *lsapi.API {
	return b.api
}

// WSClient returns the wsrefact_v6 websocket client owned by this higher-level wrapper.
func (b *API) WSClient() *wsrefact_v6.Client {
	if b == nil || b.api == nil {
		return nil
	}
	return b.api.WSClient()
}

// Events returns asynchronous websocket and client fan-out events.
func (b *API) Events() <-chan Event {
	return b.eventCh
}

func (b *API) emitEvent(event Event) {
	if b.eventCh == nil {
		panic("ls client event channel is nil")
	}
	if event.Layer == "" {
		event.Layer = "client"
	}
	if event.At.IsZero() {
		event.At = time.Now()
	}
	select {
	case b.eventCh <- event:
	default:
	}
}

func (b *API) runEventDispatch(ctx context.Context) {
	eventCh := b.api.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-eventCh:
			if !ok {
				return
			}
			b.emitEvent(eventFromWS(event))
		}
	}
}

func (b *API) runTradeDispatch(ctx context.Context) {
	tradeCh := b.api.Trades()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-tradeCh:
			if !ok {
				return
			}
			b.subs.publishTrade(msg)
		}
	}
}

func (b *API) runQuoteDispatch(ctx context.Context) {
	quoteCh := b.api.Quotes()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-quoteCh:
			if !ok {
				return
			}
			b.subs.publishQuote(msg)
		}
	}
}

func (b *API) runExecutionDispatch(ctx context.Context) {
	executionCh := b.api.Executions()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-executionCh:
			if !ok {
				return
			}
			b.subs.publishExecution(msg)
		}
	}
}
