package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

const defaultSubscriptionBuf = 64

// TradeSubscription represents one local consumer attached to a trade route.
//
// Multiple TradeSubscription values may point at the same remote LS route. The
// client deduplicates the underlying websocket subscription and fans each packet
// out to every local TradeSubscription attached to that route.
type TradeSubscription struct {
	// client owns the local route registry and the underlying realtime websocket service.
	client *API
	// trCode is the vendor-native realtime TR code used by this subscription.
	trCode string
	// trKey is the vendor-native realtime routing key used by this
	// subscription.
	trKey string
	// subscriberID uniquely identifies this local subscriber within the route.
	subscriberID uint64
	// ch delivers trade packets to this local subscriber only.
	ch <-chan TradeMessage
	// mu guards closed so Close becomes idempotent for callers.
	mu sync.Mutex
	// closed reports whether Close already completed successfully.
	closed bool
}

// Channel returns the receive-only channel dedicated to this local subscriber.
func (s *TradeSubscription) Channel() <-chan TradeMessage {
	if s == nil {
		return nil
	}
	return s.ch
}

// TRCode returns the vendor-native realtime TR code backing this subscription.
func (s *TradeSubscription) TRCode() string {
	if s == nil {
		return ""
	}
	return s.trCode
}

// TRKey returns the vendor-native realtime routing key backing this
// subscription.
func (s *TradeSubscription) TRKey() string {
	if s == nil {
		return ""
	}
	return s.trKey
}

// Close detaches this local subscriber from its route.
//
// When this is the last local subscriber on the route, Close also removes the
// underlying LS websocket subscription. Close is idempotent after the first
// successful call.
func (s *TradeSubscription) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	if err := s.client.closeTradeSubscription(normalizeContext(ctx), s.trCode, s.trKey, s.subscriberID); err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			s.closed = true
			return nil
		}
		return err
	}
	s.closed = true
	return nil
}

// QuoteSubscription represents one local consumer attached to a quote route.
type QuoteSubscription struct {
	// client owns the local route registry and the underlying realtime websocket service.
	client *API
	// trCode is the vendor-native realtime TR code used by this subscription.
	trCode string
	// trKey is the vendor-native realtime routing key used by this
	// subscription.
	trKey string
	// subscriberID uniquely identifies this local subscriber within the route.
	subscriberID uint64
	// ch delivers quote packets to this local subscriber only.
	ch <-chan QuoteMessage
	// mu guards closed so Close becomes idempotent for callers.
	mu sync.Mutex
	// closed reports whether Close already completed successfully.
	closed bool
}

// Channel returns the receive-only channel dedicated to this local subscriber.
func (s *QuoteSubscription) Channel() <-chan QuoteMessage {
	if s == nil {
		return nil
	}
	return s.ch
}

// TRCode returns the vendor-native realtime TR code backing this subscription.
func (s *QuoteSubscription) TRCode() string {
	if s == nil {
		return ""
	}
	return s.trCode
}

// TRKey returns the vendor-native realtime routing key backing this
// subscription.
func (s *QuoteSubscription) TRKey() string {
	if s == nil {
		return ""
	}
	return s.trKey
}

// Close detaches this local subscriber from its route.
//
// When this is the last local subscriber on the route, Close also removes the
// underlying LS websocket subscription. Close is idempotent after the first
// successful call.
func (s *QuoteSubscription) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	if err := s.client.closeQuoteSubscription(normalizeContext(ctx), s.trCode, s.trKey, s.subscriberID); err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			s.closed = true
			return nil
		}
		return err
	}
	s.closed = true
	return nil
}

// ExecutionSubscription represents one local consumer attached to an
// account-scoped execution route.
type ExecutionSubscription struct {
	// client owns the local route registry and the underlying realtime websocket service.
	client *API
	// trCode is the vendor-native realtime TR code used by this subscription.
	trCode string
	// trKey is the vendor-native realtime routing key used by this
	// subscription.
	trKey string
	// subscriberID uniquely identifies this local subscriber within the route.
	subscriberID uint64
	// ch delivers execution packets to this local subscriber only.
	ch <-chan ExecutionMessage
	// mu guards closed so Close becomes idempotent for callers.
	mu sync.Mutex
	// closed reports whether Close already completed successfully.
	closed bool
}

// Channel returns the receive-only channel dedicated to this local subscriber.
func (s *ExecutionSubscription) Channel() <-chan ExecutionMessage {
	if s == nil {
		return nil
	}
	return s.ch
}

// TRCode returns the vendor-native realtime TR code backing this subscription.
func (s *ExecutionSubscription) TRCode() string {
	if s == nil {
		return ""
	}
	return s.trCode
}

// TRKey returns the vendor-native realtime routing key backing this
// subscription.
func (s *ExecutionSubscription) TRKey() string {
	if s == nil {
		return ""
	}
	return s.trKey
}

// Close detaches this local subscriber from its route.
//
// When this is the last local subscriber on the route, Close also removes the
// underlying LS websocket subscription. Close is idempotent after the first
// successful call.
func (s *ExecutionSubscription) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	if err := s.client.closeExecutionSubscription(normalizeContext(ctx), s.trCode, s.trKey, s.subscriberID); err != nil {
		if errors.Is(err, ErrSubscriptionNotFound) {
			s.closed = true
			return nil
		}
		return err
	}
	s.closed = true
	return nil
}

type subscriptionRegistry struct {
	// mu guards every route mutation and keeps route teardown synchronized with
	// the publish hot path.
	mu sync.RWMutex
	// emit reports asynchronous client fan-out events to the owning client.
	emit func(Event)
	// nextSubscriberID is the monotonic identifier assigned to the next local
	// subscriber that gets attached to any route.
	nextSubscriberID uint64
	// tradeRoutes stores every active trade route keyed by TR code and TR key.
	tradeRoutes map[string]*tradeRoute
	// quoteRoutes stores every active quote route keyed by TR code and TR key.
	quoteRoutes map[string]*quoteRoute
	// executionRoutes stores every active execution route keyed by TR code and
	// TR key.
	executionRoutes map[string]*executionRoute
}

type tradeRoute struct {
	// trCode is the vendor-native realtime TR code shared by every subscriber in
	// this route.
	trCode string
	// trKey is the vendor-native realtime routing key shared by every subscriber
	// in this route.
	trKey string
	// subscribers maps local subscriber identifiers to dedicated trade channels.
	subscribers map[uint64]chan TradeMessage
}

type quoteRoute struct {
	// trCode is the vendor-native realtime TR code shared by every subscriber in
	// this route.
	trCode string
	// trKey is the vendor-native realtime routing key shared by every subscriber
	// in this route.
	trKey string
	// subscribers maps local subscriber identifiers to dedicated quote channels.
	subscribers map[uint64]chan QuoteMessage
}

type executionRoute struct {
	// trCode is the vendor-native realtime TR code shared by every subscriber in
	// this route.
	trCode string
	// trKey is the vendor-native realtime routing key shared by every subscriber
	// in this route.
	trKey string
	// subscribers maps local subscriber identifiers to dedicated execution
	// channels.
	subscribers map[uint64]chan ExecutionMessage
}

func newSubscriptionRegistry(emit func(Event)) *subscriptionRegistry {
	return &subscriptionRegistry{
		emit:            emit,
		tradeRoutes:     make(map[string]*tradeRoute),
		quoteRoutes:     make(map[string]*quoteRoute),
		executionRoutes: make(map[string]*executionRoute),
	}
}

func (r *subscriptionRegistry) acquireTrade(
	ctx context.Context,
	trCode string,
	trKey string,
	onFirst func(context.Context, string) error,
) (uint64, <-chan TradeMessage, error) {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.tradeRoutes[key]
	if !ok {
		route = &tradeRoute{
			trCode:      trCode,
			trKey:       trKey,
			subscribers: make(map[uint64]chan TradeMessage),
		}
		r.tradeRoutes[key] = route
	}

	r.nextSubscriberID++
	subscriberID := r.nextSubscriberID
	ch := make(chan TradeMessage, defaultSubscriptionBuf)
	route.subscribers[subscriberID] = ch
	if len(route.subscribers) == 1 {
		if err := onFirst(ctx, trKey); err != nil {
			delete(route.subscribers, subscriberID)
			delete(r.tradeRoutes, key)
			close(ch)
			return 0, nil, err
		}
	}

	return subscriberID, ch, nil
}

func (r *subscriptionRegistry) acquireQuote(
	ctx context.Context,
	trCode string,
	trKey string,
	onFirst func(context.Context, string) error,
) (uint64, <-chan QuoteMessage, error) {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.quoteRoutes[key]
	if !ok {
		route = &quoteRoute{
			trCode:      trCode,
			trKey:       trKey,
			subscribers: make(map[uint64]chan QuoteMessage),
		}
		r.quoteRoutes[key] = route
	}

	r.nextSubscriberID++
	subscriberID := r.nextSubscriberID
	ch := make(chan QuoteMessage, defaultSubscriptionBuf)
	route.subscribers[subscriberID] = ch
	if len(route.subscribers) == 1 {
		if err := onFirst(ctx, trKey); err != nil {
			delete(route.subscribers, subscriberID)
			delete(r.quoteRoutes, key)
			close(ch)
			return 0, nil, err
		}
	}

	return subscriberID, ch, nil
}

func (r *subscriptionRegistry) acquireExecution(
	ctx context.Context,
	trCode string,
	trKey string,
	onFirst func(context.Context, string) error,
) (uint64, <-chan ExecutionMessage, error) {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.executionRoutes[key]
	if !ok {
		route = &executionRoute{
			trCode:      trCode,
			trKey:       trKey,
			subscribers: make(map[uint64]chan ExecutionMessage),
		}
		r.executionRoutes[key] = route
	}

	r.nextSubscriberID++
	subscriberID := r.nextSubscriberID
	ch := make(chan ExecutionMessage, defaultSubscriptionBuf)
	route.subscribers[subscriberID] = ch
	if len(route.subscribers) == 1 {
		if err := onFirst(ctx, trKey); err != nil {
			delete(route.subscribers, subscriberID)
			delete(r.executionRoutes, key)
			close(ch)
			return 0, nil, err
		}
	}

	return subscriberID, ch, nil
}

func (r *subscriptionRegistry) releaseTrade(
	ctx context.Context,
	trCode string,
	trKey string,
	subscriberID uint64,
	onLast func(context.Context, string) error,
) error {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.tradeRoutes[key]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "trade",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}

	ch, ok := route.subscribers[subscriberID]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "trade",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}

	if len(route.subscribers) == 1 {
		if err := onLast(ctx, trKey); err != nil {
			return err
		}
	}

	delete(route.subscribers, subscriberID)
	if len(route.subscribers) == 0 {
		delete(r.tradeRoutes, key)
	}
	close(ch)
	return nil
}

func (r *subscriptionRegistry) releaseQuote(
	ctx context.Context,
	trCode string,
	trKey string,
	subscriberID uint64,
	onLast func(context.Context, string) error,
) error {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.quoteRoutes[key]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "quote",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}

	ch, ok := route.subscribers[subscriberID]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "quote",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}

	if len(route.subscribers) == 1 {
		if err := onLast(ctx, trKey); err != nil {
			return err
		}
	}

	delete(route.subscribers, subscriberID)
	if len(route.subscribers) == 0 {
		delete(r.quoteRoutes, key)
	}
	close(ch)
	return nil
}

func (r *subscriptionRegistry) releaseExecution(
	ctx context.Context,
	trCode string,
	trKey string,
	subscriberID uint64,
	onLast func(context.Context, string) error,
) error {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.executionRoutes[key]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "execution",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}

	ch, ok := route.subscribers[subscriberID]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "execution",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}

	if len(route.subscribers) == 1 {
		if err := onLast(ctx, trKey); err != nil {
			return err
		}
	}

	delete(route.subscribers, subscriberID)
	if len(route.subscribers) == 0 {
		delete(r.executionRoutes, key)
	}
	close(ch)
	return nil
}

func (r *subscriptionRegistry) closeTradeRoute(
	ctx context.Context,
	trCode string,
	trKey string,
	onClose func(context.Context, string) error,
) error {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.tradeRoutes[key]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "trade",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}
	if err := onClose(ctx, trKey); err != nil {
		return err
	}

	delete(r.tradeRoutes, key)
	for _, ch := range route.subscribers {
		close(ch)
	}
	return nil
}

func (r *subscriptionRegistry) closeQuoteRoute(
	ctx context.Context,
	trCode string,
	trKey string,
	onClose func(context.Context, string) error,
) error {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.quoteRoutes[key]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "quote",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}
	if err := onClose(ctx, trKey); err != nil {
		return err
	}

	delete(r.quoteRoutes, key)
	for _, ch := range route.subscribers {
		close(ch)
	}
	return nil
}

func (r *subscriptionRegistry) closeExecutionRoute(
	ctx context.Context,
	trCode string,
	trKey string,
	onClose func(context.Context, string) error,
) error {
	key := routeKey(trCode, trKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	route, ok := r.executionRoutes[key]
	if !ok {
		return &SubscriptionNotFoundError{
			Kind:   "execution",
			TRCode: trCode,
			TRKey:  trKey,
		}
	}
	if err := onClose(ctx, trKey); err != nil {
		return err
	}

	delete(r.executionRoutes, key)
	for _, ch := range route.subscribers {
		close(ch)
	}
	return nil
}

func (r *subscriptionRegistry) publishTrade(msg TradeMessage) bool {
	key := routeKey(msg.Response.Header.TRCode, msg.Response.Header.TRKey)

	r.mu.RLock()
	defer r.mu.RUnlock()

	route, ok := r.tradeRoutes[key]
	if !ok {
		return false
	}

	for _, ch := range route.subscribers {
		// out copies the immutable native DTO and the websocket receive timestamp for this subscriber.
		out := msg
		select {
		case ch <- out:
		default:
			r.emitSubscriberChannelFull("trade", route.trCode, route.trKey, msg.ConnID, msg.ReceivedAt)
		}
	}
	return true
}

func (r *subscriptionRegistry) publishQuote(msg QuoteMessage) bool {
	key := routeKey(msg.Response.Header.TRCode, msg.Response.Header.TRKey)

	r.mu.RLock()
	defer r.mu.RUnlock()

	route, ok := r.quoteRoutes[key]
	if !ok {
		return false
	}

	for _, ch := range route.subscribers {
		// out copies the immutable native DTO and the websocket receive timestamp for this subscriber.
		out := msg
		select {
		case ch <- out:
		default:
			r.emitSubscriberChannelFull("quote", route.trCode, route.trKey, msg.ConnID, msg.ReceivedAt)
		}
	}
	return true
}

func (r *subscriptionRegistry) publishExecution(msg ExecutionMessage) bool {
	key := routeKey(msg.Response.Header.TRCode, msg.Response.Header.TRKey)

	r.mu.RLock()
	defer r.mu.RUnlock()

	route, ok := r.executionRoutes[key]
	if !ok {
		return false
	}

	for _, ch := range route.subscribers {
		// out copies the immutable native DTO and the websocket receive timestamp for this subscriber.
		out := msg
		select {
		case ch <- out:
		default:
			r.emitSubscriberChannelFull("execution", route.trCode, route.trKey, msg.ConnID, msg.ReceivedAt)
		}
	}
	return true
}

func routeKey(trCode, trKey string) string {
	return strings.TrimSpace(trCode) + ":" + strings.TrimSpace(trKey)
}

func (r *subscriptionRegistry) emitSubscriberChannelFull(stream, trCode, trKey, connID string, receivedAt time.Time) {
	if r.emit == nil {
		panic("ls subscription event emitter is nil")
	}
	r.emit(Event{
		Kind:       EventSubscriberChannelFull,
		Layer:      "client",
		ConnID:     connID,
		Stream:     stream,
		TRCode:     trCode,
		TRKey:      trKey,
		ReceivedAt: receivedAt,
	})
}

func normalizeRealtimeCode(trCode, code string) (string, error) {
	normalized, ok := normalizeIssueCode(code)
	if !ok {
		return "", &InvalidIssueCodeError{
			TRCode: trCode,
			Value:  code,
		}
	}
	return normalized, nil
}

func normalizeExecutionKey(htsID string) (string, error) {
	key := strings.TrimSpace(htsID)
	if key == "" {
		return "", &MissingValueError{Field: "hts_id"}
	}
	return key, nil
}
