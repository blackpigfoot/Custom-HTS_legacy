package client

import "context"

// SubscribeTrade attaches one local consumer to the package-default trade
// stream for the normalized stock code.
func (b *API) SubscribeTrade(ctx context.Context, code string) (*TradeSubscription, error) {
	trKey, err := normalizeRealtimeCode(RealtimeTRTrade, code)
	if err != nil {
		return nil, err
	}

	subscriberID, ch, err := b.subs.acquireTrade(
		normalizeContext(ctx),
		RealtimeTRTrade,
		trKey,
		func(ctx context.Context, trKey string) error {
			return b.api.SubscribeTrade(ctx, trKey)
		},
	)
	if err != nil {
		return nil, err
	}

	return &TradeSubscription{
		client:       b,
		trCode:       RealtimeTRTrade,
		trKey:        trKey,
		subscriberID: subscriberID,
		ch:           ch,
	}, nil
}

// SubscribeQuote attaches one local consumer to the package-default quote
// stream for the normalized stock code.
func (b *API) SubscribeQuote(ctx context.Context, code string) (*QuoteSubscription, error) {
	trKey, err := normalizeRealtimeCode(RealtimeTRQuote, code)
	if err != nil {
		return nil, err
	}

	subscriberID, ch, err := b.subs.acquireQuote(
		normalizeContext(ctx),
		RealtimeTRQuote,
		trKey,
		func(ctx context.Context, trKey string) error {
			return b.api.SubscribeQuote(ctx, trKey)
		},
	)
	if err != nil {
		return nil, err
	}

	return &QuoteSubscription{
		client:       b,
		trCode:       RealtimeTRQuote,
		trKey:        trKey,
		subscriberID: subscriberID,
		ch:           ch,
	}, nil
}

// SubscribeExecution attaches one local consumer to the package-default
// account-scoped execution stream for the provided HTS identifier.
func (b *API) SubscribeExecution(ctx context.Context, htsID string) (*ExecutionSubscription, error) {
	trKey, err := normalizeExecutionKey(htsID)
	if err != nil {
		return nil, err
	}

	subscriberID, ch, err := b.subs.acquireExecution(
		normalizeContext(ctx),
		RealtimeTRExecution,
		trKey,
		func(ctx context.Context, trKey string) error {
			return b.api.SubscribeExecution(ctx, trKey)
		},
	)
	if err != nil {
		return nil, err
	}

	return &ExecutionSubscription{
		client:       b,
		trCode:       RealtimeTRExecution,
		trKey:        trKey,
		subscriberID: subscriberID,
		ch:           ch,
	}, nil
}

// UnsubscribeTrade forcibly tears down the entire trade route for the
// normalized stock code.
func (b *API) UnsubscribeTrade(ctx context.Context, code string) error {
	trKey, err := normalizeRealtimeCode(RealtimeTRTrade, code)
	if err != nil {
		return err
	}

	return b.subs.closeTradeRoute(
		normalizeContext(ctx),
		RealtimeTRTrade,
		trKey,
		func(ctx context.Context, trKey string) error {
			return b.api.UnsubscribeTrade(ctx, trKey)
		},
	)
}

// UnsubscribeQuote forcibly tears down the entire quote route for the
// normalized stock code.
func (b *API) UnsubscribeQuote(ctx context.Context, code string) error {
	trKey, err := normalizeRealtimeCode(RealtimeTRQuote, code)
	if err != nil {
		return err
	}

	return b.subs.closeQuoteRoute(
		normalizeContext(ctx),
		RealtimeTRQuote,
		trKey,
		func(ctx context.Context, trKey string) error {
			return b.api.UnsubscribeQuote(ctx, trKey)
		},
	)
}

// UnsubscribeExecution forcibly tears down the entire execution route for the
// provided HTS identifier.
func (b *API) UnsubscribeExecution(ctx context.Context, htsID string) error {
	trKey, err := normalizeExecutionKey(htsID)
	if err != nil {
		return err
	}

	return b.subs.closeExecutionRoute(
		normalizeContext(ctx),
		RealtimeTRExecution,
		trKey,
		func(ctx context.Context, trKey string) error {
			return b.api.UnsubscribeExecution(ctx, trKey)
		},
	)
}

func (b *API) closeTradeSubscription(ctx context.Context, trCode, trKey string, subscriberID uint64) error {
	return b.subs.releaseTrade(ctx, trCode, trKey, subscriberID, func(ctx context.Context, trKey string) error {
		return b.api.UnsubscribeTrade(ctx, trKey)
	})
}

func (b *API) closeQuoteSubscription(ctx context.Context, trCode, trKey string, subscriberID uint64) error {
	return b.subs.releaseQuote(ctx, trCode, trKey, subscriberID, func(ctx context.Context, trKey string) error {
		return b.api.UnsubscribeQuote(ctx, trKey)
	})
}

func (b *API) closeExecutionSubscription(ctx context.Context, trCode, trKey string, subscriberID uint64) error {
	return b.subs.releaseExecution(ctx, trCode, trKey, subscriberID, func(ctx context.Context, trKey string) error {
		return b.api.UnsubscribeExecution(ctx, trKey)
	})
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
