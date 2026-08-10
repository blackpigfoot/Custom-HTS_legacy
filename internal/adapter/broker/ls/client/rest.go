package client

import "context"

// GetPrice returns the native LS t1102 response for a single issue code.
func (b *API) GetPrice(ctx context.Context, code string) (*T1102Response, error) {
	return b.api.GetPrice(ctx, code)
}

// GetOrderbook returns the native LS t1101 response for a single issue code.
func (b *API) GetOrderbook(ctx context.Context, code string) (*T1101Response, error) {
	return b.api.GetOrderbook(ctx, code)
}

// GetMultiPrices returns the native LS t8407 multi-price response.
func (b *API) GetMultiPrices(ctx context.Context, codes ...string) (*T8407Response, error) {
	return b.api.GetMultiPrices(ctx, codes...)
}

// GetBalance returns the native LS t0424 account balance response.
func (b *API) GetBalance(ctx context.Context) (*T0424Response, error) {
	return b.api.GetBalance(ctx)
}
