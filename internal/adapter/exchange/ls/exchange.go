package lsexchange

import (
	"context"
	"strings"
	"time"

	lsclient "Custom-HTS/internal/adapter/broker/ls/client"
	"Custom-HTS/internal/core/exchange"
	"Custom-HTS/internal/core/service/config"
)

// ExchangeAdapter projects the native LS broker into the current shared
// exchange.Exchange interface.
type ExchangeAdapter struct {
	exchange.Base
	Client *lsclient.Client
}

// Wrap adapts a native LS broker into the shared exchange.Exchange contract.
func Wrap(c *lsclient.Client) exchange.Exchange {
	return &ExchangeAdapter{
		Base: exchange.Base{
			Broker:    lsclient.BrokerName,
			AssetType: config.AssetStock,
		},
		Client: c,
	}
}

func (a *ExchangeAdapter) Start(ctx context.Context) {
	if a.Client != nil {
		a.Client.Start(ctx)
	}
}

func (a *ExchangeAdapter) GetPrice(ctx context.Context, code string) (*exchange.Price, error) {
	resp, err := a.Client.GetPrice(ctx, code)
	if err != nil {
		return nil, err
	}
	return buildExchangePriceFromT1102(resp, code, time.Now())
}

func (a *ExchangeAdapter) GetOrderbook(ctx context.Context, code string) (*exchange.Orderbook, error) {
	resp, err := a.Client.GetOrderbook(ctx, code)
	if err != nil {
		return nil, err
	}
	return buildExchangeOrderbookFromT1101(resp, code), nil
}

func (a *ExchangeAdapter) GetBalance(ctx context.Context) (*exchange.Balance, error) {
	resp, err := a.Client.GetBalance(ctx)
	if err != nil {
		return nil, err
	}
	return buildExchangeBalanceFromT0424(resp)
}

func buildExchangePriceFromT1102(resp *lsclient.T1102Response, fallbackCode string, ts time.Time) (*exchange.Price, error) {
	if resp == nil {
		return nil, nil
	}

	changePct, err := resp.OutBlock.Diff.Scaled(lsclient.RateScale)
	if err != nil {
		return nil, &lsclient.FieldParseError{
			Field: "t1102OutBlock.diff",
			Kind:  "scaled-int64",
			Value: resp.OutBlock.Diff.String(),
			Err:   err,
		}
	}

	return &exchange.Price{
		Code:      firstNonEmpty(strings.TrimSpace(resp.OutBlock.Shcode), strings.TrimSpace(fallbackCode)),
		Current:   resp.OutBlock.Price,
		Open:      resp.OutBlock.Open,
		High:      resp.OutBlock.High,
		Low:       resp.OutBlock.Low,
		PrevClose: resp.OutBlock.JnilClose,
		Volume:    resp.OutBlock.Volume,
		Change:    resp.OutBlock.Change,
		ChangePct: changePct,
		Timestamp: ts,
	}, nil
}

func buildExchangeOrderbookFromT1101(resp *lsclient.T1101Response, fallbackCode string) *exchange.Orderbook {
	if resp == nil {
		return nil
	}

	out := resp.OutBlock
	return &exchange.Orderbook{
		Code: firstNonEmpty(strings.TrimSpace(out.Shcode), strings.TrimSpace(fallbackCode)),
		Asks: []exchange.OrderbookItem{
			{Price: float64(out.OfferHo1), Volume: float64(out.OfferRem1)},
			{Price: float64(out.OfferHo2), Volume: float64(out.OfferRem2)},
			{Price: float64(out.OfferHo3), Volume: float64(out.OfferRem3)},
			{Price: float64(out.OfferHo4), Volume: float64(out.OfferRem4)},
			{Price: float64(out.OfferHo5), Volume: float64(out.OfferRem5)},
			{Price: float64(out.OfferHo6), Volume: float64(out.OfferRem6)},
			{Price: float64(out.OfferHo7), Volume: float64(out.OfferRem7)},
			{Price: float64(out.OfferHo8), Volume: float64(out.OfferRem8)},
			{Price: float64(out.OfferHo9), Volume: float64(out.OfferRem9)},
			{Price: float64(out.OfferHo10), Volume: float64(out.OfferRem10)},
		},
		Bids: []exchange.OrderbookItem{
			{Price: float64(out.BidHo1), Volume: float64(out.BidRem1)},
			{Price: float64(out.BidHo2), Volume: float64(out.BidRem2)},
			{Price: float64(out.BidHo3), Volume: float64(out.BidRem3)},
			{Price: float64(out.BidHo4), Volume: float64(out.BidRem4)},
			{Price: float64(out.BidHo5), Volume: float64(out.BidRem5)},
			{Price: float64(out.BidHo6), Volume: float64(out.BidRem6)},
			{Price: float64(out.BidHo7), Volume: float64(out.BidRem7)},
			{Price: float64(out.BidHo8), Volume: float64(out.BidRem8)},
			{Price: float64(out.BidHo9), Volume: float64(out.BidRem9)},
			{Price: float64(out.BidHo10), Volume: float64(out.BidRem10)},
		},
	}
}

func buildExchangeBalanceFromT0424(resp *lsclient.T0424Response) (*exchange.Balance, error) {
	if resp == nil {
		return nil, nil
	}

	totalAsset := resp.OutBlock.Tappamt
	if totalAsset == 0 {
		totalAsset = resp.OutBlock.Sunamt
	}

	out := &exchange.Balance{
		TotalAsset: totalAsset,
		Holdings:   make([]exchange.Holding, 0, len(resp.OutBlock1)),
	}

	for _, holding := range resp.OutBlock1 {
		code, ok := normalizeIssueCode(holding.Expcode)
		if !ok {
			continue
		}

		pnlPct, err := holding.Sunikrt.Scaled(lsclient.RateScale)
		if err != nil {
			return nil, &lsclient.FieldParseError{
				Field: "t0424OutBlock1.sunikrt",
				Kind:  "scaled-int64",
				Value: holding.Sunikrt.String(),
				Err:   err,
			}
		}

		out.Holdings = append(out.Holdings, exchange.Holding{
			Code:         code,
			Name:         strings.TrimSpace(holding.Hname),
			Quantity:     holding.Janqty,
			AvgCost:      holding.Pamt,
			CurrentPrice: holding.Price,
			PnL:          holding.Dtsunik,
			PnLPct:       pnlPct,
		})
	}

	return out, nil
}

func normalizeIssueCode(code string) (string, bool) {
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
