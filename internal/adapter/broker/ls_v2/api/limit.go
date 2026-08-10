package api

import "Custom-HTS/internal/core/pkg/rate"

// limits stores TR-specific rate limiters for LS REST calls.
type limits struct {
	// trPrice throttles single-price TR requests.
	trPrice rate.Limiter
	// trOrderbook throttles orderbook TR requests.
	trOrderbook rate.Limiter
	// trMultiPrice throttles multi-price TR requests.
	trMultiPrice rate.Limiter
	// trChartMinute throttles chart-minute TR requests.
	trChartMinute rate.Limiter
	// trBalanceT0424 throttles t0424 balance TR requests.
	trBalanceT0424 rate.Limiter
	// trBalanceCSPAQ throttles CSPAQ balance TR requests.
	trBalanceCSPAQ rate.Limiter
	
	// tr_Stock_Price_By_Minute throttles t1302 minute price TR requests.
	tr_Stock_Price_By_Minute rate.Limiter

	// tr_Integration_Stock_Minute throttles t8452 minute price TR requests.
	tr_chart_Integration_Stock_Minute rate.Limiter

	// tr_chart_Integration_Stock_Tick throttles t8453 tick price TR requests.
	tr_chart_Integration_Stock_Tick rate.Limiter
}

func (svc *REST) setLimiters() {
	svc.trLimiters = &limits{
		trPrice:      rate.New(2, 2),
		trMultiPrice: rate.New(5, 5),
		trOrderbook:   rate.New(2, 2),
		trChartMinute: rate.New(1, 1),
		trBalanceT0424: rate.New(2, 2),
		trBalanceCSPAQ: rate.New(2, 2),
		tr_Stock_Price_By_Minute: rate.New(1, 1),
		tr_chart_Integration_Stock_Minute: rate.New(1, 1),
		tr_chart_Integration_Stock_Tick:   rate.New(1, 1),
	}
}

func (svc *REST) limiterForTR(trID string) rate.Limiter {
	switch trID {
	case TrIDPrice:
		return svc.trLimiters.trPrice
	case TrIDOrderbook:
		return svc.trLimiters.trOrderbook
	case TrIDMultiPrice:
		return svc.trLimiters.trMultiPrice
	case TrIDBalanceT0424:
		return svc.trLimiters.trBalanceT0424
	case TrIDBalanceCSPAQ:
		return svc.trLimiters.trBalanceCSPAQ
	case TrIDChartMinute:
		return svc.trLimiters.trChartMinute
	case TrIDStockPriceByMinute:
		return svc.trLimiters.tr_Stock_Price_By_Minute
	case TrIDChart_Integration_Stock_Minute:
		return svc.trLimiters.tr_chart_Integration_Stock_Minute
	case TrIDChart_Integration_Stock_Tick:
		return svc.trLimiters.tr_chart_Integration_Stock_Tick
	default:
		return nil
	}
}
