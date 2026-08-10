package rest

import "Custom-HTS/internal/core/pkg/rate"

const (
	// defaultRESTRPS is the conservative fallback request-per-second limit.
	defaultRESTRPS = 2
)

func (svc *REST) setLimiters() {
	// defaultLimiter is shared until endpoint-specific quotas are verified.
	defaultLimiter := rate.NewRateLimit(defaultRESTRPS, defaultRESTRPS)
	svc.limiter = defaultLimiter
	svc.trLimiters = map[string]rate.Limiter{
		TrIDPrice:        defaultLimiter,
		TrIDOrderbook:    defaultLimiter,
		TrIDMultiPrice:   defaultLimiter,
		TrIDBalanceT0424: defaultLimiter,
		TrIDBalanceCSPAQ: defaultLimiter,
	}
}

func (svc *REST) limiterForTR(trID string) rate.Limiter {
	if svc == nil {
		return nil
	}
	if limiter, ok := svc.trLimiters[trID]; ok && limiter != nil {
		return limiter
	}
	return svc.limiter
}
