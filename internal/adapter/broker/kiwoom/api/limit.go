package api

import "Custom-HTS/internal/core/pkg/rate"

// limits stores API-specific rate limiters for Kiwoom REST calls.
type limits struct {
	// defaultREST throttles generic Kiwoom REST requests until TR-specific limits are defined.
	defaultREST rate.Limiter
}

func (svc *REST) setLimiters() {
	svc.apiLimiters = &limits{
		defaultREST: rate.New(5, 5),
	}
}

func (svc *REST) limiterForAPI(apiID string) rate.Limiter {
	// apiID is reserved for future TR-specific limiter routing.
	_ = apiID
	return svc.apiLimiters.defaultREST
}
