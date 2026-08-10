package rest

import (
	rootls "Custom-HTS/internal/adapter/broker/ls"
	authsvc "Custom-HTS/internal/adapter/broker/ls/api/auth"
	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
	"Custom-HTS/internal/core/pkg/rate"
	"Custom-HTS/internal/core/pkg/requester"
)

// Dependencies contains the low-level primitives required by the REST service.
type Dependencies struct {
	// Requester sends HTTP requests through the shared retry and rate-limit pipeline.
	Requester *requester.Requester
	// Auth provides bearer tokens for REST calls.
	Auth *authsvc.Auth
	// RestURL is the LS REST endpoint base URL.
	RestURL string
	// AccountNo stores the configured LS account number for account-scoped TRs.
	AccountNo string
}

// REST is the low-level LS REST service.
//
// REST owns only LS-native REST request composition, TR-specific rate
// limiting, and response DTO decoding. It does not know about realtime
// subscription handles or fan-out policies.
type REST struct {
	// requester sends HTTP requests through the shared retry and rate-limit pipeline.
	requester *requester.Requester
	// auth provides bearer tokens for REST calls.
	auth *authsvc.Auth
	// restURL is the LS REST endpoint base URL.
	restURL string
	// accountNo stores the configured LS account number for account-scoped TRs.
	accountNo string
	// limiter is the fallback limiter used when no TR-specific bucket is
	// configured.
	limiter rate.Limiter
	// trLimiters stores TR-specific limiters keyed by LS TR code.
	trLimiters map[string]rate.Limiter
}

// New creates a low-level LS REST service from explicit dependencies.
func New(deps Dependencies) (*REST, error) {
	if deps.Requester == nil {
		return nil, apierr.ErrNilRequester
	}
	if deps.Auth == nil {
		return nil, apierr.ErrNilAuth
	}
	if deps.RestURL == "" {
		deps.RestURL = rootls.BaseURLDefault
	}

	rest := &REST{
		requester: deps.Requester,
		auth:      deps.Auth,
		restURL:   deps.RestURL,
		accountNo: deps.AccountNo,
	}
	rest.setLimiters()
	return rest, nil
}
