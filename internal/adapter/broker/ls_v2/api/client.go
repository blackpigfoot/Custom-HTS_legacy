package api

import (
	rootls "Custom-HTS/internal/adapter/broker/ls"
	"Custom-HTS/internal/core/pkg/requester"
)

// Config is the root LS API configuration.
type Config = rootls.Config

// BrokerName is the logical LS broker identifier exposed to callers.
const BrokerName = rootls.BrokerName

// API is the low-level combined LS REST API service.
type API struct {
	// REST exposes low-level LS REST operations.
	*REST
	// auth is the shared OAuth token service used by the embedded services.
	*Auth
}

// New creates a combined low-level LS REST API service.
func New(cfg Config) (*API, error) {
	// req is the shared HTTP requester used by auth and REST services.
	req, err := requester.New(nil)
	if err != nil {
		return nil, err
	}

	authService, err := NewAuth(AuthConfig{
		Requester: req,
		RestURL:   rootls.BaseURLDefault,
		AppKey:    cfg.AppKey,
		AppSecret: cfg.AppSecret,
	})
	if err != nil {
		return nil, err
	}

	restService, err := NewREST(RESTDependencies{
		Requester: req,
		Auth:      authService,
		AccountNo: cfg.AccountNo,
	})
	if err != nil {
		return nil, err
	}

	return &API{
		REST: restService,
		Auth: authService,
	}, nil
}