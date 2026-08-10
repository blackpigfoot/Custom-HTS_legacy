package api

import (
	rootkiwoom "Custom-HTS/internal/adapter/broker/kiwoom"
	"Custom-HTS/internal/core/pkg/requester"
)

// Config is the root Kiwoom API configuration.
type Config = rootkiwoom.Config

// BrokerName is the logical Kiwoom broker identifier exposed to callers.
const BrokerName = rootkiwoom.BrokerName

// API is the low-level combined Kiwoom REST API service.
type API struct {
	// REST exposes low-level Kiwoom REST operations.
	*REST
	// Auth is the shared OAuth token service used by the embedded services.
	*Auth
}

// New creates a combined low-level Kiwoom REST API service.
func New(cfg Config) (*API, error) {
	// req is the shared HTTP requester used by auth and REST services.
	req, err := requester.New(nil)
	if err != nil {
		return nil, err
	}

	// restURL is the configured Kiwoom REST endpoint or the default endpoint.
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = rootkiwoom.BaseURLDefault
	}

	// authService manages Kiwoom OAuth token issuance and caching.
	authService, err := NewAuth(AuthConfig{
		Requester: req,
		RestURL:   restURL,
		AppKey:    cfg.AppKey,
		SecretKey: cfg.SecretKey,
	})
	if err != nil {
		return nil, err
	}

	// restService sends authenticated Kiwoom REST requests.
	restService, err := NewREST(RESTDependencies{
		Requester: req,
		Auth:      authService,
		RestURL:   restURL,
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
