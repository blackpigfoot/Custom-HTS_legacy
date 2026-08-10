package api

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	rootls "Custom-HTS/internal/adapter/broker/ls"
	"Custom-HTS/internal/core/pkg/rate"
	"Custom-HTS/internal/core/pkg/requester"
	"Custom-HTS/internal/core/pkg/security/keystore"
)

const (
	// issueRPM is the conservative OAuth issue-token request limit.
	issueRPM = 5
)

// AuthConfig configures LS OAuth token issuance.
type AuthConfig struct {
	// Requester sends HTTP requests through the shared retry and rate-limit pipeline. Required.
	Requester *requester.Requester
	// RestURL is the LS REST endpoint base URL. Optional, defaults to rootls.BaseURLDefault.
	RestURL string
	// AppKey is the LS OpenAPI application key. Required.
	AppKey string
	// AppSecret is the LS OpenAPI application secret. Required.
	AppSecret string
}

func (cfg *AuthConfig) validate() error {
	if cfg.Requester == nil {
		return ErrNilRequester
	}
	if cfg.AppKey == "" {
		return &MissingValueError{Field: "appKey"}
	}
	if cfg.AppSecret == "" {
		return &MissingValueError{Field: "appSecret1"}
	}
	return nil
}

// Auth manages OAuth token issuance and caching for LS Securities API access.
type Auth struct {
	// req is used to send HTTP requests for token issuance.
	req *requester.Requester
	// tokenStore caches the issued token and its expiry time.
	tokenStore *keystore.KeyStore[cachedToken]
	// restURL is the LS REST endpoint base URL.
	restURL string
	// appKey is the LS OpenAPI application key.
	appKey string
	// appSecret is the LS OpenAPI application secret.
	appSecret string
	// tokenLimiter rate-limits token issuance requests to avoid vendor limits.
	tokenLimiter rate.Limiter
}

// NewAuth creates a new Auth instance with the provided configuration.
func NewAuth(cfg AuthConfig) (*Auth, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.RestURL == "" {
		cfg.RestURL = rootls.BaseURLDefault
	}

	auth := &Auth{
		req:          cfg.Requester,
		restURL:      cfg.RestURL,
		appKey:       cfg.AppKey,
		appSecret:    cfg.AppSecret,
		tokenLimiter: rate.New(1, 1),
	}
	store, err := keystore.NewKeyStore(keystore.StoreConfig[cachedToken]{
		Issue: auth.issueToken,
	})
	if err != nil {
		return nil, &OperationError{
			Op:  "ls create token store",
			Err: err,
		}
	}
	auth.tokenStore = store

	return auth, nil
}

// GetToken retrieves a valid access token from the cache or issues a new one if needed.
func (c *Auth) GetToken(ctx context.Context) (string, error) {
	token, err := c.tokenStore.Get(ctx)
	if err != nil {
		return "", &OperationError{
			Op:  "ls get token",
			Err: err,
		}
	}
	return token.Response.AccessToken, nil
}

// SetToken manually installs a bearer token with a relative lifetime.
func (c *Auth) SetToken(token string, expiresIn time.Duration) {
	c.tokenStore.Set(cachedToken{
		Response: tokenResponse{
			AccessToken: token,
			ExpiresIn:   int(expiresIn.Seconds()),
		},
		ExpiresAt: time.Now().Add(expiresIn),
	})
}

// ClearToken removes the cached bearer token.
func (c *Auth) ClearToken() {
	c.tokenStore.Clear()
}

// issueToken performs the OAuth token issuance flow for LS Securities.
func (c *Auth) issueToken(ctx context.Context) (cachedToken, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("appkey", c.appKey)
	form.Set("appsecretkey", c.appSecret)
	form.Set("scope", "oob")

	var resp tokenResponse
	if err := c.req.Send(ctx, func(ctx context.Context) (*requester.Item, error) {
		if err := c.tokenLimiter.Wait(ctx); err != nil {
			return nil, &OperationError{
				Op:  "ls issue token rate limit",
				Err: err,
			}
		}
		headers := make(requester.Header)
		headers.Set("Content-Type", "application/x-www-form-urlencoded")

		return &requester.Item{
			Method:     http.MethodPost,
			Path:       c.restURL + rootls.PathTokenIssue,
			Headers:    headers,
			Body:       strings.NewReader(form.Encode()),
			ResultBody: &resp,
		}, nil
	}); err != nil {
		return cachedToken{}, &OperationError{
			Op:  "ls issue token",
			Err: err,
		}
	}

	if !resp.isSuccess() {
		return cachedToken{}, &MissingValueError{Field: "access_token"}
	}

	return cachedToken{
		Response:  resp,
		ExpiresAt: time.Now().Add(time.Duration(resp.ExpiresIn) * time.Second),
	}, nil
}

// tokenResponse is the OAuth token issue response DTO.
type tokenResponse struct {
	// AccessToken is the bearer token returned by LS OAuth.
	AccessToken string `json:"access_token"`
	// ExpiresIn is the token lifetime in seconds.
	ExpiresIn int `json:"expires_in"`
	// Scope is the granted OAuth scope.
	Scope string `json:"scope"`
	// TokenType is the issued token type.
	TokenType string `json:"token_type"`
}

// cachedToken stores the LS OAuth response plus the absolute local expiry time.
type cachedToken struct {
	// Response is the raw LS OAuth response DTO.
	Response tokenResponse
	// ExpiresAt is the local absolute expiry time derived when the token was issued.
	ExpiresAt time.Time
}

// IsValid reports whether the cached LS token is still usable.
func (t cachedToken) IsValid() bool {
	return t.Response.isSuccess() && time.Now().Before(t.ExpiresAt)
}

// isSuccess reports whether the token payload contains an access token.
func (r *tokenResponse) isSuccess() bool {
	return r.AccessToken != ""
}
