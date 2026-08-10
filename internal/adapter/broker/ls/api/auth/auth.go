package auth

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	rootls "Custom-HTS/internal/adapter/broker/ls"
	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
	"Custom-HTS/internal/core/pkg/rate"
	"Custom-HTS/internal/core/pkg/requester"
	"Custom-HTS/internal/core/pkg/security/keystore"
)

const (
	// issueRPM is the conservative OAuth issue-token request limit.
	issueRPM = 5
)

// Config configures LS OAuth token issuance.
type Config struct {
	// Requester sends HTTP requests through the shared retry and rate-limit pipeline. Required.
	Requester *requester.Requester
	// RestURL is the LS REST endpoint base URL. Optional, defaults to rootls.BaseURLDefault.
	RestURL string
	// AppKey is the LS OpenAPI application key. Required.
	AppKey string
	// AppSecret is the LS OpenAPI application secret. Required.
	AppSecret string
}

func (cfg *Config) validate() error {
	if cfg.Requester == nil {
		return apierr.ErrNilRequester
	}
	if cfg.AppKey == "" {
		return &apierr.MissingValueError{Field: "appKey"}
	}
	if cfg.AppSecret == "" {
		return &apierr.MissingValueError{Field: "appSecret"}
	}
	return nil
}

// Auth manages OAuth token issuance and caching for LS Securities API access.
// It uses a keystore to cache the token and automatically refresh it before expiry.
// The Auth struct is designed to be thread-safe and efficient, minimizing redundant token requests.
type Auth struct {
	// Requester is used to send HTTP requests for token issuance.
	// It should be shared across the application to benefit from retry and rate-limiting features.
	req *requester.Requester
	// tokenStore caches the issued token and its expiry time, automatically refreshing it when needed.
	tokenStore *keystore.KeyStore[cachedToken]
	// restURL is the LS REST endpoint base URL.
	restURL string
	// appKey and appSecret are the credentials used for token issuance.
	appKey string
	// appSecret is the secret key for the LS OpenAPI application.
	appSecret string
	// tokenLimiter is used to rate-limit token issuance requests to avoid hitting LS API limits.
	tokenLimiter rate.Limiter
}

// New creates a new Auth instance with the provided configuration.
func New(cfg Config) (*Auth, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	auth := &Auth{
		req:          cfg.Requester,
		restURL:      cfg.RestURL,
		appKey:       cfg.AppKey,
		appSecret:    cfg.AppSecret,
		tokenLimiter: rate.NewRateLimitPerInterval(issueRPM, time.Minute, issueRPM),
	}
	store, err := keystore.NewKeyStore(keystore.StoreConfig[cachedToken]{
		IsValid: auth.isValid,
		Issue:   auth.issueToken,
	})
	if err != nil {
		return nil, &apierr.OperationError{
			Op:  "ls create token store",
			Err: err,
		}
	}
	auth.tokenStore = store

	return auth, nil
}

// GetToken retrieves a valid access token from the cache or issues a new one if needed.
// It returns the access token string or an error if token retrieval fails.
func (c *Auth) GetToken(ctx context.Context) (string, error) {
	token, err := c.tokenStore.Get(ctx)
	if err != nil {
		return "", &apierr.OperationError{
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

func (a *Auth) isValid(t cachedToken) bool {
	return t.Response.isSuccess() && time.Until(t.ExpiresAt) > tokenExpiryMargin
}

// issueToken performs the OAuth token issuance flow for LS Securities.
func (c *Auth) issueToken(ctx context.Context) (cachedToken, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("appkey", c.appKey)
	form.Set("appsecretkey", c.appSecret)

	var resp tokenResponse
	if err := c.req.Send(ctx, func(ctx context.Context) (*requester.Item, error) {
		if err := c.tokenLimiter.Wait(ctx); err != nil {
			return nil, &apierr.OperationError{
				Op:  "ls issue token rate limit",
				Err: err,
			}
		}
		return &requester.Item{
			Method: http.MethodPost,
			Path:   c.restURL + rootls.PathTokenIssue,
			Headers: map[string]string{
				"Content-Type": "application/x-www-form-urlencoded",
			},
			Body:   strings.NewReader(form.Encode()),
			Result: &resp,
		}, nil
	}); err != nil {
		return cachedToken{}, &apierr.OperationError{
			Op:  "ls issue token",
			Err: err,
		}
	}

	if !resp.isSuccess() {
		return cachedToken{}, &apierr.MissingValueError{Field: "access_token"}
	}

	return cachedToken{
		Response:  resp,
		ExpiresAt: time.Now().Add(time.Duration(resp.ExpiresIn) * time.Second),
	}, nil
}

// tokenExpiryMargin refreshes the token slightly before the server expiry time.
const tokenExpiryMargin = 10 * time.Minute

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

// isSuccess reports whether the token payload contains an access token.
func (r *tokenResponse) isSuccess() bool {
	return r.AccessToken != ""
}
