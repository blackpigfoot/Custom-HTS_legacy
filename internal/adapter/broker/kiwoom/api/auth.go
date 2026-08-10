package api

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	rootkiwoom "Custom-HTS/internal/adapter/broker/kiwoom"
	"Custom-HTS/internal/core/pkg/rate"
	"Custom-HTS/internal/core/pkg/requester"
	"Custom-HTS/internal/core/pkg/security/keystore"
)

const (
	// issueRPM is the conservative OAuth issue-token request limit.
	issueRPM = 5
	// tokenExpiryLayout is the Kiwoom expires_dt timestamp layout.
	tokenExpiryLayout = "20060102150405"
)

// kiwoomTimeLocation is the fixed Korea Standard Time zone used by expires_dt.
var kiwoomTimeLocation = time.FixedZone("KST", 9*60*60)

// AuthConfig configures Kiwoom OAuth token issuance.
type AuthConfig struct {
	// Requester sends HTTP requests through the shared retry and rate-limit pipeline. Required.
	Requester *requester.Requester
	// RestURL is the Kiwoom REST endpoint base URL. Optional, defaults to rootkiwoom.BaseURLDefault.
	RestURL string
	// AppKey is the Kiwoom REST API application key. Required.
	AppKey string
	// SecretKey is the Kiwoom REST API secret key. Required.
	SecretKey string
}

func (cfg *AuthConfig) validate() error {
	if cfg.Requester == nil {
		return ErrNilRequester
	}
	if cfg.AppKey == "" {
		return &MissingValueError{Field: "appKey"}
	}
	if cfg.SecretKey == "" {
		return &MissingValueError{Field: "secretKey"}
	}
	return nil
}

// Auth manages OAuth token issuance and caching for Kiwoom REST API access.
type Auth struct {
	// req is used to send HTTP requests for token issuance.
	req *requester.Requester
	// tokenStore caches the issued token and its expiry time.
	tokenStore *keystore.KeyStore[cachedToken]
	// restURL is the Kiwoom REST endpoint base URL.
	restURL string
	// appKey is the Kiwoom REST API application key.
	appKey string
	// secretKey is the Kiwoom REST API secret key.
	secretKey string
	// tokenLimiter rate-limits token issuance requests to avoid vendor limits.
	tokenLimiter rate.Limiter
}

// NewAuth creates a new Auth instance with the provided configuration.
func NewAuth(cfg AuthConfig) (*Auth, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.RestURL == "" {
		cfg.RestURL = rootkiwoom.BaseURLDefault
	}

	// auth is the Kiwoom OAuth service being initialized.
	auth := &Auth{
		req:          cfg.Requester,
		restURL:      cfg.RestURL,
		appKey:       cfg.AppKey,
		secretKey:    cfg.SecretKey,
		tokenLimiter: rate.NewRateLimitPerInterval(issueRPM, time.Minute, issueRPM),
	}

	// store caches and refreshes Kiwoom OAuth tokens.
	store, err := keystore.NewKeyStore(keystore.StoreConfig[cachedToken]{
		Issue: auth.issueToken,
	})
	if err != nil {
		return nil, &OperationError{
			Op:  "kiwoom create token store",
			Err: err,
		}
	}
	auth.tokenStore = store

	return auth, nil
}

// GetToken retrieves a valid access token from the cache or issues a new one if needed.
func (c *Auth) GetToken(ctx context.Context) (string, error) {
	// token is the cached or newly issued Kiwoom OAuth token.
	token, err := c.tokenStore.Get(ctx)
	if err != nil {
		return "", &OperationError{
			Op:  "kiwoom get token",
			Err: err,
		}
	}
	return token.Response.Token, nil
}

// SetToken manually installs a bearer token with a relative lifetime.
func (c *Auth) SetToken(token string, expiresIn time.Duration) {
	c.tokenStore.Set(cachedToken{
		Response: tokenResponse{
			CommonResponse: CommonResponse{ReturnCode: 0},
			Token:          token,
			TokenType:      "bearer",
		},
		ExpiresAt: time.Now().Add(expiresIn),
	})
}

// ClearToken removes the cached bearer token.
func (c *Auth) ClearToken() {
	c.tokenStore.Clear()
}

// issueToken performs the OAuth token issuance flow for Kiwoom REST API access.
func (c *Auth) issueToken(ctx context.Context) (cachedToken, error) {
	// bodyReq is the Kiwoom OAuth token request DTO.
	bodyReq := tokenRequest{
		GrantType: "client_credentials",
		AppKey:    c.appKey,
		SecretKey: c.secretKey,
	}

	// payload is the JSON-encoded Kiwoom OAuth request body.
	payload, err := json.Marshal(bodyReq)
	if err != nil {
		return cachedToken{}, &OperationError{
			Op:  "kiwoom issue token encode body",
			Err: err,
		}
	}

	// resp stores the decoded Kiwoom OAuth response payload.
	var resp tokenResponse
	if err := c.req.Send(ctx, func(ctx context.Context) (*requester.Item, error) {
		if err := c.tokenLimiter.Wait(ctx); err != nil {
			return nil, &OperationError{
				Op:  "kiwoom issue token rate limit",
				Err: err,
			}
		}

		// headers stores the Kiwoom OAuth request headers.
		headers := make(requester.Header)
		headers.Set("Content-Type", "application/json;charset=UTF-8")

		return &requester.Item{
			Method:     requester.PostMethod,
			Path:       c.restURL + rootkiwoom.PathTokenIssue,
			Headers:    headers,
			Body:       bytes.NewReader(payload),
			ResultBody: &resp,
		}, nil
	}); err != nil {
		return cachedToken{}, &OperationError{
			Op:  "kiwoom issue token",
			Err: err,
		}
	}

	if apiErr := resp.CheckError(); apiErr != nil {
		return cachedToken{}, apiErr
	}
	if !resp.isSuccess() {
		return cachedToken{}, &MissingValueError{Field: "token"}
	}

	// expiresAt is the parsed absolute Kiwoom token expiration time.
	expiresAt, err := parseExpiresAt(resp.ExpiresDT)
	if err != nil {
		return cachedToken{}, &OperationError{
			Op:  "kiwoom parse token expiry",
			Err: err,
		}
	}

	return cachedToken{
		Response:  resp,
		ExpiresAt: expiresAt,
	}, nil
}

// tokenRequest is the OAuth token issue request DTO.
type tokenRequest struct {
	// GrantType is the OAuth grant type and should be client_credentials.
	GrantType string `json:"grant_type"`
	// AppKey is the Kiwoom REST API application key.
	AppKey string `json:"appkey"`
	// SecretKey is the Kiwoom REST API secret key.
	SecretKey string `json:"secretkey"`
}

// tokenResponse is the OAuth token issue response DTO.
type tokenResponse struct {
	// CommonResponse stores the shared Kiwoom business response fields.
	CommonResponse
	// ExpiresDT is the token expiration timestamp in yyyyMMddHHmmss format.
	ExpiresDT string `json:"expires_dt"`
	// TokenType is the issued token type.
	TokenType string `json:"token_type"`
	// Token is the bearer token returned by Kiwoom OAuth.
	Token string `json:"token"`
}

// cachedToken stores the Kiwoom OAuth response plus the absolute local expiry time.
type cachedToken struct {
	// Response is the raw Kiwoom OAuth response DTO.
	Response tokenResponse
	// ExpiresAt is the local absolute expiry time parsed from the response.
	ExpiresAt time.Time
}

// IsValid reports whether the cached Kiwoom token is still usable.
func (t cachedToken) IsValid() bool {
	return t.Response.isSuccess() && time.Now().Before(t.ExpiresAt)
}

// isSuccess reports whether the token payload contains an access token.
func (r *tokenResponse) isSuccess() bool {
	return r.Token != ""
}

// parseExpiresAt parses a Kiwoom expires_dt timestamp.
func parseExpiresAt(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, &MissingValueError{Field: "expires_dt"}
	}
	return time.ParseInLocation(tokenExpiryLayout, value, kiwoomTimeLocation)
}
