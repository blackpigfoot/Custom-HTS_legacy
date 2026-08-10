package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	rootkiwoom "Custom-HTS/internal/adapter/broker/kiwoom"
	"Custom-HTS/internal/core/pkg/requester"
)

// RESTDependencies contains the low-level primitives required by the REST service.
type RESTDependencies struct {
	// Requester sends HTTP requests through the shared retry and rate-limit pipeline.
	Requester *requester.Requester
	// Auth provides bearer tokens for REST calls.
	Auth *Auth
	// RestURL is the Kiwoom REST endpoint base URL.
	RestURL string
	// AccountNo stores the configured Kiwoom account number for account-scoped APIs.
	AccountNo string
}

// REST is the low-level Kiwoom REST service.
type REST struct {
	// requester sends HTTP requests through the shared retry and rate-limit pipeline.
	requester *requester.Requester
	// auth provides bearer tokens for REST calls.
	auth *Auth
	// restURL is the Kiwoom REST endpoint base URL.
	restURL string
	// accountNo stores the configured Kiwoom account number for account-scoped APIs.
	accountNo string
	// apiLimiters stores API-specific limiters keyed by Kiwoom API ID.
	apiLimiters *limits
}

// NewREST creates a low-level Kiwoom REST service from explicit dependencies.
func NewREST(deps RESTDependencies) (*REST, error) {
	if deps.Requester == nil {
		return nil, ErrNilRequester
	}
	if deps.Auth == nil {
		return nil, ErrNilAuth
	}
	if deps.RestURL == "" {
		deps.RestURL = rootkiwoom.BaseURLDefault
	}

	// rest is the low-level Kiwoom REST service being initialized.
	rest := &REST{
		requester: deps.Requester,
		auth:      deps.Auth,
		restURL:   deps.RestURL,
		accountNo: deps.AccountNo,
	}
	rest.setLimiters()
	return rest, nil
}

// SendRawREST sends a generic Kiwoom REST request and returns raw response data.
func (svc *REST) SendRawREST(ctx context.Context, req RawRequest) (*RawResponse, error) {
	// resp stores the raw Kiwoom response headers and body.
	resp := RawResponse{
		Header: make(requester.Header),
	}
	if err := svc.sendREST(ctx, restRequest{
		method:       req.Method,
		path:         req.Path,
		apiID:        req.APIID,
		contYN:       req.ContinuationYN,
		nextKey:      req.NextKey,
		bodyReq:      req.Body,
		resultHeader: resp.Header,
		resultBody:   &resp.Body,
	}); err != nil {
		return nil, &OperationError{
			Op:  "kiwoom send raw REST",
			Err: err,
		}
	}
	return &resp, nil
}

// sendREST sends a single Kiwoom REST request using the shared requester pipeline.
func (svc *REST) sendREST(ctx context.Context, req restRequest) error {
	// method keeps the request default at POST when the caller leaves it blank.
	method := req.method
	if method == "" {
		method = requester.PostMethod
	}

	// limiter resolves the API-specific limiter for the outbound request.
	limiter := svc.limiterForAPI(req.apiID)

	// err stores the requester pipeline result for the outbound Kiwoom call.
	err := svc.requester.Send(ctx, func(ctx context.Context) (*requester.Item, error) {
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return nil, &OperationError{
					Op:  "kiwoom send REST rate limit",
					Err: err,
				}
			}
		}

		// token is the bearer token used by the Kiwoom REST request.
		token, err := svc.auth.GetToken(ctx)
		if err != nil {
			return nil, &OperationError{
				Op:  "kiwoom send REST token",
				Err: err,
			}
		}

		// body is the encoded JSON request body reader.
		var body io.Reader
		if req.bodyReq != nil {
			// payload is the JSON-encoded vendor-native request DTO.
			payload, err := json.Marshal(req.bodyReq)
			if err != nil {
				return nil, &OperationError{
					Op:  "kiwoom send REST encode body",
					Err: err,
				}
			}
			body = bytes.NewReader(payload)
		}

		return &requester.Item{
			Method:        method,
			Path:          svc.restURL + req.path,
			Headers:       svc.buildRESTHeaders(token, req),
			Body:          body,
			ResultHeaders: req.resultHeader,
			ResultBody:    req.resultBody,
		}, nil
	})
	if err != nil {
		return err
	}

	if checker, ok := req.resultBody.(responseChecker); ok {
		if apiErr := checker.CheckError(); apiErr != nil {
			return apiErr
		}
	}

	return nil
}

// buildRESTHeaders creates the vendor-required headers for Kiwoom REST calls.
func (svc *REST) buildRESTHeaders(token string, req restRequest) requester.Header {
	// headers stores the Kiwoom request headers for this REST call.
	headers := make(requester.Header)
	headers.Set("Content-Type", "application/json;charset=UTF-8")
	headers.Set(HeaderAuthorization, "Bearer "+token)
	headers.Set(HeaderContinuationYN, req.contYN)
	headers.Set(HeaderNextKey, req.nextKey)
	headers.Set(HeaderAPIID, req.apiID)
	return headers
}
