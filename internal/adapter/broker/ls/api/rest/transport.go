package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
	"Custom-HTS/internal/core/pkg/requester"
)

// sendREST sends a single LS REST request using the shared requester pipeline.
//
// The REST layer intentionally avoids sync.Pool for request bodies now. Request
// payloads are small, readability matters more here, and json.Marshal keeps the
// body creation path explicit.
func (svc *REST) sendREST(ctx context.Context, req restRequest) error {
	method := req.meta.method
	if method == "" {
		method = http.MethodPost
	}
	limiter := svc.limiterForTR(req.meta.trID)

	err := svc.requester.Send(ctx, func(ctx context.Context) (*requester.Item, error) {
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return nil, &apierr.OperationError{
					Op:  "ls send REST rate limit",
					Err: err,
				}
			}
		}

		token, err := svc.auth.GetToken(ctx)
		if err != nil {
			return nil, &apierr.OperationError{
				Op:  "ls send REST token",
				Err: err,
			}
		}

		var body io.Reader
		if req.bodyReq != nil {
			payload, err := json.Marshal(req.bodyReq)
			if err != nil {
				return nil, &apierr.OperationError{
					Op:  "ls send REST encode body",
					Err: err,
				}
			}
			body = bytes.NewReader(payload)
		}

		return &requester.Item{
			Method:  method,
			Path:    svc.restURL + req.meta.path,
			Headers: svc.buildRESTHeaders(token, req),
			Body:    body,
			Result:  req.result,
		}, nil
	})
	if err != nil {
		return err
	}

	if checker, ok := req.result.(responseChecker); ok {
		if apiErr := checker.CheckError(); apiErr != nil {
			return apiErr
		}
	}

	return nil
}

// buildRESTHeaders creates the vendor-required headers for LS REST calls.
func (svc *REST) buildRESTHeaders(token string, req restRequest) map[string]string {
	return map[string]string{
		"Content-Type":  "application/json; charset=utf-8",
		"authorization": "Bearer " + token,
		"tr_cd":         req.meta.trID,
		"tr_cont":       orDefault(req.trCont, "N"),
		"tr_cont_key":   req.trContKey,
	}
}

// GetPrice returns the native LS t1102 response for a single issue code.
func (svc *REST) GetPrice(ctx context.Context, code string) (*T1102Response, error) {
	resp, err := svc.fetchT1102(ctx, code)
	if err != nil {
		return nil, &apierr.OperationError{
			Op:  "ls get price",
			Err: err,
		}
	}
	return resp, nil
}

// GetOrderbook returns the native LS t1101 response for a single issue code.
func (svc *REST) GetOrderbook(ctx context.Context, code string) (*T1101Response, error) {
	resp, err := svc.fetchT1101(ctx, code)
	if err != nil {
		return nil, &apierr.OperationError{
			Op:  "ls get orderbook",
			Err: err,
		}
	}
	return resp, nil
}

// GetMultiPrices returns the native LS t8407 multi-price response.
//
// LS expects up to 50 six-digit issue codes concatenated into one shcode field.
func (svc *REST) GetMultiPrices(ctx context.Context, codes ...string) (*T8407Response, error) {
	resp, err := svc.fetchT8407(ctx, codes)
	if err != nil {
		return nil, &apierr.OperationError{
			Op:  "ls get multi prices",
			Err: err,
		}
	}
	return resp, nil
}

// GetBalance returns the native LS t0424 account balance response.
func (svc *REST) GetBalance(ctx context.Context) (*T0424Response, error) {
	resp, err := svc.fetchT0424(ctx)
	if err != nil {
		return nil, &apierr.OperationError{
			Op:  "ls get balance",
			Err: err,
		}
	}
	return resp, nil
}

// fetchT1102 issues the t1102 current-price REST call.
func (svc *REST) fetchT1102(ctx context.Context, code string) (*T1102Response, error) {
	var resp T1102Response
	if err := svc.sendREST(ctx, restMetaT1102.request(
		t1102Request{
			In: t1102InBlock{
				Shcode: code,
			},
		},
		&resp,
	)); err != nil {
		return nil, err
	}
	return &resp, nil
}

// fetchT1101 issues the t1101 orderbook REST call.
func (svc *REST) fetchT1101(ctx context.Context, code string) (*T1101Response, error) {
	var resp T1101Response
	if err := svc.sendREST(ctx, restMetaT1101.request(
		t1101Request{
			In: t1101InBlock{
				Shcode: code,
			},
		},
		&resp,
	)); err != nil {
		return nil, err
	}
	return &resp, nil
}

// fetchT8407 issues the t8407 multi-price REST call.
//
// Request creation stays inline here because the request shape is tiny and the
// validation rules are easier to read next to the actual REST call.
func (svc *REST) fetchT8407(ctx context.Context, codes []string) (*T8407Response, error) {
	if len(codes) == 0 {
		return nil, ErrT8407CodesRequired
	}
	if len(codes) > t8407MaxCodes {
		return nil, &CodeLimitError{
			TRCode: TrIDMultiPrice,
			Limit:  t8407MaxCodes,
			Count:  len(codes),
		}
	}

	var joined strings.Builder
	for _, code := range codes {
		normalized, ok := normalizeIssueCode(code)
		if !ok {
			return nil, &apierr.InvalidIssueCodeError{
				TRCode: TrIDMultiPrice,
				Value:  code,
			}
		}
		joined.WriteString(normalized)
	}

	var resp T8407Response
	if err := svc.sendREST(ctx, restMetaT8407.request(
		t8407Request{
			In: t8407InBlock{
				Nrec:   len(codes),
				Shcode: joined.String(),
			},
		},
		&resp,
	)); err != nil {
		return nil, err
	}
	return &resp, nil
}

// fetchT0424 issues the t0424 balance REST call.
func (svc *REST) fetchT0424(ctx context.Context) (*T0424Response, error) {
	var resp T0424Response
	if err := svc.sendREST(ctx, restMetaT0424.request(
		t0424Request{
			In: t0424InBlock{
				Prcgb:  "1",
				Chegb:  "0",
				Dangb:  "0",
				Charge: "1",
			},
		},
		&resp,
	)); err != nil {
		return nil, err
	}
	return &resp, nil
}

// orDefault returns fallback when the value is blank after trimming spaces.
func orDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// normalizeIssueCode normalizes an LS issue code into a six-digit stock code.
//
// It accepts both plain six-digit codes and the common "A123456" prefixed form.
func normalizeIssueCode(code string) (string, bool) {
	code = strings.TrimSpace(code)
	if code == "" {
		return "", false
	}
	if after, ok := strings.CutPrefix(code, "A"); ok {
		code = after
	}
	if len(code) != 6 {
		return "", false
	}
	return code, isDecimalDigits(code)
}

// isDecimalDigits reports whether every byte in the string is an ASCII digit.
func isDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
