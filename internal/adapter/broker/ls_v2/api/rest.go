package api

import (
	"bytes"
	"context"
	"github.com/goccy/go-json"
	"errors"
	"io"
	"strconv"
	"strings"

	rootls "Custom-HTS/internal/adapter/broker/ls"
	"Custom-HTS/internal/core/pkg/requester"
)

type Header = requester.Header

// errors reports the various error conditions that can occur in the REST service.
var (
	// ErrT8407CodesRequired reports that t8407 was called without any codes.
	ErrT8407CodesRequired = errors.New("t8407 requires at least one code")

	// ErrTooManyCodes reports that the caller provided more codes than a TR allows.
	ErrTooManyCodes = errors.New("too many codes")

	// ErrInvalidDecimalScale reports that a decimal scale must be a positive power of 10.
	ErrInvalidDecimalScale = errors.New("invalid decimal scale")
)

// CodeLimitError reports that a request exceeded the TR-specific code limit.
type CodeLimitError struct {
	// TRCode is the LS TR code that owns the limit.
	TRCode string
	// Limit is the maximum allowed item count.
	Limit int
	// Count is the caller-provided item count.
	Count int
}

func (e *CodeLimitError) Error() string {
	if e == nil {
		return ""
	}

	msg := "too many codes"
	if e.TRCode != "" {
		msg = "ls " + e.TRCode + " allows up to " + strconv.Itoa(e.Limit) + " codes"
	} else if e.Limit > 0 {
		msg = "allows up to " + strconv.Itoa(e.Limit) + " codes"
	}
	if e.Count > 0 {
		msg += ": got " + strconv.Itoa(e.Count)
	}
	return msg
}

func (e *CodeLimitError) Is(target error) bool {
	return target == ErrTooManyCodes
}

// RESTDependencies contains the low-level primitives required by the REST service.
type RESTDependencies struct {
	// Requester sends HTTP requests through the shared retry and rate-limit pipeline.
	Requester *requester.Requester
	// Auth provides bearer tokens for REST calls.
	Auth *Auth
	// AccountNo stores the configured LS account number for account-scoped TRs.
	AccountNo string
}

// REST is the low-level LS REST service.
type REST struct {
	// requester sends HTTP requests through the shared retry and rate-limit pipeline.
	requester *requester.Requester
	// auth provides bearer tokens for REST calls.
	auth *Auth
	// restURL is the LS REST endpoint base URL.
	restURL string
	// accountNo stores the configured LS account number for account-scoped TRs.
	accountNo string
	// trLimiters stores TR-specific limiters keyed by LS TR code.
	trLimiters *limits
}

// NewREST creates a low-level LS REST service from explicit dependencies.
func NewREST(deps RESTDependencies) (*REST, error) {
	if deps.Requester == nil {
		return nil, ErrNilRequester
	}
	if deps.Auth == nil {
		return nil, ErrNilAuth
	}

	rest := &REST{
		requester: deps.Requester,
		auth:      deps.Auth,
		restURL:   rootls.BaseURLDefault,
		accountNo: deps.AccountNo,
	}
	rest.setLimiters()
	return rest, nil
}

// sendREST sends a single LS REST request using the shared requester pipeline.
func (svc *REST) sendREST(ctx context.Context, req restRequest) error {
	// method keeps the request default at POST when the caller leaves it blank.
	if req.method == "" {
		return errors.New("HTTP method is required")
	}

	// limiter resolves the TR-specific limiter for the outbound request.
	limiter := svc.limiterForTR(req.trID)

	err := svc.requester.Send(ctx, func(ctx context.Context) (*requester.Item, error) {
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return nil, &OperationError{
					Op:  "ls send REST rate limit",
					Err: err,
				}
			}
		}

		token, err := svc.auth.GetToken(ctx)
		if err != nil {
			return nil, &OperationError{
				Op:  "ls send REST token",
				Err: err,
			}
		}

		var body io.Reader
		if req.bodyReq != nil {
			payload, err := json.Marshal(req.bodyReq)
			if err != nil {
				return nil, &OperationError{
					Op:  "ls send REST encode body",
					Err: err,
				}
			}
			body = bytes.NewReader(payload)
		}

		return &requester.Item{
			Method:        req.method,
			Path:          svc.restURL + req.path,
			Headers:       req.header.build(token, req.trID),
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

// build creates the vendor-required LS REST headers for one request attempt.
func (spec restHeaderSpec) build(token, trID string) requester.Header {
	headers := make(requester.Header)
	headers.Set("Content-Type", "application/json; charset=utf-8")
	headers.Set("authorization", "Bearer "+token)
	headers.Set("tr_cd", trID)
	headers.Set("tr_cont", orDefault(spec.trCont, "N"))
	headers.Set("tr_cont_key", spec.trContKey)
	headers.Set("mac_address", spec.macAddr)
	return headers
}

// GetPrice returns the native LS t1102 response for a single issue code.
func (svc *REST) GetPrice(ctx context.Context, code string) (*T1102Response, error) {
	// resp stores the decoded vendor-native response payload.
	var resp T1102Response
	if err := svc.sendREST(ctx, restRequest{
		method: requester.PostMethod,
		path:   PathStockMarket,
		trID:   TrIDPrice,
		bodyReq: t1102Request{
			In: t1102InBlock{
				Shcode: code,
			},
		},
		resultBody: &resp,
	}); err != nil {
		return nil, &OperationError{
			Op:  "ls get price",
			Err: err,
		}
	}
	return &resp, nil
}

func (svc *REST) RawGetPrice(ctx context.Context, code string) (map[string][]string, []byte, error) {
	// resp stores the decoded vendor-native response payload.
	var resp []byte
	header := make(requester.Header)
	if err := svc.sendREST(ctx, restRequest{
		method: requester.PostMethod,
		path:   PathStockMarket,
		trID:   TrIDPrice,
		bodyReq: t1102Request{
			In: t1102InBlock{
				Shcode: code,
			},
		},
		resultHeader: header,
		resultBody:   &resp,
	}); err != nil {
		return nil, nil, &OperationError{
			Op:  "ls get price",
			Err: err,
		}
	}
	return header, resp, nil
}

// GetOrderbook returns the native LS t1101 response for a single issue code.
func (svc *REST) GetOrderbook(ctx context.Context, code string) (*T1101Response, error) {
	// resp stores the decoded vendor-native response payload.
	var resp T1101Response
	if err := svc.sendREST(ctx, restRequest{
		method: requester.PostMethod,
		path:   PathStockMarket,
		trID:   TrIDOrderbook,
		bodyReq: t1101Request{
			In: t1101InBlock{
				Shcode: code,
			},
		},
		resultBody: &resp,
	}); err != nil {
		return nil, &OperationError{
			Op:  "ls get orderbook",
			Err: err,
		}
	}
	return &resp, nil
}

// GetMultiPrices returns the native LS t8407 multi-price response.
func (svc *REST) GetMultiPrices(ctx context.Context, codes []string) (Header, *T8407Response, error) {
	if len(codes) == 0 {
		return nil, nil, &OperationError{
			Op:  "ls get multi prices",
			Err: ErrT8407CodesRequired,
		}
	}
	if len(codes) > t8407MaxCodes {
		return nil, nil, &OperationError{
			Op: "ls get multi prices",
			Err: &CodeLimitError{
				TRCode: TrIDMultiPrice,
				Limit:  t8407MaxCodes,
				Count:  len(codes),
			},
		}
	}

	// joined stores the concatenated vendor-native code list.
	var joined strings.Builder
	for _, code := range codes {
		normalized, ok := normalizeIssueCode(code)
		if !ok {
			return nil, nil, &OperationError{
				Op: "ls get multi prices",
				Err: &InvalidIssueCodeError{
					TRCode: TrIDMultiPrice,
					Value:  code,
				},
			}
		}
		joined.WriteString(normalized)
	}
	header := make(requester.Header)
	// resp stores the decoded vendor-native response payload.
	var resp T8407Response
	if err := svc.sendREST(ctx, restRequest{
		method: requester.PostMethod,
		path:   PathStockMarket,
		trID:   TrIDMultiPrice,
		bodyReq: t8407Request{
			In: t8407InBlock{
				Nrec:   len(codes),
				Shcode: joined.String(),
			},
		},
		resultHeader: header,
		resultBody: &resp,
	}); err != nil {
		return nil, nil, &OperationError{
			Op:  "ls get multi prices",
			Err: err,
		}
	}
	return header, &resp, nil
}

// GetBalance returns the native LS t0424 account balance response.
func (svc *REST) GetBalance(ctx context.Context) (*T0424Response, error) {
	// resp stores the decoded vendor-native response payload.
	var resp T0424Response
	if err := svc.sendREST(ctx, restRequest{
		method: requester.PostMethod,
		path:   PathStockAccount,
		trID:   TrIDBalanceT0424,
		bodyReq: t0424Request{
			In: t0424InBlock{
				Prcgb:  "1",
				Chegb:  "0",
				Dangb:  "0",
				Charge: "1",
			},
		},
		resultBody: &resp,
	}); err != nil {
		return nil, &OperationError{
			Op:  "ls get balance",
			Err: err,
		}
	}
	return &resp, nil
}

// InvestmentWarning returns the native t1405 investment-warning response.
func (svc *REST) InvestmentWarning(ctx context.Context, cont bool, gubun, jongchk string) (map[string][]string, *T1405Response, error) {
	// merged stores the first response plus any continuation rows.
	var merged *T1405Response

	// nextTrCont stores the continuation header from the previous response.
	nextTrCont := "N"
	// nextTrContKey stores the continuation key header from the previous response.
	nextTrContKey := ""
	// nextCtsShcode stores the continuation issue code from the previous response body.
	nextCtsShcode := ""

	// lastHeader stores the response headers from the most recent request.
	var lastHeader requester.Header
	for {
		// resp stores the decoded vendor-native response payload for one page.
		var resp T1405Response
		// header stores the response headers for one page.
		header := make(requester.Header)
		if err := svc.sendREST(ctx, restRequest{
			method: requester.PostMethod,
			path:   PathStockMarket,
			trID:   TrIDInvestmentWarning,
			bodyReq: t1405Request{
				In: t1405InBlock{
					Gubun:      gubun,
					Jongchk:    jongchk,
					Cts_shcode: nextCtsShcode,
				},
			},
			header: restHeaderSpec{
				trCont:    nextTrCont,
				trContKey: nextTrContKey,
			},
			resultHeader: header,
			resultBody:   &resp,
		}); err != nil {
			return nil, nil, &OperationError{
				Op:  "ls get investment warning",
				Err: err,
			}
		}
		lastHeader = header
		if merged == nil {
			merged = &resp
		} else {
			merged.T1405OutBlock = resp.T1405OutBlock
			merged.T1405OutBlock1 = append(merged.T1405OutBlock1, resp.T1405OutBlock1...)
		}

		nextTrCont = strings.TrimSpace(header.Get("tr_cont"))
		nextTrContKey = strings.TrimSpace(header.Get("tr_cont_key"))
		nextCtsShcode = strings.TrimSpace(resp.T1405OutBlock.Cts_shcode)
		if !cont || !strings.EqualFold(nextTrCont, "Y") || nextCtsShcode == "" {
			return lastHeader, merged, nil
		}
	}
}

// orDefault returns fallback when the value is blank after trimming spaces.
func orDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// normalizeIssueCode normalizes an LS issue code into a six-digit stock code.
func normalizeIssueCode(code string) (string, bool) {
	code = strings.TrimSpace(code)
	if code == "" {
		return "", false
	}
	if after, ok := strings.CutPrefix(code, "A"); ok {
		code = after
	}
	if code == "" {
		return "", false
	}
	return code, true
}
