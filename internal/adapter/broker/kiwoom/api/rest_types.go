package api

import "Custom-HTS/internal/core/pkg/requester"

const (
	// PathDomesticStockQuote is the Kiwoom domestic stock quote REST path.
	PathDomesticStockQuote = "/api/dostk/stkinfo"
	// PathDomesticStockOrder is the Kiwoom domestic stock order REST path.
	PathDomesticStockOrder = "/api/dostk/ordr"
	// PathDomesticStockAccount is the Kiwoom domestic stock account REST path.
	PathDomesticStockAccount = "/api/dostk/acnt"
	// PathDomesticStockChart is the Kiwoom domestic stock chart REST path.
	PathDomesticStockChart = "/api/dostk/chart"
)

const (
	// HeaderAuthorization is the Kiwoom bearer-token header name.
	HeaderAuthorization = "authorization"
	// HeaderContinuationYN is the Kiwoom continuation flag header name.
	HeaderContinuationYN = "cont-yn"
	// HeaderNextKey is the Kiwoom continuation key header name.
	HeaderNextKey = "next-key"
	// HeaderAPIID is the Kiwoom TR identifier header name.
	HeaderAPIID = "api-id"
)

// restRequest is the internal transport envelope used by sendREST.
type restRequest struct {
	method       string           // HTTP method.
	path         string           // REST path.
	apiID        string           // Kiwoom API or TR identifier.
	contYN       string           // Continuation flag header value.
	nextKey      string           // Continuation key header value.
	bodyReq      any              // Request body DTO.
	resultHeader requester.Header // Response header decode target.
	resultBody   any              // Response decode target DTO.
}

// responseChecker is implemented by DTOs that expose Kiwoom business errors.
type responseChecker interface {
	CheckError() error
}

// CommonResponse contains fields shared by Kiwoom REST JSON responses.
type CommonResponse struct {
	// ReturnCode is the Kiwoom business response code.
	ReturnCode int `json:"return_code"`
	// ReturnMsg is the Kiwoom business response message.
	ReturnMsg string `json:"return_msg"`
}

// CheckError reports a Kiwoom business error when ReturnCode is non-zero.
func (r *CommonResponse) CheckError() error {
	if r == nil || r.ReturnCode == 0 {
		return nil
	}
	return &KiwoomError{
		ReturnCode: r.ReturnCode,
		ReturnMsg:  r.ReturnMsg,
	}
}

// RawRequest describes a generic Kiwoom REST request for early TR integration work.
type RawRequest struct {
	// Method is the HTTP method for this request. Empty defaults to POST.
	Method string
	// Path is the Kiwoom REST path such as /api/dostk/stkinfo.
	Path string
	// APIID is the Kiwoom TR identifier placed in the api-id header.
	APIID string
	// ContinuationYN is the optional continuation flag placed in the cont-yn header.
	ContinuationYN string
	// NextKey is the optional continuation key placed in the next-key header.
	NextKey string
	// Body is the vendor-native JSON body DTO.
	Body any
}

// RawResponse stores undecoded Kiwoom response headers and body bytes.
type RawResponse struct {
	// Header stores response headers returned by Kiwoom.
	Header requester.Header
	// Body stores the raw JSON response body returned by Kiwoom.
	Body []byte
}
