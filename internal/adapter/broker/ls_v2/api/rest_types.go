package api

import (
	"strconv"
	"strings"

	"Custom-HTS/internal/core/pkg/requester"
)

// const defines the various constants used by the REST service.
const (
	
	// PathStockAccount is the REST path for account TRs.
	PathStockAccount = "/stock/accno"
	// PathStockChart is the REST path for chart TRs.
	PathStockChart = "/stock/chart"
	// TrIDPrice is the LS TR code for the single-price lookup.
	TrIDPrice = "t1102"
	// TrIDOrderbook is the LS TR code for the orderbook lookup.
	TrIDOrderbook = "t1101"
	// TrIDMultiPrice is the LS TR code for the multi-price lookup.
	TrIDMultiPrice = "t8407"
	// TrIDChartMinute is the LS TR code for the chart-minute lookup.
	TrIDChartMinute = "t8412"
	// TrIDBalanceT0424 is the LS TR code for the stock balance lookup.
	TrIDBalanceT0424 = "t0424"
	// TrIDInvestmentWarning is the LS TR code for the investment warning lookup.
	TrIDInvestmentWarning = "t1405"
	// TrIDChart_Integration_Stock_Minute is the LS TR code for the integrated stock minute lookup.
	TrIDChart_Integration_Stock_Minute = "t8452"
	// TrIDChart_Integration_Stock_Tick is the LS TR code for the integrated stock tick(n-tick) lookup.
	TrIDChart_Integration_Stock_Tick = "t8453"
	// TrIDBalanceCSPAQ is the LS TR code for the account asset lookup.
	TrIDBalanceCSPAQ = "CSPAQ12300"
	// t1101LevelCount is the number of orderbook levels exposed by t1101.
	t1101LevelCount = 10

	// t8407MaxCodes is the documented maximum number of codes allowed by t8407.
	t8407MaxCodes = 50
)

const (
	// jsonFieldRspCd is the common LS response-code field name.
	jsonFieldRspCd = "rsp_cd"

	// jsonFieldRspMsg is the common LS response-message field name.
	jsonFieldRspMsg = "rsp_msg"

	// jsonBlockT1102Out is the t1102 output block name.
	jsonBlockT1102Out = "t1102OutBlock"

	// jsonBlockT1101Out is the t1101 output block name.
	jsonBlockT1101Out = "t1101OutBlock"

	// jsonBlockT0424Out is the t0424 summary block name.
	jsonBlockT0424Out = "t0424OutBlock"

	// jsonBlockT0424Holdings is the t0424 holdings block name.
	jsonBlockT0424Holdings = "t0424OutBlock1"

	// jsonBlockCSPAQOut1 is the CSPAQ12300 meta block name.
	jsonBlockCSPAQOut1 = "CSPAQ12300OutBlock1"

	// jsonBlockCSPAQOut2 is the CSPAQ12300 summary block name.
	jsonBlockCSPAQOut2 = "CSPAQ12300OutBlock2"

	// jsonBlockCSPAQOut3 is the CSPAQ12300 holdings block name.
	jsonBlockCSPAQOut3 = "CSPAQ12300OutBlock3"
)

const (
	// RateScale is the default fixed-point scale used by parseRate.
	RateScale int64 = 10000

	// rateScaleDigits is the number of decimal digits preserved by RateScale.
	rateScaleDigits = 4
)

const (
	// t1102FieldDiff is the t1102 diff field used by adapter-side conversions.
	t1102FieldDiff = "diff"

	// t0424FieldSunikrt is the t0424 profit-rate field used by adapter-side conversions.
	t0424FieldSunikrt = "sunikrt"
)

// baseResponse is the common LS response envelope embedded by selected DTOs.
type baseResponse struct {
	RspCd  string `json:"rsp_cd"`  // Response code.
	RspMsg string `json:"rsp_msg"` // Response message.
}

// restRequest is the internal transport envelope used by sendREST.
type restRequest struct {
	method       string           // HTTP method.
	path         string           // REST path.
	trID         string           // LS TR code.
	header       restHeaderSpec   // REST header values excluding the bearer token.
	bodyReq      any              // Request body DTO.
	resultHeader requester.Header // Response header decode target.
	resultBody   any              // Response decode target DTO.
}

// restHeaderSpec contains LS REST header values that are stable for one generated attempt.
type restHeaderSpec struct {
	trCont    string // Continuation flag header value.
	trContKey string // Continuation key header value.
	macAddr   string // MAC address header value.
}

// responseChecker is implemented by DTOs that expose LS business errors.
type responseChecker interface {
	CheckError() error
}

// DecimalValue stores an LS decimal field as its original text representation.
type DecimalValue string

// String returns the raw decimal text.
func (d DecimalValue) String() string {
	return string(d)
}

// Empty reports whether the decimal value is blank.
func (d DecimalValue) Empty() bool {
	return trimASCIISpaceString(string(d)) == ""
}

// Float64 parses the decimal text into float64 when the caller explicitly asks for it.
func (d DecimalValue) Float64() (float64, error) {
	value := trimASCIISpaceString(string(d))
	if value == "" {
		return 0, ErrMissingValue
	}
	return strconv.ParseFloat(value, 64)
}

// Scaled parses the decimal text into a fixed-point int64 using a caller-defined scale.
func (d DecimalValue) Scaled(scale int64) (int64, error) {
	digits, ok := decimalScaleDigits(scale)
	if !ok {
		return 0, ErrInvalidDecimalScale
	}

	value := trimASCIISpaceString(string(d))
	if value == "" {
		return 0, ErrMissingValue
	}

	sign := int64(1)
	switch value[0] {
	case '-':
		sign = -1
		value = value[1:]
	case '+':
		value = value[1:]
	}
	if value == "" {
		return 0, strconv.ErrSyntax
	}

	wholePart, fracPart, hasDot := strings.Cut(value, ".")
	if hasDot && strings.Contains(fracPart, ".") {
		return 0, strconv.ErrSyntax
	}
	if wholePart == "" {
		wholePart = "0"
	}
	whole, err := strconv.ParseInt(wholePart, 10, 64)
	if err != nil {
		return 0, err
	}

	frac := int64(0)
	limit := min(len(fracPart), digits)
	for i := range limit {
		frac = frac*10 + int64(fracPart[i]-'0')
	}
	for i := limit; i < digits; i++ {
		frac *= 10
	}

	return sign * (whole*scale + frac), nil
}

// trimASCIISpaceString trims ASCII whitespace around a string.
func trimASCIISpaceString(value string) string {
	start := 0
	end := len(value)
	for start < end && isASCIISpace(value[start]) {
		start++
	}
	for start < end && isASCIISpace(value[end-1]) {
		end--
	}
	return value[start:end]
}

// decimalScaleDigits validates that the scale is a positive power of 10.
func decimalScaleDigits(scale int64) (int, bool) {
	if scale <= 0 {
		return 0, false
	}
	if scale == 1 {
		return 0, true
	}
	digits := 0
	for scale > 1 {
		if scale%10 != 0 {
			return 0, false
		}
		scale /= 10
		digits++
	}
	return digits, true
}

// isASCIISpace reports whether b is an ASCII whitespace byte.
func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\n' || b == '\r' || b == '\t'
}
