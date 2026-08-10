package rest

import (
	"net/http"
	"strconv"
	"strings"

	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
)

const (
	// PathStockMarket is the REST path for market-data TRs.
	PathStockMarket = "/stock/market-data"

	// PathStockAccount is the REST path for account TRs.
	PathStockAccount = "/stock/accno"

	// TrIDPrice is the LS TR code for the single-price lookup.
	TrIDPrice = "t1102"

	// TrIDOrderbook is the LS TR code for the orderbook lookup.
	TrIDOrderbook = "t1101"

	// TrIDMultiPrice is the LS TR code for the multi-price lookup.
	TrIDMultiPrice = "t8407"

	// TrIDBalanceT0424 is the LS TR code for the stock balance lookup.
	TrIDBalanceT0424 = "t0424"

	// TrIDBalanceCSPAQ is the LS TR code for the account asset lookup.
	TrIDBalanceCSPAQ = "CSPAQ12300"
)

const (
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

// restRequest is the internal transport envelope used by sendREST.
type restRequest struct {
	meta      restRequestMeta // Static REST endpoint metadata.
	trCont    string          // Continuation flag header value.
	trContKey string          // Continuation key header value.
	bodyReq   any             // Request body DTO.
	result    any             // Response decode target DTO.
}

// restRequestMeta describes the fixed endpoint metadata for one TR.
type restRequestMeta struct {
	method string // HTTP method.
	path   string // REST path.
	trID   string // LS TR code.
}

// request binds a body DTO and response DTO to the static TR metadata.
func (m restRequestMeta) request(bodyReq, result any) restRequest {
	return restRequest{
		meta:    m,
		bodyReq: bodyReq,
		result:  result,
	}
}

var (
	// restMetaT1102 is the static REST metadata for t1102.
	restMetaT1102 = restRequestMeta{
		method: http.MethodPost,
		path:   PathStockMarket,
		trID:   TrIDPrice,
	}

	// restMetaT1101 is the static REST metadata for t1101.
	restMetaT1101 = restRequestMeta{
		method: http.MethodPost,
		path:   PathStockMarket,
		trID:   TrIDOrderbook,
	}

	// restMetaT8407 is the static REST metadata for t8407.
	restMetaT8407 = restRequestMeta{
		method: http.MethodPost,
		path:   PathStockMarket,
		trID:   TrIDMultiPrice,
	}

	// restMetaT0424 is the static REST metadata for t0424.
	restMetaT0424 = restRequestMeta{
		method: http.MethodPost,
		path:   PathStockAccount,
		trID:   TrIDBalanceT0424,
	}

	// restMetaCSPAQ12300 is the static REST metadata for CSPAQ12300.
	restMetaCSPAQ12300 = restRequestMeta{
		method: http.MethodPost,
		path:   PathStockAccount,
		trID:   TrIDBalanceCSPAQ,
	}
)

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
		return 0, apierr.ErrMissingValue
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
		return 0, apierr.ErrMissingValue
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
	if !isDecimalDigits(wholePart) {
		return 0, strconv.ErrSyntax
	}
	if fracPart != "" && !isDecimalDigits(fracPart) {
		return 0, strconv.ErrSyntax
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

// t1101Request is the native request body for t1101.
type t1101Request struct {
	In t1101InBlock `json:"t1101InBlock"` // Input block.
}

// t1101InBlock is the native input block for t1101.
type t1101InBlock struct {
	Shcode string `json:"shcode"` // Short issue code.
}

// t1102Request is the native request body for t1102.
type t1102Request struct {
	In t1102InBlock `json:"t1102InBlock"` // Input block.
}

// t1102InBlock is the native input block for t1102.
type t1102InBlock struct {
	Shcode string `json:"shcode"` // Short issue code.
}

// t8407Request is the native request body for t8407.
type t8407Request struct {
	In t8407InBlock `json:"t8407InBlock"` // Input block.
}

// t8407InBlock is the native input block for t8407.
type t8407InBlock struct {
	Nrec   int    `json:"nrec"`   // Number of requested codes.
	Shcode string `json:"shcode"` // Concatenated short-code list without delimiters.
}

// T1102Response is the native LS response DTO for t1102.
type T1102Response struct {
	RspCd    string        `json:"rsp_cd"`        // Response code.
	RspMsg   string        `json:"rsp_msg"`       // Response message.
	OutBlock T1102OutBlock `json:"t1102OutBlock"` // Price output block.
}

// T1102OutBlock keeps the vendor-native t1102 payload intact.
type T1102OutBlock struct {
	Hname       string       `json:"hname"`       // Issue name.
	Price       int64        `json:"price"`       // Current price.
	Sign        string       `json:"sign"`        // Change sign code.
	Change      int64        `json:"change"`      // Change from previous close.
	Diff        DecimalValue `json:"diff"`        // Change rate text.
	Volume      int64        `json:"volume"`      // Cumulative volume.
	RecPrice    int64        `json:"recprice"`    // Reference price.
	Avg         int64        `json:"avg"`         // Weighted average price.
	JnilClose   int64        `json:"jnilclose"`   // Previous close.
	OfferHo     int64        `json:"offerho"`     // Best ask price.
	BidHo       int64        `json:"bidho"`       // Best bid price.
	OfferRem    int64        `json:"offerrem"`    // Best ask quantity.
	BidRem      int64        `json:"bidrem"`      // Best bid quantity.
	PreOfferCha int64        `json:"preoffercha"` // Ask quantity delta from the previous snapshot.
	PreBidCha   int64        `json:"prebidcha"`   // Bid quantity delta from the previous snapshot.
	Open        int64        `json:"open"`        // Opening price.
	High        int64        `json:"high"`        // Session high.
	Low         int64        `json:"low"`         // Session low.
	HoStatus    string       `json:"ho_status"`   // Quote status.
	Hotime      string       `json:"hotime"`      // Quote time.
	YePrice     int64        `json:"yeprice"`     // Expected match price.
	YeVolume    int64        `json:"yevolume"`    // Expected match volume.
	YeSign      string       `json:"yesign"`      // Expected match sign code.
	YeChange    int64        `json:"yechange"`    // Expected match change.
	YeDiff      DecimalValue `json:"yediff"`      // Expected match change rate text.
	TmOffer     int64        `json:"tmoffer"`     // After-hours ask quantity.
	TmBid       int64        `json:"tmbid"`       // After-hours bid quantity.
	Shcode      string       `json:"shcode"`      // Short issue code.
	UplmtPrice  int64        `json:"uplmtprice"`  // Upper limit price.
	DnlmtPrice  int64        `json:"dnlmtprice"`  // Lower limit price.
	Value       int64        `json:"value"`       // Cumulative traded amount.
	MarketCap   int64        `json:"marketcap"`   // Market capitalization.
}

// T1101Response is the native LS response DTO for t1101.
type T1101Response struct {
	RspCd    string        `json:"rsp_cd"`        // Response code.
	RspMsg   string        `json:"rsp_msg"`       // Response message.
	OutBlock T1101OutBlock `json:"t1101OutBlock"` // Orderbook output block.
}

// T1101OutBlock keeps the vendor-native t1101 orderbook payload intact.
type T1101OutBlock struct {
	Hname      string       `json:"hname"`         // Issue name.
	Price      int64        `json:"price"`         // Current price.
	Sign       string       `json:"sign"`          // Change sign code.
	Change     int64        `json:"change"`        // Change from previous close.
	Diff       DecimalValue `json:"diff"`          // Change rate text.
	Volume     int64        `json:"volume"`        // Cumulative volume.
	JnilClose  int64        `json:"jnilclose"`     // Previous close.
	OfferHo1   int64        `json:"offerho1"`      // Ask price level 1.
	BidHo1     int64        `json:"bidho1"`        // Bid price level 1.
	OfferRem1  int64        `json:"offerrem1"`     // Ask quantity level 1.
	BidRem1    int64        `json:"bidrem1"`       // Bid quantity level 1.
	PreOffer1  int64        `json:"preoffercha1"`  // Ask quantity delta level 1.
	PreBid1    int64        `json:"prebidcha1"`    // Bid quantity delta level 1.
	OfferHo2   int64        `json:"offerho2"`      // Ask price level 2.
	BidHo2     int64        `json:"bidho2"`        // Bid price level 2.
	OfferRem2  int64        `json:"offerrem2"`     // Ask quantity level 2.
	BidRem2    int64        `json:"bidrem2"`       // Bid quantity level 2.
	PreOffer2  int64        `json:"preoffercha2"`  // Ask quantity delta level 2.
	PreBid2    int64        `json:"prebidcha2"`    // Bid quantity delta level 2.
	OfferHo3   int64        `json:"offerho3"`      // Ask price level 3.
	BidHo3     int64        `json:"bidho3"`        // Bid price level 3.
	OfferRem3  int64        `json:"offerrem3"`     // Ask quantity level 3.
	BidRem3    int64        `json:"bidrem3"`       // Bid quantity level 3.
	PreOffer3  int64        `json:"preoffercha3"`  // Ask quantity delta level 3.
	PreBid3    int64        `json:"prebidcha3"`    // Bid quantity delta level 3.
	OfferHo4   int64        `json:"offerho4"`      // Ask price level 4.
	BidHo4     int64        `json:"bidho4"`        // Bid price level 4.
	OfferRem4  int64        `json:"offerrem4"`     // Ask quantity level 4.
	BidRem4    int64        `json:"bidrem4"`       // Bid quantity level 4.
	PreOffer4  int64        `json:"preoffercha4"`  // Ask quantity delta level 4.
	PreBid4    int64        `json:"prebidcha4"`    // Bid quantity delta level 4.
	OfferHo5   int64        `json:"offerho5"`      // Ask price level 5.
	BidHo5     int64        `json:"bidho5"`        // Bid price level 5.
	OfferRem5  int64        `json:"offerrem5"`     // Ask quantity level 5.
	BidRem5    int64        `json:"bidrem5"`       // Bid quantity level 5.
	PreOffer5  int64        `json:"preoffercha5"`  // Ask quantity delta level 5.
	PreBid5    int64        `json:"prebidcha5"`    // Bid quantity delta level 5.
	OfferHo6   int64        `json:"offerho6"`      // Ask price level 6.
	BidHo6     int64        `json:"bidho6"`        // Bid price level 6.
	OfferRem6  int64        `json:"offerrem6"`     // Ask quantity level 6.
	BidRem6    int64        `json:"bidrem6"`       // Bid quantity level 6.
	PreOffer6  int64        `json:"preoffercha6"`  // Ask quantity delta level 6.
	PreBid6    int64        `json:"prebidcha6"`    // Bid quantity delta level 6.
	OfferHo7   int64        `json:"offerho7"`      // Ask price level 7.
	BidHo7     int64        `json:"bidho7"`        // Bid price level 7.
	OfferRem7  int64        `json:"offerrem7"`     // Ask quantity level 7.
	BidRem7    int64        `json:"bidrem7"`       // Bid quantity level 7.
	PreOffer7  int64        `json:"preoffercha7"`  // Ask quantity delta level 7.
	PreBid7    int64        `json:"prebidcha7"`    // Bid quantity delta level 7.
	OfferHo8   int64        `json:"offerho8"`      // Ask price level 8.
	BidHo8     int64        `json:"bidho8"`        // Bid price level 8.
	OfferRem8  int64        `json:"offerrem8"`     // Ask quantity level 8.
	BidRem8    int64        `json:"bidrem8"`       // Bid quantity level 8.
	PreOffer8  int64        `json:"preoffercha8"`  // Ask quantity delta level 8.
	PreBid8    int64        `json:"prebidcha8"`    // Bid quantity delta level 8.
	OfferHo9   int64        `json:"offerho9"`      // Ask price level 9.
	BidHo9     int64        `json:"bidho9"`        // Bid price level 9.
	OfferRem9  int64        `json:"offerrem9"`     // Ask quantity level 9.
	BidRem9    int64        `json:"bidrem9"`       // Bid quantity level 9.
	PreOffer9  int64        `json:"preoffercha9"`  // Ask quantity delta level 9.
	PreBid9    int64        `json:"prebidcha9"`    // Bid quantity delta level 9.
	OfferHo10  int64        `json:"offerho10"`     // Ask price level 10.
	BidHo10    int64        `json:"bidho10"`       // Bid price level 10.
	OfferRem10 int64        `json:"offerrem10"`    // Ask quantity level 10.
	BidRem10   int64        `json:"bidrem10"`      // Bid quantity level 10.
	PreOffer10 int64        `json:"preoffercha10"` // Ask quantity delta level 10.
	PreBid10   int64        `json:"prebidcha10"`   // Bid quantity delta level 10.
	Offer      int64        `json:"offer"`         // Total ask quantity.
	Bid        int64        `json:"bid"`           // Total bid quantity.
	PreOffer   int64        `json:"preoffercha"`   // Total ask quantity delta.
	PreBid     int64        `json:"prebidcha"`     // Total bid quantity delta.
	Hotime     string       `json:"hotime"`        // Quote time.
	YePrice    int64        `json:"yeprice"`       // Expected match price.
	YeVolume   int64        `json:"yevolume"`      // Expected match volume.
	YeSign     string       `json:"yesign"`        // Expected match sign code.
	YeChange   int64        `json:"yechange"`      // Expected match change.
	YeDiff     DecimalValue `json:"yediff"`        // Expected match change rate text.
	TmOffer    int64        `json:"tmoffer"`       // After-hours ask quantity.
	TmBid      int64        `json:"tmbid"`         // After-hours bid quantity.
	HoStatus   string       `json:"ho_status"`     // Quote status.
	Shcode     string       `json:"shcode"`        // Short issue code.
	UplmtPrice int64        `json:"uplmtprice"`    // Upper limit price.
	DnlmtPrice int64        `json:"dnlmtprice"`    // Lower limit price.
	Open       int64        `json:"open"`          // Opening price.
	High       int64        `json:"high"`          // Session high.
	Low        int64        `json:"low"`           // Session low.
}

// T8407Response is the native LS response DTO for t8407.
type T8407Response struct {
	RspCd     string           `json:"rsp_cd"`         // Response code.
	RspMsg    string           `json:"rsp_msg"`        // Response message.
	OutBlock1 []T8407OutBlock1 `json:"t8407OutBlock1"` // Multi-price snapshot list.
}

// T8407OutBlock1 keeps the vendor-native t8407 snapshot payload intact.
type T8407OutBlock1 struct {
	Shcode      string       `json:"shcode"`      // Short issue code.
	Hname       string       `json:"hname"`       // Issue name.
	Price       int64        `json:"price"`       // Current price.
	Sign        string       `json:"sign"`        // Change sign code.
	Change      int64        `json:"change"`      // Change from previous close.
	Diff        DecimalValue `json:"diff"`        // Change rate text.
	Volume      int64        `json:"volume"`      // Cumulative volume.
	OfferHo     int64        `json:"offerho"`     // Best ask price.
	BidHo       int64        `json:"bidho"`       // Best bid price.
	Cvolume     int64        `json:"cvolume"`     // Trade volume.
	Chdegree    DecimalValue `json:"chdegree"`    // Trade strength text.
	Open        int64        `json:"open"`        // Opening price.
	High        int64        `json:"high"`        // Session high.
	Low         int64        `json:"low"`         // Session low.
	Value       int64        `json:"value"`       // Cumulative traded amount.
	OfferRem    int64        `json:"offerrem"`    // Best ask quantity.
	BidRem      int64        `json:"bidrem"`      // Best bid quantity.
	TotOfferRem int64        `json:"totofferrem"` // Total ask quantity.
	TotBidRem   int64        `json:"totbidrem"`   // Total bid quantity.
	JnilClose   int64        `json:"jnilclose"`   // Previous close.
	UplmtPrice  int64        `json:"uplmtprice"`  // Upper limit price.
	DnlmtPrice  int64        `json:"dnlmtprice"`  // Lower limit price.
}

// t0424Request is the native request body for t0424.
type t0424Request struct {
	In t0424InBlock `json:"t0424InBlock"` // Input block.
}

// t0424InBlock is the native input block for t0424.
type t0424InBlock struct {
	Prcgb      string `json:"prcgb"`       // Price type code.
	Chegb      string `json:"chegb"`       // Execution type code.
	Dangb      string `json:"dangb"`       // Single-price type code.
	Charge     string `json:"charge"`      // Include charges flag.
	CtsExpcode string `json:"cts_expcode"` // Continuation issue code.
}

// T0424Response is the native LS response DTO for t0424.
type T0424Response struct {
	RspCd     string           `json:"rsp_cd"`         // Response code.
	RspMsg    string           `json:"rsp_msg"`        // Response message.
	OutBlock  T0424OutBlock    `json:"t0424OutBlock"`  // Balance summary block.
	OutBlock1 []T0424OutBlock1 `json:"t0424OutBlock1"` // Holding list.
}

// T0424OutBlock keeps the vendor-native t0424 summary block intact.
type T0424OutBlock struct {
	Dtsunik    int64  `json:"dtsunik"`     // Total evaluation profit/loss.
	CtsExpcode string `json:"cts_expcode"` // Continuation issue code.
	Mamt       int64  `json:"mamt"`        // Total purchase amount.
	Sunamt1    int64  `json:"sunamt1"`     // Vendor-specific raw net-asset field.
	Tappamt    int64  `json:"tappamt"`     // Total evaluated amount.
	Sunamt     int64  `json:"sunamt"`      // Net asset amount.
	Tdtsunik   int64  `json:"tdtsunik"`    // Vendor-specific raw profit/loss field.
}

// T0424OutBlock1 keeps the vendor-native t0424 holding block intact.
type T0424OutBlock1 struct {
	Sininter   int64        `json:"sininter"`   // Credit interest.
	Fee        int64        `json:"fee"`        // Fee.
	Mamt       int64        `json:"mamt"`       // Purchase amount.
	Sinamt     int64        `json:"sinamt"`     // Credit amount.
	Mpmd       int64        `json:"mpmd"`       // Vendor-specific raw mpmd field.
	Mdposqt    int64        `json:"mdposqt"`    // Sellable quantity.
	Jsat       int64        `json:"jsat"`       // Same-day sell amount.
	Janqty     int64        `json:"janqty"`     // Holding quantity.
	Loandt     string       `json:"loandt"`     // Loan date.
	Sysprocseq int64        `json:"sysprocseq"` // System process sequence.
	Price      int64        `json:"price"`      // Current price.
	Janrt      DecimalValue `json:"janrt"`      // Holding weight text.
	Jdat       int64        `json:"jdat"`       // Vendor-specific same-day sell date field.
	Jpms       int64        `json:"jpms"`       // Same-day sell unit price.
	Hname      string       `json:"hname"`      // Issue name.
	Appamt     int64        `json:"appamt"`     // Evaluated amount.
	Sunikrt    DecimalValue `json:"sunikrt"`    // Profit rate text.
	Jonggb     string       `json:"jonggb"`     // Issue type code.
	Msat       int64        `json:"msat"`       // Same-day buy amount.
	Tax        int64        `json:"tax"`        // Tax.
	Pamt       int64        `json:"pamt"`       // Average purchase price.
	Jpmd       int64        `json:"jpmd"`       // Vendor-specific raw jpmd field.
	Marketgb   string       `json:"marketgb"`   // Market code.
	Jangb      string       `json:"jangb"`      // Holding classification.
	Dtsunik    int64        `json:"dtsunik"`    // Evaluation profit/loss.
	Expcode    string       `json:"expcode"`    // Extended issue code.
	Mdat       int64        `json:"mdat"`       // Vendor-specific same-day buy date field.
	Mpms       int64        `json:"mpms"`       // Same-day buy unit price.
	Lastdt     string       `json:"lastdt"`     // Maturity date.
}

// cspaq12300Request is the native request body for CSPAQ12300.
type cspaq12300Request struct {
	In cspaq12300InBlock1 `json:"CSPAQ12300InBlock1"` // Input block.
}

// cspaq12300InBlock1 is the native input block for CSPAQ12300.
type cspaq12300InBlock1 struct {
	RecCnt         int64  `json:"RecCnt"`         // Record count.
	BalCreTp       string `json:"BalCreTp"`       // Balance creation type.
	CmsnAppTpCode  string `json:"CmsnAppTpCode"`  // Commission application type.
	D2BalBaseQryTp string `json:"D2balBaseQryTp"` // D+2 balance base query type.
	UprcTpCode     string `json:"UprcTpCode"`     // Unit price type code.
}

// CSPAQ12300Response is the native LS response DTO for CSPAQ12300.
type CSPAQ12300Response struct {
	OutBlock1 CSPAQ12300OutBlock1   `json:"CSPAQ12300OutBlock1"` // Response meta block.
	OutBlock2 CSPAQ12300OutBlock2   `json:"CSPAQ12300OutBlock2"` // Asset summary block.
	OutBlock3 []CSPAQ12300OutBlock3 `json:"CSPAQ12300OutBlock3"` // Holding list.
}

// CSPAQ12300OutBlock1 keeps the vendor-native response meta block intact.
type CSPAQ12300OutBlock1 struct {
	RspCd  string `json:"rsp_cd"`  // Response code.
	RspMsg string `json:"rsp_msg"` // Response message.
}

// CSPAQ12300OutBlock2 keeps the vendor-native summary block intact.
type CSPAQ12300OutBlock2 struct {
	Dps          int64 `json:"Dps"`          // Deposit balance.
	DpsastTotamt int64 `json:"DpsastTotamt"` // Total deposit assets.
	BalEvalAmt   int64 `json:"BalEvalAmt"`   // Evaluated balance amount.
}

// CSPAQ12300OutBlock3 keeps the vendor-native holding block intact.
type CSPAQ12300OutBlock3 struct {
	IsuNo   string       `json:"IsuNo"`   // Issue code.
	IsuNm   string       `json:"IsuNm"`   // Issue name.
	BalQty  int64        `json:"BalQty"`  // Holding quantity.
	AvrUprc int64        `json:"AvrUprc"` // Average unit price.
	NowPrc  int64        `json:"NowPrc"`  // Current price.
	EvalPnl int64        `json:"EvalPnl"` // Evaluation profit/loss.
	PnlRat  DecimalValue `json:"PnlRat"`  // Profit/loss rate text.
}

// CheckError reports the LS business error embedded in t1102.
func (r *T1102Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &apierr.LSError{
		RspCd:  r.RspCd,
		RspMsg: r.RspMsg,
	}
}

// CheckError reports the LS business error embedded in t1101.
func (r *T1101Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &apierr.LSError{
		RspCd:  r.RspCd,
		RspMsg: r.RspMsg,
	}
}

// CheckError reports the LS business error embedded in t8407.
func (r *T8407Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &apierr.LSError{
		RspCd:  r.RspCd,
		RspMsg: r.RspMsg,
	}
}

// CheckError reports the LS business error embedded in t0424.
func (r *T0424Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &apierr.LSError{
		RspCd:  r.RspCd,
		RspMsg: r.RspMsg,
	}
}

// CheckError reports the LS business error embedded in CSPAQ12300.
func (r *CSPAQ12300Response) CheckError() error {
	if r == nil {
		return nil
	}
	if r.OutBlock1.RspCd == "" || r.OutBlock1.RspCd == "00000" {
		return nil
	}
	return &apierr.LSError{
		RspCd:  r.OutBlock1.RspCd,
		RspMsg: r.OutBlock1.RspMsg,
	}
}
