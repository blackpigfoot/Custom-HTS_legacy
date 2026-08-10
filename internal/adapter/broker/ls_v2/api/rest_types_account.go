package api

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

// CheckError reports the LS business error embedded in t0424.
func (r *T0424Response) CheckError() error {
	if r == nil || r.RspCd == "" || r.RspCd == "00000" {
		return nil
	}
	return &LSError{
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
	return &LSError{
		RspCd:  r.OutBlock1.RspCd,
		RspMsg: r.OutBlock1.RspMsg,
	}
}
