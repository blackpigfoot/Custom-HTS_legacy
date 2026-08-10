package kis

import "time"

// KisConfig — KIS 전용 추가 설정. exchange.SetupParams.T에 담아서 전달.
type KisConfig struct {
	BaseURL string // 테스트용 mock 서버 URL. 빈 문자열이면 실전/모의 기본값 사용.
}

const (
	BaseURLReal  = "https://openapi.koreainvestment.com:9443"
	BaseURLPaper = "https://openapivts.koreainvestment.com:29443"
)

const (
	PathTokenIssue  = "/oauth2/tokenP"
	PathApprovalKey = "/oauth2/Approval"
	PathPrice       = "/uapi/domestic-stock/v1/quotations/inquire-price"
	PathOrderbook   = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
	PathOrderCash   = "/uapi/domestic-stock/v1/trading/order-cash"
	PathBalance     = "/uapi/domestic-stock/v1/trading/inquire-balance"
)

const (
	TrIDPrice     = "FHKST01010100"
	TrIDOrderbook = "FHKST01010200"

	TrIDBuyReal   = "TTTC0802U"
	TrIDBuyPaper  = "VTTC0802U"
	TrIDSellReal  = "TTTC0801U"
	TrIDSellPaper = "VTTC0801U"

	TrIDBalanceReal  = "TTTC8434R"
	TrIDBalancePaper = "VTTC8434R"
)

// tokenExpiryMargin — 토큰/승인키 만료 전 갱신 여유 시간.
// 네트워크 지연이나 시계 오차로 인해 만료 직전에 토큰을 쓰는 상황을 방지.
const tokenExpiryMargin = 10 * time.Minute

// RateScale — 비율(%) 고정소수점 스케일.
// 1.25% → 12500, 0.01% 정밀도.
// 실제 퍼센트 = raw / RateScale.
const RateScale int64 = 10000

// ApprovalKeyValidity — KIS 승인키 유효기간 (문서 기준 24시간).
var ApprovalKeyValidity = 24 * time.Hour

// kisResponse — 모든 KIS REST API 응답의 공통 필드.
type kisResponse struct {
	RtCd  string `json:"rt_cd"`
	MsgCd string `json:"msg_cd"`
	Msg1  string `json:"msg1"`
}

func (r *kisResponse) isSuccess() bool { return r.RtCd == "0" }

func (r *kisResponse) CheckError() error {
	if r.isSuccess() {
		return nil
	}
	return &KISError{
		RtCd:  r.RtCd,
		MsgCd: r.MsgCd,
		Msg:   r.Msg1,
	}
}

// restRequest — sendREST() 인자 구조체.
//
// path는 완성된 전체 URL (baseURL + path + query params).
// sendREST가 baseURL을 덧붙이지 않으므로 호출부에서 완성해서 넘긴다.
type restRequest struct {
	method  string
	path    string // 완성된 전체 URL
	trID    string
	bodyReq any // nil이면 Body 없음
	result  any // 응답 디코딩 대상
}

// tokenRequest/Response — POST /oauth2/tokenP
type tokenRequest struct {
	GrantType string `json:"grant_type"`
	AppKey    string `json:"appkey"`
	AppSecret string `json:"appsecret"` // KIS 전용 필드명
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

func (t *tokenResponse) isSuccess() bool { return t.AccessToken != "" }

// approvalKeyRequest/Response — POST /oauth2/Approval
type approvalKeyRequest struct {
	GrantType string `json:"grant_type"`
	AppKey    string `json:"appkey"`
	AppSecret string `json:"secretkey"` // 토큰 발급과 필드명 다름
}

type approvalKeyResponse struct {
	ApprovalKey string `json:"approval_key"`
}

func (r *approvalKeyResponse) isSuccess() bool { return r.ApprovalKey != "" }

// priceResponse — GET /uapi/domestic-stock/v1/quotations/inquire-price
type priceResponse struct {
	kisResponse
	Output priceOutput `json:"output"`
}

type priceOutput struct {
	StckPrpr string `json:"stck_prpr"` // 현재가
	StckOprc string `json:"stck_oprc"` // 시가
	StckHgpr string `json:"stck_hgpr"` // 고가
	StckLwpr string `json:"stck_lwpr"` // 저가
	StckSdpr string `json:"stck_sdpr"` // 전일종가
	AcmlVol  string `json:"acml_vol"`  // 누적거래량
	PrdyVrss string `json:"prdy_vrss"` // 전일대비
	PrdyCtrt string `json:"prdy_ctrt"` // 전일대비율(%)
}

// orderRequest/Response — POST /uapi/domestic-stock/v1/trading/order-cash
type orderRequest struct {
	CANO    string `json:"CANO"`
	ACNT    string `json:"ACNT_PRDT_CD"`
	PDNO    string `json:"PDNO"`
	OrdDvSn string `json:"ORD_DVSN"`
	OrdQty  string `json:"ORD_QTY"`
	OrdUnpr string `json:"ORD_UNPR"`
}

type orderResponse struct {
	kisResponse
	Output orderOutput `json:"output"`
}

type orderOutput struct {
	OrdNo  string `json:"ODNO"`
	OrdTmd string `json:"ORD_TMD"`
}

// balanceResponse — GET /uapi/domestic-stock/v1/trading/inquire-balance
type balanceResponse struct {
	kisResponse
	Output1 []balanceHolding `json:"output1"`
	Output2 []balanceSummary `json:"output2"`
}

type balanceHolding struct {
	Pdno        string `json:"pdno"`
	PrdtNm      string `json:"prdt_name"`
	HldgQty     string `json:"hldg_qty"`
	PchsAvgPric string `json:"pchs_avg_pric"`
	Prpr        string `json:"prpr"`
	EvluPflsAmt string `json:"evlu_pfls_amt"`
	EvluPflsRt  string `json:"evlu_pfls_rt"`
}

type balanceSummary struct {
	TotEvluAmt string `json:"tot_evlu_amt"`
	DncaTotAmt string `json:"dnca_tot_amt"`
}
