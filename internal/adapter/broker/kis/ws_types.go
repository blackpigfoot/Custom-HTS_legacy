package kis

import (
	"Custom-HTS/internal/core/domain"
	"time"
)

const (
	wsURLReal  = "ws://ops.koreainvestment.com:21000"
	wsURLPaper = "ws://ops.koreainvestment.com:31000"
)

const (
	trIDStockTrade = "H0STCNT0" // 주식 실시간 체결가
	trIDStockQuote = "H0STASP0" // 주식 실시간 호가(10단계)
	trIDStockExec  = "H0STCNI0" // 주식 체결통보 (내 주문, AES 암호화)
)

const (
	wsTypeRegister   = "1" // 구독 등록
	wsTypeUnregister = "2" // 구독 해제
)

const (
	pipeSeparator  = '|' // 1차 구분자: "암호화여부|TR_ID|건수|데이터"
	fieldSeparator = '^' // 2차 구분자: "005930^72000^100^..."
	encryptedFlag  = "1" // 첫 필드가 이 값이면 AES 복호화 필요
)

// wsRequest — buildSubPayload()에서 생성 후 json.Marshal → conn.Send.
type wsRequest struct {
	Header wsRequestHeader `json:"header"`
	Body   wsRequestBody   `json:"body"`
}

type wsRequestHeader struct {
	ApprovalKey string `json:"approval_key"`
	CustType    string `json:"custtype"`
	TrType      string `json:"tr_type"`
	ContentType string `json:"content-type"`
}

type wsRequestBody struct {
	Input wsRequestInput `json:"input"`
}

// wsRequestInput — KIS는 body.input 중첩 구조 사용.
type wsRequestInput struct {
	TrID  string `json:"tr_id"`
	TrKey string `json:"tr_key"`
}

// wsRawMessage — JSON 형식으로 수신되는 메시지 파싱 구조체.
//
// 수신 시점:
//   - 구독/해제 확인 응답
//   - 승인키 에러 (MsgCode = "EGW00123", "EGW00201")
//   - 연결 직후 AES 암호화키 전달 (Output.Key/IV)
type wsRawMessage struct {
	Header struct {
		TrID      string `json:"tr_id"`
		TrKey     string `json:"tr_key"`
		Encrypted string `json:"encrypt"` // "N" 또는 "Y"
	} `json:"header"`
	Body struct {
		RtCD    string `json:"rt_cd"`
		MsgCode string `json:"msg_cd"`
		Msg1    string `json:"msg1"`
		Output  struct {
			Key string `json:"key"`
			IV  string `json:"iv"`
		} `json:"output"`
	} `json:"body"`
}

// pipeMsg — parseRawMessage 반환값.
// TrID로 분기 후 Body를 각 파서에 전달.
type pipeMsg struct {
	Encrypted bool
	TrID      string
	Count     int
	Body      string
}

// stockTradeMsg — H0STCNT0 파싱 결과. 핵심 필드만 추출.
//
// 가격 필드: int64 (KRW, scale=1)
// 비율 필드: int64 (×RateScale, 1.25% → 12500)
type stockTradeMsg struct {
	Code       string
	ExecTime   string
	Price      int64 // 현재가 (KRW)
	Change     int64 // 전일대비 (KRW)
	ChangeRate int64 // 등락률 (×RateScale)
	Open       int64 // 시가 (KRW)
	High       int64 // 고가 (KRW)
	Low        int64 // 저가 (KRW)
	AskPrice   int64 // 매도호가1 (KRW)
	BidPrice   int64 // 매수호가1 (KRW)
	Volume     int64 // 체결수량
	AccVolume  int64 // 누적거래량
}

// stockQuoteMsg — H0STASP0 파싱 결과. 매도/매수 각 10단계.
type stockQuoteMsg struct {
	Code     string
	ExecTime string
	Asks     [10]depthEntry
	Bids     [10]depthEntry
	TotalAsk int64
	TotalBid int64
}

type depthEntry struct {
	Price int64 // KRW
	Size  int64
}

// stockExecNotify — H0STCNI0 파싱 결과 (AES 복호화 후).
type stockExecNotify struct {
	AccountNo string
	OrderNo   string
	Code      string
	Side      domain.Side
	Price     int64 // 체결단가 (KRW)
	Quantity  int64
	TotalQty  int64
	RemainQty int64
	Status    string
	ExecTime  time.Time
}
