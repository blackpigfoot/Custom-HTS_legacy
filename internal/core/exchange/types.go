package exchange

import (
	"Custom-HTS/internal/core/pkg/logger"
	"Custom-HTS/internal/core/service/config"
	"time"
)

var (
	// EnvironmentReal — 실전 투자 환경.
	EnvironmentReal = "real"
	// EnvironmentPaper — 모의 투자 환경.
	EnvironmentPaper = "paper"
)

// SetupParams — Exchange.Setup()에 전달되는 설정 구조체.
// AccountConfig을 포함하여, 각 거래소 구현에서 필요로 하는 추가 설정을 포함.
type SetupParams struct {

	// 공통 설정 (거래소/증권사 구분 없이 모든 구현에서 필요)
	config.AccountConfig

	// 기본 API URL (테스트용 httptest.Server URL 또는 실/모의 투자 URL)
	BaseURL string

	// 이벤트 채널
	Channels *BrokerChannels

	// 거래소별 로그. Broker 필드 포함하도록 초기화.
	Log *logger.Logger

	// 추가 설정 값들
	// 사용자에서 타입단언으로 추출
	T any
}

func (s *SetupParams) SetOptions(options any) {
	s.T = options
}

// NewSetupParams — SetupParams 생성자.
func NewSetupParams(cfg config.AccountConfig, ch *BrokerChannels, log *logger.Logger) *SetupParams {
	return &SetupParams{
		AccountConfig: cfg,
		Channels:      ch,
		Log:           log,
	}
}

// Price — 현재가 정보.
//
// 각 거래소 매핑:
//
//	KIS:     stck_prpr → Current, prdy_vrss → Change, prdy_ctrt → ChangePct
//	Binance: price → Current (24hr ticker에서 나머지 필드)
//	Upbit:   trade_price → Current, signed_change_price → Change
type Price struct {
	Code      string    `json:"code"`       // 종목코드 (주식: "005930", 코인: "BTCUSDT")
	Current   int64     `json:"current"`    // 현재가
	Open      int64     `json:"open"`       // 시가
	High      int64     `json:"high"`       // 고가
	Low       int64     `json:"low"`        // 저가
	PrevClose int64     `json:"prev_close"` // 전일 종가
	Volume    int64     `json:"volume"`     // 거래량
	Change    int64     `json:"change"`     // 전일대비 변동 (절대값)
	ChangePct int64     `json:"change_pct"` // 전일대비 변동률 (%)
	Timestamp time.Time `json:"timestamp"`  // 시세 시각
}

// Orderbook — 호가 정보.
//
// 매수/매도 호가를 분리하여 저장.
type Orderbook struct {
	Code string          `json:"code"`
	Bids []OrderbookItem `json:"bids"` // 매수 호가 (가격 내림차순, bids[0]이 최우선)
	Asks []OrderbookItem `json:"asks"` // 매도 호가 (가격 오름차순, asks[0]이 최우선)
}

// OrderbookItem — 개별 호가.
type OrderbookItem struct {
	Price  float64 `json:"price"`  // 호가 가격
	Volume float64 `json:"volume"` // 잔량
}

// Candle — OHLCV 캔들 데이터.
//
// Phase 5 DataHistoryManager가 수집하여 DB에 저장.
// Phase 6 백테스팅 엔진의 입력 데이터.
type Candle struct {
	Code      string    `json:"code"`
	Timestamp time.Time `json:"timestamp"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
}

// Side — 매수/매도 구분.
type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

// OrderType — 주문 유형.
type OrderType string

const (
	Limit  OrderType = "limit"  // 지정가 (가격 지정)
	Market OrderType = "market" // 시장가 (즉시 체결, 가격은 시장에 맡김)
)

// OrderStatus — 주문 상태.
type OrderStatus string

const (
	StatusPending   OrderStatus = "pending"   // 접수 대기 (서버에 전송됨)
	StatusOpen      OrderStatus = "open"      // 미체결 (주문장에 등록됨)
	StatusFilled    OrderStatus = "filled"    // 전량 체결
	StatusPartial   OrderStatus = "partial"   // 부분 체결 (일부만 체결)
	StatusCancelled OrderStatus = "cancelled" // 취소됨
	StatusRejected  OrderStatus = "rejected"  // 거부됨 (잔고 부족 등)
)

// OrderRequest — 주문 요청.
//
// UI에서 사용자가 입력하는 주문 정보.
// Gateway → Account → Exchange.PlaceOrder()로 전달.
//
// 사용:
//
//	gateway.PlaceOrder(ctx, "kis-main", exchange.OrderRequest{
//	    Code:     "005930",
//	    Side:     exchange.Buy,
//	    Type:     exchange.Limit,
//	    Quantity: 10,
//	    Price:    72000,
//	})
type OrderRequest struct {
	Code     string    `json:"code"`     // 종목코드
	Side     Side      `json:"side"`     // 매수/매도
	Type     OrderType `json:"type"`     // 지정가/시장가
	Quantity int64     `json:"quantity"` // 수량
	Price    int64     `json:"price"`    // 지정가 가격 (시장가면 0)
}

// OrderResponse — 주문 접수 응답.
//
// PlaceOrder()가 성공하면 이 구조체 반환.
// OrderID를 CancelOrder()에 전달하여 주문 취소 가능.
type OrderResponse struct {
	OrderID   string      `json:"order_id"`   // 거래소에서 부여한 주문번호
	Code      string      `json:"code"`       // 종목코드
	Side      Side        `json:"side"`       // 매수/매도
	Type      OrderType   `json:"type"`       // 주문 유형
	Quantity  int64       `json:"quantity"`   // 주문 수량
	Price     int64       `json:"price"`      // 주문 가격
	Status    OrderStatus `json:"status"`     // 초기 상태 (보통 Open)
	CreatedAt time.Time   `json:"created_at"` // 접수 시각
}

// Order — 주문 상세 (체결 내역 포함).
type Order struct {
	OrderID      string      `json:"order_id"`
	Code         string      `json:"code"`
	Side         Side        `json:"side"`
	Type         OrderType   `json:"type"`
	Quantity     float64     `json:"quantity"`       // 원래 주문 수량
	FilledQty    float64     `json:"filled_qty"`     // 체결된 수량
	Price        float64     `json:"price"`          // 주문 가격
	AvgFillPrice float64     `json:"avg_fill_price"` // 평균 체결가
	Status       OrderStatus `json:"status"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}

// HistoryOpts — 주문 이력 조회 옵션.
type HistoryOpts struct {
	StartDate time.Time `json:"start_date"`
	EndDate   time.Time `json:"end_date"`
	Code      string    `json:"code"`  // 특정 종목 필터 (빈 문자열이면 전체)
	Limit     int       `json:"limit"` // 최대 조회 건수
}

// Balance — 계좌 잔고 정보.
type Balance struct {
	TotalAsset  int64     `json:"total_asset"`  // 총 자산 평가액
	CashBalance int64     `json:"cash_balance"` // 현금 잔고 (예수금)
	Holdings    []Holding `json:"holdings"`     // 보유 종목 목록
}

// Holding — 보유 종목 정보.
type Holding struct {
	Code         string `json:"code"`          // 종목코드
	Name         string `json:"name"`          // 종목명
	Quantity     int64  `json:"quantity"`      // 보유 수량
	AvgCost      int64  `json:"avg_cost"`      // 평균 매입가
	CurrentPrice int64  `json:"current_price"` // 현재가
	PnL          int64  `json:"pnl"`           // 평가 손익 (원)
	PnLPct       int64  `json:"pnl_pct"`       // 평가 손익률 (%)
}
