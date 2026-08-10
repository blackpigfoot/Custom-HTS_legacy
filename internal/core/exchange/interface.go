package exchange

import (
	"context"

	"Custom-HTS/internal/core/service/config"
)

// Exchange — 거래소/증권사 공통 인터페이스.
//
// KIS, Upbit, Binance, COM 브로커 등 모든 구현체가 이 인터페이스를 만족.
// Engine(MarketEngine, ExecutionEngine)은 이 인터페이스만 바라봄.
//
// WS 미지원 거래소는 exchange.Base를 임베딩하면
// Subscribe* 메서드가 자동으로 "not supported" 에러를 반환.
// REST-only 거래소는 별도 WS 메서드 구현 불필요.
type Exchange interface {
	// ─── 메타데이터 ─────────────────────────────────────

	GetName() string
	GetAssetType() config.AssetType

	// ─── 생명주기 ────────────────────────────────────────

	// Start — WS 연결 시작 (비동기 고루틴).
	// REST-only 거래소는 Base 기본 구현(no-op) 사용.
	Start(ctx context.Context)

	// ─── 시세 (REST) ─────────────────────────────────────

	GetPrice(ctx context.Context, code string) (*Price, error)
	GetOrderbook(ctx context.Context, code string) (*Orderbook, error)
	GetCandles(ctx context.Context, code string, interval string, count int) ([]Candle, error)

	// ─── 주문 ────────────────────────────────────────────

	PlaceOrder(ctx context.Context, req OrderRequest) (*OrderResponse, error)
	CancelOrder(ctx context.Context, orderID string) error
	GetOpenOrders(ctx context.Context) ([]Order, error)
	GetOrderHistory(ctx context.Context, opts HistoryOpts) ([]Order, error)

	// ─── 잔고 ────────────────────────────────────────────

	GetBalance(ctx context.Context) (*Balance, error)

	// ─── WebSocket 구독 ───────────────────────────────────
	// WS 미지원 거래소: Base 기본 구현이 "not supported" 에러 반환.
	// WS 지원 거래소: Broker가 해당 메서드를 오버라이드.

	SubscribeTrade(codes ...string) error
	SubscribeQuote(codes ...string) error
	SubscribeExecution(htsID string) error
	UnsubscribeTrade(codes ...string) error
}
