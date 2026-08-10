// Package ordertracker — 주문번호 기반 FSM 상태 추적기.
//
// Engine이 이벤트별로 동기 호출. 자체 Run 루프/락 없음.
//
// 상태 전이:
//
//	PlaceOrder 결과 → Register(StatusOpen)
//	체결통보 접수     → Apply → StatusOpen (이미 Open이면 무시)
//	체결통보 부분체결  → Apply → StatusPartial, FilledQty 갱신
//	체결통보 완전체결  → Apply → StatusFilled, FilledQty 갱신
//	체결통보 거부     → Apply → StatusRejected
//	REST 동기화      → Reconcile → 누락 주문 추가, 완료 주문 제거
package ordertracker

import (
	"math"
	"time"

	"Custom-HTS/internal/core/domain"
	"Custom-HTS/internal/core/exchange"
	"Custom-HTS/internal/core/pkg/logger"
)

// Tracker — 주문 상태 추적기.
type Tracker struct {
	orders map[string]*TrackedOrder // orderID → order
	log    *logger.Logger
}

// TrackedOrder — 추적 중인 주문 하나의 상태.
type TrackedOrder struct {
	OrderID   string
	AccountID string
	Broker    string
	Code      string
	Side      domain.Side
	Price     int64
	Quantity  int64 // 주문총수량
	FilledQty int64 // 누적 체결수량
	RemainQty int64 // 미체결수량
	Status    domain.OrderStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

func New(log *logger.Logger) *Tracker {
	return &Tracker{
		orders: make(map[string]*TrackedOrder),
		log:    log,
	}
}

// Register — PlaceOrder 성공 시 새 주문 등록.
// IO Worker의 ActionResult에서 호출.
func (t *Tracker) Register(accountID string, resp *exchange.OrderResponse) {
	if resp == nil {
		return
	}

	now := time.Now()
	t.orders[resp.OrderID] = &TrackedOrder{
		OrderID:   resp.OrderID,
		AccountID: accountID,
		Code:      resp.Code,
		Side:      convertSide(resp.Side),
		Price:     resp.Price,
		Quantity:  resp.Quantity,
		RemainQty: resp.Quantity,
		Status:    domain.StatusOpen,
		CreatedAt: resp.CreatedAt,
		UpdatedAt: now,
	}

	t.log.Info("order registered",
		"orderID", resp.OrderID,
		"code", resp.Code,
		"side", resp.Side,
		"qty", resp.Quantity,
	)
}

// Apply — 체결통보(ExecEvent) 수신 시 상태 전이.
//
// 멱등 처리: 이미 Filled/Cancelled/Rejected인 주문은 무시.
// 미등록 주문은 WS가 REST보다 빠를 때 발생 → 신규 등록 후 적용.
func (t *Tracker) Apply(exec domain.ExecEvent) {
	order, exists := t.orders[exec.OrderID]
	if !exists {
		// WS 체결통보가 REST PlaceOrder 응답보다 먼저 도착한 경우.
		order = &TrackedOrder{
			OrderID:   exec.OrderID,
			AccountID: exec.AccountID,
			Broker:    exec.Broker,
			Code:      exec.Code,
			Side:      exec.Side,
			Quantity:  exec.TotalQty,
			CreatedAt: exec.ExecTime,
		}
		t.orders[exec.OrderID] = order
		t.log.Info("order registered from exec notification",
			"orderID", exec.OrderID,
			"code", exec.Code,
		)
	}

	if isTerminal(order.Status) {
		t.log.Debug("ignoring exec for terminal order",
			"orderID", exec.OrderID,
			"status", order.Status,
		)
		return
	}

	newStatus := mapExecStatus(exec.Status)
	order.Status = newStatus
	order.FilledQty = exec.TotalQty - exec.RemainQty
	order.RemainQty = exec.RemainQty
	if exec.Price > 0 {
		order.Price = exec.Price
	}
	order.UpdatedAt = exec.ReceivedAt

	t.log.Info("order updated",
		"orderID", exec.OrderID,
		"status", newStatus,
		"filled", order.FilledQty,
		"remain", order.RemainQty,
	)
}

// Reconcile — REST GetOpenOrders 결과로 일괄 동기화.
//
// REST 스냅샷이 진실의 원천 — 추적 목록을 스냅샷 기준으로 보정.
//   - 스냅샷에 있고 추적에 없으면 → 추가 (disconnect 중 접수된 주문)
//   - 추적에 있고 스냅샷에 없으면 → 체결/취소 완료로 간주, 제거
//
// exchange.Order의 Quantity/FilledQty/Price가 float64인 상태.
// int64 통일 마이그레이션 전까지 경계 변환(roundToInt64)으로 처리.
func (t *Tracker) Reconcile(accountID string, orders []exchange.Order) {
	snapshot := make(map[string]bool, len(orders))

	for _, o := range orders {
		snapshot[o.OrderID] = true
		if _, exists := t.orders[o.OrderID]; !exists {
			qty := roundToInt64(o.Quantity)
			filled := roundToInt64(o.FilledQty)
			t.orders[o.OrderID] = &TrackedOrder{
				OrderID:   o.OrderID,
				AccountID: accountID,
				Code:      o.Code,
				Side:      convertSide(o.Side),
				Price:     roundToInt64(o.Price),
				Quantity:  qty,
				FilledQty: filled,
				RemainQty: qty - filled,
				Status:    convertOrderStatus(o.Status),
				CreatedAt: o.CreatedAt,
				UpdatedAt: time.Now(),
			}
			t.log.Info("order added from reconcile",
				"orderID", o.OrderID,
				"code", o.Code,
			)
		}
	}

	for id, order := range t.orders {
		if order.AccountID != accountID {
			continue
		}
		if !snapshot[id] && !isTerminal(order.Status) {
			t.log.Info("order removed by reconcile (no longer open)",
				"orderID", id,
				"lastStatus", order.Status,
			)
			delete(t.orders, id)
		}
	}
}

// GetOrder — 주문 조회. 미추적 주문은 nil.
func (t *Tracker) GetOrder(orderID string) *TrackedOrder {
	return t.orders[orderID]
}

// GetOpenOrders — 미체결 주문 목록. Strategy에서 중복 주문 방지 등에 사용.
func (t *Tracker) GetOpenOrders(accountID string) []*TrackedOrder {
	var result []*TrackedOrder
	for _, o := range t.orders {
		if o.AccountID == accountID && !isTerminal(o.Status) {
			result = append(result, o)
		}
	}
	return result
}

// HasOpenOrder — 해당 종목에 미체결 주문이 있는지 확인.
// accountID가 빈 문자열이면 전 계좌 대상으로 검색.
func (t *Tracker) HasOpenOrder(accountID, code string) bool {
	for _, o := range t.orders {
		if code != o.Code || isTerminal(o.Status) {
			continue
		}
		if accountID == "" || o.AccountID == accountID {
			return true
		}
	}
	return false
}

func isTerminal(s domain.OrderStatus) bool {
	switch s {
	case domain.StatusFilled, domain.StatusCancelled, domain.StatusRejected:
		return true
	}
	return false
}

// mapExecStatus — 체결통보 Status 문자열 → domain.OrderStatus.
//
// KIS 파서(ws_parser.go)가 출력하는 문자열:
//
//	"accepted" → StatusOpen
//	"filled"   → StatusFilled
//	"partial"  → StatusPartial
//	"rejected" → StatusRejected
func mapExecStatus(raw string) domain.OrderStatus {
	switch raw {
	case "accepted":
		return domain.StatusOpen
	case "filled":
		return domain.StatusFilled
	case "partial":
		return domain.StatusPartial
	case "rejected":
		return domain.StatusRejected
	default:
		return domain.OrderStatus(raw)
	}
}

func convertSide(s exchange.Side) domain.Side {
	switch s {
	case exchange.Buy:
		return domain.Buy
	case exchange.Sell:
		return domain.Sell
	default:
		return domain.Side(s)
	}
}

func convertOrderStatus(s exchange.OrderStatus) domain.OrderStatus {
	switch s {
	case exchange.StatusOpen:
		return domain.StatusOpen
	case exchange.StatusFilled:
		return domain.StatusFilled
	case exchange.StatusPartial:
		return domain.StatusPartial
	case exchange.StatusCancelled:
		return domain.StatusCancelled
	case exchange.StatusRejected:
		return domain.StatusRejected
	default:
		return domain.OrderStatus(s)
	}
}

// roundToInt64 — exchange.Order의 float64 필드 → int64 경계 변환.
// KRW 가격/수량은 정수이므로 반올림으로 충분.
func roundToInt64(f float64) int64 {
	return int64(math.Round(f))
}
