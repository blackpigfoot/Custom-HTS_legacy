// Package balancemgr — 계좌별 잔고 관리.
//
// Engine이 이벤트별로 동기 호출. 자체 Run 루프/락 없음.
//
// 동작 모델:
//  1. SetBaseline(REST 스냅샷) → 전체 교체
//  2. Apply(체결통보) → 증분 반영
//  3. 주기적 폴링으로 SetBaseline 재호출 → 누적 오차 자동 보정
package balancemgr

import (
	"time"

	"Custom-HTS/internal/core/domain"
	"Custom-HTS/internal/core/exchange"
	"Custom-HTS/internal/core/pkg/logger"
)

// Manager — 계좌별 잔고 관리.
type Manager struct {
	accounts map[string]*AccountBalance // accountID → balance
	log      *logger.Logger
}

// AccountBalance — 계좌 하나의 잔고 상태.
type AccountBalance struct {
	AccountID   string
	CashBalance int64                    // 예수금
	TotalAsset  int64                    // 총평가금액
	Holdings    map[string]*HoldingState // code → 보유 상태
	BaselineAt  time.Time                // 마지막 SetBaseline 시점
}

// HoldingState — 종목 하나의 보유 상태.
type HoldingState struct {
	Code     string
	Name     string
	Quantity int64 // 보유수량
	AvgCost  int64 // 매입평균가 (KRW)
}

func New(log *logger.Logger) *Manager {
	return &Manager{
		accounts: make(map[string]*AccountBalance),
		log:      log.With("component", "BalanceManager"),
	}
}

// SetBaseline — REST GetBalance 결과로 전체 교체.
//
// 재연결 후 또는 폴링 주기마다 IO Worker가 조회한 결과를 Engine이 전달.
// 이전 Apply 결과를 모두 덮어쓰므로 누적 오차 자동 보정.
func (m *Manager) SetBaseline(accountID string, bal *exchange.Balance) {
	if bal == nil {
		return
	}

	holdings := make(map[string]*HoldingState, len(bal.Holdings))
	for _, h := range bal.Holdings {
		holdings[h.Code] = &HoldingState{
			Code:     h.Code,
			Name:     h.Name,
			Quantity: h.Quantity,
			AvgCost:  h.AvgCost,
		}
	}

	m.accounts[accountID] = &AccountBalance{
		AccountID:   accountID,
		CashBalance: bal.CashBalance,
		TotalAsset:  bal.TotalAsset,
		Holdings:    holdings,
		BaselineAt:  time.Now(),
	}

	m.log.Info("baseline set",
		"account", accountID,
		"cash", bal.CashBalance,
		"holdings", len(bal.Holdings),
	)
}

// Apply — 체결통보(ExecEvent) 수신 시 증분 반영.
//
// 체결(filled/partial)만 잔고에 영향. accepted/rejected는 무시.
// 매수 체결: 예수금 감소, 보유수량 증가, 매입평균가 재계산.
// 매도 체결: 예수금 증가, 보유수량 감소.
// 정밀한 수수료/세금 반영은 하지 않음 — 다음 베이스라인에서 보정.
func (m *Manager) Apply(exec domain.ExecEvent) {
	if !isExecFill(exec.Status) {
		return
	}

	acct, ok := m.accounts[exec.AccountID]
	if !ok {
		// 베이스라인 수립 전 체결통보 도착. 다음 SetBaseline이 정확한 상태를 가져옴.
		m.log.Warn("exec received before baseline",
			"account", exec.AccountID,
			"orderID", exec.OrderID,
		)
		return
	}

	amount := exec.Price * exec.Quantity

	switch exec.Side {
	case domain.Buy:
		acct.CashBalance -= amount
		h := m.getOrCreateHolding(acct, exec.Code)
		oldTotal := h.AvgCost * h.Quantity
		h.Quantity += exec.Quantity
		if h.Quantity > 0 {
			h.AvgCost = (oldTotal + amount) / h.Quantity
		}

	case domain.Sell:
		acct.CashBalance += amount
		if h, exists := acct.Holdings[exec.Code]; exists {
			h.Quantity -= exec.Quantity
			if h.Quantity <= 0 {
				delete(acct.Holdings, exec.Code)
			}
		}
	}

	m.log.Debug("balance applied",
		"account", exec.AccountID,
		"side", exec.Side,
		"code", exec.Code,
		"qty", exec.Quantity,
		"price", exec.Price,
		"cash", acct.CashBalance,
	)
}

// GetBalance — 계좌 잔고 조회. 미수립 계좌는 nil.
func (m *Manager) GetBalance(accountID string) *AccountBalance {
	return m.accounts[accountID]
}

// GetHolding — 특정 종목 보유 상태 조회. 미보유면 nil.
// accountID가 빈 문자열이면 전 계좌에서 검색 (첫 매칭 반환).
func (m *Manager) GetHolding(accountID, code string) *HoldingState {
	if accountID != "" {
		acct, ok := m.accounts[accountID]
		if !ok {
			return nil
		}
		return acct.Holdings[code]
	}
	for _, acct := range m.accounts {
		if h, ok := acct.Holdings[code]; ok {
			return h
		}
	}
	return nil
}

// FindHoldingWithAccount — 종목 보유 검색 + 계좌 ID 함께 반환.
// Strategy에서 매도 신호 생성 시 accountID가 필요할 때 사용.
func (m *Manager) FindHoldingWithAccount(code string) (accountID string, holding *HoldingState) {
	for id, acct := range m.accounts {
		if h, ok := acct.Holdings[code]; ok {
			return id, h
		}
	}
	return "", nil
}

// GetCash — 예수금 조회. 미수립 계좌는 0.
func (m *Manager) GetCash(accountID string) int64 {
	acct, ok := m.accounts[accountID]
	if !ok {
		return 0
	}
	return acct.CashBalance
}

// HasBaseline — 해당 계좌의 베이스라인이 수립되었는지 확인.
func (m *Manager) HasBaseline(accountID string) bool {
	_, ok := m.accounts[accountID]
	return ok
}

func (m *Manager) getOrCreateHolding(acct *AccountBalance, code string) *HoldingState {
	h, exists := acct.Holdings[code]
	if !exists {
		h = &HoldingState{Code: code}
		acct.Holdings[code] = h
	}
	return h
}

// isExecFill — 잔고에 영향을 주는 체결 상태인지 판단.
// ordertracker.mapExecStatus와 동일한 KIS 파서 출력 문자열 기준.
func isExecFill(status string) bool {
	return status == "filled" || status == "partial"
}
