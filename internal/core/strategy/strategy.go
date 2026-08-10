package strategy

import (
	"Custom-HTS/internal/core/domain"
)

// Strategy — 전략 판단 인터페이스.
//
// Engine이 이벤트 수신 시 동기 호출.
// TradeSignal 슬라이스 반환 → Engine이 각 신호를 ActionRequest로 변환하여 IO Worker에 전달.
// nil 또는 빈 슬라이스 반환 시 아무 동작 없음.
//
// 구현체(strategy 패키지)는 pricestore, ordertracker, balancemgr를 읽기 참조하여 판단.
// Engine 내부에서만 호출되므로 구현체 내부 락 불필요.
type Strategy interface {
	OnTick(tick domain.TickEvent) []*domain.TradeSignal
	OnExec(exec domain.ExecEvent) []*domain.TradeSignal
}

// NoopStrategy — 아무 동작 없는 기본 전략. 테스트용.
type NoopStrategy struct{}

func (s *NoopStrategy) OnTick(_ domain.TickEvent) []*domain.TradeSignal { return nil }
func (s *NoopStrategy) OnExec(_ domain.ExecEvent) []*domain.TradeSignal { return nil }
