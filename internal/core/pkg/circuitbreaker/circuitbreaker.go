package circuitbreaker

import "sync/atomic"

// Breaker — 연속 실패 감지 및 임계값 초과 시 액션 트리거.
//
// 용도:
//   - WS 파싱 연속 실패 → EventBus 에러 발행, SyncManager 트리거 (Stage 2)
//   - REST 연속 실패 → 재연결 또는 알림
//
// 동시성:
//   atomic.Int64, atomic.Bool만 사용하므로 뮤텍스 없이 고루틴 안전.
//   onOpen/onClose 콜백 자체의 동시성은 호출부 책임.
//
// 상태 전이:
//   초기(Closed) → Fail() 누적 → 임계값 도달 → Open (onOpen 1회 호출)
//   Open → Success() → Closed (onClose 1회 호출)
type Breaker struct {
	threshold int64
	count     atomic.Int64
	opened    atomic.Bool

	// onOpen — 임계값 초과 시 1회 호출.
	// count: 임계값 도달 시점의 누적 실패 수 (로그/메트릭용).
	onOpen func(count int64)

	// onClose — 열림 상태에서 성공으로 복구될 때 1회 호출.
	// nil이면 no-op.
	onClose func()
}

// New — Breaker 생성.
//
// threshold: 연속 실패 허용 횟수. 이 횟수를 넘으면 onOpen 호출.
// onOpen:    임계값 초과 시 액션 (에러 발행, 동기화 트리거 등). nil 불가.
// onClose:   복구 시 액션 (로그, 상태 초기화 등). nil 허용.
func New(threshold int, onOpen func(count int64), onClose func()) *Breaker {
	return &Breaker{
		threshold: int64(threshold),
		onOpen:    onOpen,
		onClose:   onClose,
	}
}

// Fail — 실패 1건 기록.
//
// 이미 열린 상태(opened=true)이면 count 증가를 막음.
// 닫힌 상태에서 임계값 도달 시 onOpen을 정확히 1회 호출.
func (b *Breaker) Fail() {
	// 이미 열린 상태면 count 추가 증가 불필요.
	if b.opened.Load() {
		return
	}

	count := b.count.Add(1)
	if count >= b.threshold {
		// CAS로 onOpen 중복 호출 방지.
		// 여러 고루틴이 동시에 임계값을 넘어도 1회만 호출됨.
		if b.opened.CompareAndSwap(false, true) {
			b.onOpen(count)
		}
	}
}

// Success — 성공 1건 기록. 카운터 리셋.
//
// 열린 상태였으면 닫힘으로 전환하고 onClose 호출.
func (b *Breaker) Success() {
	b.count.Store(0)
	if b.opened.CompareAndSwap(true, false) {
		if b.onClose != nil {
			b.onClose()
		}
	}
}

// IsOpen — 현재 열림(차단) 상태 여부.
func (b *Breaker) IsOpen() bool {
	return b.opened.Load()
}

// Count — 현재 연속 실패 횟수.
func (b *Breaker) Count() int64 {
	return b.count.Load()
}
