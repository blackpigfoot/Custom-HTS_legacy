package subscription

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrSubNotFound = errors.New("subscription not found")
	ErrNoBroker    = errors.New("selectFn returned nil broker")
)

// Exchange — Manager가 구독/해제 호출에 사용하는 인터페이스.
// core/exchange.Exchange의 부분 집합. 순환 의존 방지.
type Exchange interface {
	Subscribe(sub Sub) error
	Unsubscribe(sub Sub) error
}

// SelectFn — 첫 구독 시 최적 broker를 선택하는 콜백.
//
// Manager.Subscribe의 mu.Lock 안에서 호출됨.
// 빠르게 반환해야 함 (lock 보유 시간 최소화).
//
// Stage 1~2: func(Sub) { return kisBroker, nil }
// Stage 5: SmartRouter.Select
type SelectFn func(sub Sub) (Exchange, error)

// info — Sub 하나에 대한 내부 상태.
type info struct {
	count  int
	broker Exchange // 이 구독을 현재 담당하는 broker
}

// Manager — 구독 참조 카운팅 + broker 호출 원자화.
//
// v4 Store 대비 핵심 변경:
//   - refcount 체크와 broker.Subscribe가 같은 lock 안에서 수행.
//     Acquire(판단)과 Subscribe(행동) 사이 gap에서 발생하던 TOCTOU 제거.
//   - info에 broker를 저장하여 Release 시 호출자가 broker를 몰라도 됨.
//   - selectFn 주입으로 SmartRouter를 호출 계층이 아닌 선택 로직으로 분리.
//
// 사용:
//
//	// Stage 1~2: broker 하나
//	mgr := NewManager(func(sub Sub) (Exchange, error) {
//	    return kisBroker, nil
//	})
//
//	// Stage 5: SmartRouter가 선택 로직 제공
//	mgr := NewManager(router.Select)
//
//	mgr.Subscribe(Sub{Code: "005930", Tr: SubTypeTrade})
//	// → 첫 구독: selectFn → broker 선택 → broker.Subscribe → entry 저장
//	// → 추가 구독: count++ 후 즉시 반환. broker 호출 없음.
type Manager struct {
	mu       sync.Mutex
	entry    map[Sub]*info
	selectFn SelectFn
}

func NewManager(selectFn SelectFn) *Manager {
	return &Manager{
		entry:    make(map[Sub]*info),
		selectFn: selectFn,
	}
}

// Subscribe — 구독 참조 추가.
//
// 이미 구독 중이면 count++ 후 반환. selectFn 호출 안 함.
// 첫 구독이면 selectFn → broker 선택 → broker.Subscribe → entry 저장.
// 모든 과정이 mu.Lock 안에서 수행되어 TOCTOU 불가.
//
// broker.Subscribe 내부는 WsConnection.cmdCh 전송 + resultCh 대기 (수 ms).
// 구독/해제 빈도가 낮아 lock 보유 시간 문제없음.
func (m *Manager) Subscribe(sub Sub) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e, ok := m.entry[sub]; ok {
		e.count++
		return nil
	}

	broker, err := m.selectFn(sub)
	if err != nil {
		return fmt.Errorf("select broker for %s: %w", sub, err)
	}
	if broker == nil {
		return fmt.Errorf("select broker for %s: %w", sub, ErrNoBroker)
	}

	if err := broker.Subscribe(sub); err != nil {
		return fmt.Errorf("subscribe %s: %w", sub, err)
	}

	m.entry[sub] = &info{count: 1, broker: broker}
	return nil
}

// Release — 구독 참조 해제.
//
// 마지막 구독자이면 info.broker.Unsubscribe 호출.
// 호출자는 어떤 broker가 담당 중인지 몰라도 됨 — info가 알고 있음.
//
// Unsubscribe 실패 시 count를 롤백하여 다음 Release에서 재시도 가능.
func (m *Manager) Release(sub Sub) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entry[sub]
	if !ok {
		return nil
	}

	e.count--
	if e.count > 0 {
		return nil
	}

	if err := e.broker.Unsubscribe(sub); err != nil {
		e.count++ // 롤백. 다음 Release에서 재시도.
		return fmt.Errorf("unsubscribe %s: %w", sub, err)
	}

	delete(m.entry, sub)
	return nil
}

// Swap — 구독 할당 broker 변경.
// SmartRouter가 계좌 장애/리밸런싱 시 호출.
//
// 새 broker에 먼저 구독 → 성공 후 기존 해제.
// 순간적 중복 수신 허용 — 데이터 유실보다 안전.
//
// old 해제 실패 시에도 info.broker는 newBroker로 교체.
// old broker가 이미 연결 끊긴 상황일 가능성 높음.
// 이후 Release가 올바른 broker에 요청하는 것이 더 중요.
func (m *Manager) Swap(sub Sub, newBroker Exchange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entry[sub]
	if !ok {
		return ErrSubNotFound
	}

	if e.broker == newBroker {
		return nil
	}

	if err := newBroker.Subscribe(sub); err != nil {
		return fmt.Errorf("swap subscribe %s: %w", sub, err)
	}

	// old 해제 실패는 로그만. newBroker로 교체가 우선.
	_ = e.broker.Unsubscribe(sub)
	e.broker = newBroker
	return nil
}

// SubsByBroker — 특정 broker가 담당하는 구독 목록.
// SmartRouter.OnBrokerDown에서 재분배 대상 조회에 사용.
func (m *Manager) SubsByBroker(broker Exchange) []Sub {
	m.mu.Lock()
	defer m.mu.Unlock()

	var subs []Sub
	for sub, e := range m.entry {
		if e.broker == broker {
			subs = append(subs, sub)
		}
	}
	return subs
}

// Count — 특정 구독의 현재 참조 수.
func (m *Manager) Count(sub Sub) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entry[sub]; ok {
		return e.count
	}
	return 0
}

// Has — 해당 Sub가 구독 중인지 확인.
func (m *Manager) Has(sub Sub) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.entry[sub]
	return ok
}

// Keys — 현재 구독 중인 Sub 목록.
func (m *Manager) Keys() []Sub {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]Sub, 0, len(m.entry))
	for sub := range m.entry {
		keys = append(keys, sub)
	}
	return keys
}
