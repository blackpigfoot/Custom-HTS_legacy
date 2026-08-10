package exchange

import "Custom-HTS/internal/core/domain"

// BrokerChannels — 브로커가 Engine에 데이터를 올려보내는 채널 묶음.
//
// 모든 브로커가 동일한 BrokerChannels 인스턴스를 공유.
// 브로커 추가/제거 시 채널은 유지되고 브로커만 교체.
//
// 사용 패턴:
//   - Broker(sender):  bc.SendTick(event), bc.SendExec(event) ...
//   - Engine(receiver): select { case t := <-bc.Ticks(): ... }
//
// Send 메서드는 버퍼 초과 시 블로킹.
// Engine 처리 속도(~2M/sec)가 실제 부하(~1K/sec)를 크게 상회하므로
// 정상 운영에서 블로킹되지 않음.
type BrokerChannels struct {
	tick   chan domain.TickEvent
	exec   chan domain.ExecEvent
	quote  chan domain.QuoteEvent // nil이면 SendQuote가 drop. Stage 3에서 연결.
	status chan domain.StatusEvent
	err    chan domain.ErrorEvent
}

const defaultBrokerChBuf = 256

// NewBrokerChannels — 필수 채널 생성. quote는 nil(Stage 3 보류).
func NewBrokerChannels() *BrokerChannels {
	return &BrokerChannels{
		tick:   make(chan domain.TickEvent, defaultBrokerChBuf),
		exec:   make(chan domain.ExecEvent, defaultBrokerChBuf),
		status: make(chan domain.StatusEvent, 16), // 연결 상태 변경은 저빈도
		err:    make(chan domain.ErrorEvent, 16),  // 에러도 저빈도
	}
}

// EnableQuote — quote 채널 활성화. Stage 3에서 호출.
// 이미 활성화된 상태에서 재호출하면 기존 채널을 반환(중복 생성 방지).
func (bc *BrokerChannels) EnableQuote() {
	if bc.quote == nil {
		bc.quote = make(chan domain.QuoteEvent, defaultBrokerChBuf)
	}
}

// SendTick — 체결 이벤트를 Engine에 전달.
func (bc *BrokerChannels) SendTick(e domain.TickEvent) {
	bc.tick <- e
}

// SendExec — 체결통보 이벤트를 Engine에 전달.
func (bc *BrokerChannels) SendExec(e domain.ExecEvent) {
	bc.exec <- e
}

// SendQuote — 호가 이벤트를 Engine에 전달.
// quote 채널 미연결(nil) 시 silent drop. Stage 3 전까지 정상 동작.
func (bc *BrokerChannels) SendQuote(e domain.QuoteEvent) {
	if bc.quote == nil {
		return
	}
	bc.quote <- e
}

// SendStatus — 연결 상태 변경을 Engine에 전달.
func (bc *BrokerChannels) SendStatus(e domain.StatusEvent) {
	bc.status <- e
}

// SendError — 에러 이벤트를 Engine에 전달.
func (bc *BrokerChannels) SendError(e domain.ErrorEvent) {
	bc.err <- e
}

// Ticks — Engine select용 수신 채널.
func (bc *BrokerChannels) Ticks() <-chan domain.TickEvent { return bc.tick }

// Execs — Engine select용 수신 채널.
func (bc *BrokerChannels) Execs() <-chan domain.ExecEvent { return bc.exec }

// Quotes — Engine select용 수신 채널. nil 가능 (Stage 2에서는 select에 포함하지 않음).
func (bc *BrokerChannels) Quotes() <-chan domain.QuoteEvent { return bc.quote }

// Statuses — Engine select용 수신 채널.
func (bc *BrokerChannels) Statuses() <-chan domain.StatusEvent { return bc.status }

// Errors — Engine select용 수신 채널.
func (bc *BrokerChannels) Errors() <-chan domain.ErrorEvent { return bc.err }
