package ioworker

import (
	"Custom-HTS/internal/core/exchange"
)

// Channels — Engine ↔ IO Worker 간 채널 묶음.
//
// 사용 패턴:
//
//	Engine(sender):     ch.SendRefresh(req), ch.SendAction(req)
//	Worker(receiver):   select { case r := <-ch.Refreshes(): ... }
//	Worker(sender):     ch.SendBaseline(result), ch.SendResult(result)
//	Engine(receiver):   select { case b := <-ch.Baselines(): ... }
type Channels struct {
	refresh  chan RefreshRequest
	action   chan ActionRequest
	baseline chan BaselineResult
	result   chan ActionResult
}

const defaultBuf = 64

// NewChannels — IO 채널 생성.
// 버퍼가 BrokerChannels(256)보다 작은 이유: IO 요청은 저빈도(폴링/주문).
func NewChannels() *Channels {
	return &Channels{
		refresh:  make(chan RefreshRequest, defaultBuf),
		action:   make(chan ActionRequest, defaultBuf),
		baseline: make(chan BaselineResult, defaultBuf),
		result:   make(chan ActionResult, defaultBuf),
	}
}

// SendRefresh — Engine이 IO Worker에 잔고/미체결 조회 요청.
func (c *Channels) SendRefresh(r RefreshRequest) { c.refresh <- r }

// SendAction — Engine이 IO Worker에 주문 실행 요청.
func (c *Channels) SendAction(a ActionRequest) { c.action <- a }

// SendBaseline — IO Worker가 REST 조회 결과를 Engine에 반환.
func (c *Channels) SendBaseline(b BaselineResult) { c.baseline <- b }

// SendResult — IO Worker가 주문 실행 결과를 Engine에 반환.
func (c *Channels) SendResult(r ActionResult) { c.result <- r }

// Refreshes — Worker select용 수신 채널.
func (c *Channels) Refreshes() <-chan RefreshRequest { return c.refresh }

// Actions — Worker select용 수신 채널.
func (c *Channels) Actions() <-chan ActionRequest { return c.action }

// Baselines — Engine select용 수신 채널.
func (c *Channels) Baselines() <-chan BaselineResult { return c.baseline }

// Results — Engine select용 수신 채널.
func (c *Channels) Results() <-chan ActionResult { return c.result }

// RefreshRequest — Engine → Worker: 잔고/미체결 조회 요청.
type RefreshRequest struct {
	AccountID string
	Broker    exchange.Exchange
}

// ActionRequest — Engine → Worker: 주문 실행 요청.
type ActionRequest struct {
	AccountID string
	Broker    exchange.Exchange
	Order     exchange.OrderRequest
}

// BaselineResult — Worker → Engine: 잔고/미체결 조회 결과.
type BaselineResult struct {
	AccountID string
	Balance   *exchange.Balance
	Orders    []exchange.Order
	Err       error
}

// ActionResult — Worker → Engine: 주문 실행 결과.
type ActionResult struct {
	AccountID string
	Response  *exchange.OrderResponse
	Err       error
}
