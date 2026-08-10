// Package engine — 단일 select 루프. 앱의 중심 파이프라인.
//
// 채널 이벤트를 직렬 처리하여 "코드 순서 = 실행 순서" 보장.
// IO는 절대 수행하지 않음 — ioworker 패키지에 채널로 요청만 보냄.
// 상태 관리는 pricestore, ordertracker, balancemgr에 위임.
package engine

import (
	"context"
	"time"

	"Custom-HTS/internal/core/balancemgr"
	"Custom-HTS/internal/core/domain"
	"Custom-HTS/internal/core/exchange"
	"Custom-HTS/internal/core/ioworker"
	"Custom-HTS/internal/core/ordertracker"
	"Custom-HTS/internal/core/pkg/logger"
	"Custom-HTS/internal/core/pricestore"
	"Custom-HTS/internal/core/strategy"
)

// Engine — 단일 select 루프.
//
// 이벤트 처리 순서:
//
//	tick     → pricestore.Update → Strategy.OnTick → (Action) → IO 주문 요청
//	exec     → ordertracker.Apply → balancemgr.Apply → Strategy.OnExec
//	status   → connected → IO에 베이스라인 요청
//	error    → 로깅 + (fatal) 계좌 비활성화
//	baseline → balancemgr.SetBaseline + ordertracker.Reconcile
//	result   → ordertracker.Register
//	poll     → 전 계좌 IO에 베이스라인 요청
type Engine struct {
	broker *exchange.BrokerChannels
	io     *ioworker.Channels

	prices   *pricestore.Store
	orders   *ordertracker.Tracker
	balances *balancemgr.Manager
	strategy strategy.Strategy

	// 계좌 레지스트리 — IO Worker가 REST 호출할 때 broker 인스턴스 필요.
	accounts map[string]exchange.Exchange

	pollInterval time.Duration
	dryRun       bool
	log          *logger.Logger
}

// Config — Engine 생성 설정.
type Config struct {
	BrokerCh     *exchange.BrokerChannels
	IOCh         *ioworker.Channels
	Strategy     strategy.Strategy
	PollInterval time.Duration // 0이면 폴링 비활성화
	DryRun       bool
	Log          *logger.Logger
}

func New(cfg Config) *Engine {
	log := cfg.Log
	return &Engine{
		broker:       cfg.BrokerCh,
		io:           cfg.IOCh,
		prices:       pricestore.New(),
		orders:       ordertracker.New(log),
		balances:     balancemgr.New(log),
		strategy:     cfg.Strategy,
		accounts:     make(map[string]exchange.Exchange),
		pollInterval: cfg.PollInterval,
		dryRun:       cfg.DryRun,
		log:          log,
	}
}

// RegisterAccount — 계좌 등록. Run 전에 호출.
func (e *Engine) RegisterAccount(accountID string, broker exchange.Exchange) {
	e.accounts[accountID] = broker
}

// 하위 모듈 접근자 — Strategy 구현체가 생성 시 참조를 받기 위해 사용.
func (e *Engine) Prices() *pricestore.Store     { return e.prices }
func (e *Engine) Orders() *ordertracker.Tracker { return e.orders }
func (e *Engine) Balances() *balancemgr.Manager { return e.balances }

// SetStrategy — Run 전에 전략 교체.
// Engine 생성 후 Prices/Orders/Balances 참조를 전략에 주입한 뒤 호출.
func (e *Engine) SetStrategy(s strategy.Strategy) { e.strategy = s }

// Run — select 루프 진입. ctx 취소 시 종료.
func (e *Engine) Run(ctx context.Context) {
	e.log.Info("engine started",
		"accounts", len(e.accounts),
		"pollInterval", e.pollInterval,
		"dryRun", e.dryRun,
	)
	defer e.log.Info("engine stopped")

	var pollCh <-chan time.Time
	if e.pollInterval > 0 {
		ticker := time.NewTicker(e.pollInterval)
		pollCh = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case tick := <-e.broker.Ticks():
			e.handleTick(tick)

		case exec := <-e.broker.Execs():
			e.handleExec(exec)

		case status := <-e.broker.Statuses():
			e.handleStatus(status)

		case errEvt := <-e.broker.Errors():
			e.handleError(errEvt)

		case baseline := <-e.io.Baselines():
			e.handleBaseline(baseline)

		case result := <-e.io.Results():
			e.handleResult(result)

		case <-pollCh:
			e.handlePoll()
		}
	}
}

func (e *Engine) handleTick(tick domain.TickEvent) {
	e.prices.Update(tick)

	for _, sig := range e.strategy.OnTick(tick) {
		e.dispatchSignal(sig)
	}
}

func (e *Engine) handleExec(exec domain.ExecEvent) {
	e.orders.Apply(exec)
	e.balances.Apply(exec)

	for _, sig := range e.strategy.OnExec(exec) {
		e.dispatchSignal(sig)
	}
}

func (e *Engine) handleStatus(status domain.StatusEvent) {
	e.log.Info("connection status changed",
		"account", status.AccountID,
		"status", status.Status,
		"msg", status.Message,
	)

	if status.Status == domain.StatusConnected {
		broker, ok := e.accounts[status.AccountID]
		if !ok {
			e.log.Error("unknown account in status event", "account", status.AccountID)
			return
		}
		e.io.SendRefresh(ioworker.RefreshRequest{
			AccountID: status.AccountID,
			Broker:    broker,
		})
	}
}

func (e *Engine) handleError(errEvt domain.ErrorEvent) {
	if errEvt.Fatal {
		e.log.Error("fatal error from broker",
			"account", errEvt.AccountID,
			"source", errEvt.Source,
			"code", errEvt.Code,
			"msg", errEvt.Message,
			"err", errEvt.Err,
		)
		// TODO: 계좌 비활성화 로직. Stage 5 다계좌에서 구체화.
		return
	}

	e.log.Warn("broker error",
		"account", errEvt.AccountID,
		"source", errEvt.Source,
		"code", errEvt.Code,
		"msg", errEvt.Message,
	)
}

func (e *Engine) handleBaseline(baseline ioworker.BaselineResult) {
	if baseline.Err != nil {
		e.log.Error("baseline refresh failed",
			"account", baseline.AccountID,
			"err", baseline.Err,
		)
		return
	}

	if baseline.Balance != nil {
		e.balances.SetBaseline(baseline.AccountID, baseline.Balance)
	}
	if baseline.Orders != nil {
		e.orders.Reconcile(baseline.AccountID, baseline.Orders)
	}
}

func (e *Engine) handleResult(result ioworker.ActionResult) {
	if result.Err != nil {
		e.log.Error("order placement failed",
			"account", result.AccountID,
			"err", result.Err,
		)
		return
	}

	e.orders.Register(result.AccountID, result.Response)
}

func (e *Engine) handlePoll() {
	for accountID, broker := range e.accounts {
		e.io.SendRefresh(ioworker.RefreshRequest{
			AccountID: accountID,
			Broker:    broker,
		})
	}
}

// dispatchSignal — TradeSignal → ActionRequest 변환 후 IO Worker에 전달.
//
// Strategy는 "무엇을 거래할지"만 결정.
// "어떤 계좌/브로커로"는 이 함수가 결정:
//   - signal.AccountID가 지정되어 있으면 해당 계좌 사용
//   - 비어 있으면 기본 계좌(첫 번째 등록 계좌) 사용
func (e *Engine) dispatchSignal(sig *domain.TradeSignal) {
	if sig == nil {
		return
	}

	accountID, broker := e.resolveAccount(sig.AccountID)
	if broker == nil {
		e.log.Error("no broker found for signal",
			"accountID", sig.AccountID,
			"code", sig.Code,
		)
		return
	}

	if e.dryRun {
		e.log.Info("[DRY RUN] would place order",
			"account", accountID,
			"code", sig.Code,
			"side", sig.Side,
			"qty", sig.Quantity,
			"price", sig.Price,
			"reason", sig.Reason,
		)
		return
	}

	e.io.SendAction(ioworker.ActionRequest{
		AccountID: accountID,
		Broker:    broker,
		Order: exchange.OrderRequest{
			Code:     sig.Code,
			Side:     exchange.Side(sig.Side),
			Type:     exchange.OrderType(sig.Type),
			Quantity: sig.Quantity,
			Price:    sig.Price,
		},
	})

	e.log.Info("order dispatched",
		"account", accountID,
		"code", sig.Code,
		"side", sig.Side,
		"qty", sig.Quantity,
		"reason", sig.Reason,
	)
}

// resolveAccount — 계좌 ID로 브로커 조회. 빈 문자열이면 첫 번째 등록 계좌 반환.
func (e *Engine) resolveAccount(accountID string) (string, exchange.Exchange) {
	if accountID != "" {
		if broker, ok := e.accounts[accountID]; ok {
			return accountID, broker
		}
		return accountID, nil
	}
	// 기본 계좌: 첫 번째 등록 계좌 (Stage 2에서는 단일 계좌)
	for id, broker := range e.accounts {
		return id, broker
	}
	return "", nil
}
