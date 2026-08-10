package engine

import (
	"context"

	"Custom-HTS/internal/core/ioworker"
	"Custom-HTS/internal/core/pkg/logger"
)

// Worker — 블로킹 IO 전담 고루틴.
//
// 처리하는 IO:
//   - RefreshRequest → broker.GetBalance + broker.GetOpenOrders → BaselineResult
//   - ActionRequest  → broker.PlaceOrder → ActionResult
//
// 단일 고루틴으로 시작. 병목 발생 시 풀로 확장 가능(로드맵 미결 사항).
type Worker struct {
	ch  *ioworker.Channels
	log *logger.Logger
}

func NewWorker(ch *ioworker.Channels, log *logger.Logger) *Worker {
	return &Worker{
		ch:  ch,
		log: log,
	}
}

// Run — select 루프. ctx 취소 시 종료.
func (w *Worker) Run(ctx context.Context) {
	w.log.Info("io worker started")
	defer w.log.Info("io worker stopped")

	for {
		select {
		case <-ctx.Done():
			return

		case req := <-w.ch.Refreshes():
			w.handleRefresh(ctx, req)

		case req := <-w.ch.Actions():
			w.handleAction(ctx, req)
		}
	}
}

// handleRefresh — 잔고 + 미체결 조회 후 결과 반환.
//
// GetBalance 실패 시 에러만 반환하고 종료.
// GetOpenOrders 실패 시 Balance만이라도 반환 —
// Engine이 부분 결과라도 활용할 수 있도록.
func (w *Worker) handleRefresh(ctx context.Context, req ioworker.RefreshRequest) {
	w.log.Debug("refresh started", "account", req.AccountID)

	result := ioworker.BaselineResult{AccountID: req.AccountID}

	balance, err := req.Broker.GetBalance(ctx)
	if err != nil {
		w.log.Error("GetBalance failed", "account", req.AccountID, "err", err)
		result.Err = err
		w.ch.SendBaseline(result)
		return
	}
	result.Balance = balance

	orders, err := req.Broker.GetOpenOrders(ctx)
	if err != nil {
		w.log.Warn("GetOpenOrders failed, sending balance only",
			"account", req.AccountID,
			"err", err,
		)
	}
	result.Orders = orders

	w.ch.SendBaseline(result)
	w.log.Debug("refresh completed",
		"account", req.AccountID,
		"holdings", len(balance.Holdings),
		"openOrders", len(orders),
	)
}

// handleAction — 주문 실행 후 결과 반환.
func (w *Worker) handleAction(ctx context.Context, req ioworker.ActionRequest) {
	w.log.Info("placing order",
		"account", req.AccountID,
		"code", req.Order.Code,
		"side", req.Order.Side,
		"qty", req.Order.Quantity,
		"price", req.Order.Price,
	)

	resp, err := req.Broker.PlaceOrder(ctx, req.Order)
	if err != nil {
		w.log.Error("PlaceOrder failed",
			"account", req.AccountID,
			"code", req.Order.Code,
			"err", err,
		)
	}

	w.ch.SendResult(ioworker.ActionResult{
		AccountID: req.AccountID,
		Response:  resp,
		Err:       err,
	})
}
