package exchange

import (
	"context"
	"strconv"
	"time"

	"Custom-HTS/internal/core/domain"
	"Custom-HTS/internal/core/pkg/logger"
	"Custom-HTS/internal/core/pkg/requester"
	"Custom-HTS/internal/core/service/config"
)

// Base holds common broker fields and default Exchange behavior.
type Base struct {
	AccountID string
	Broker    string
	BaseURL   string
	IsPaper   bool
	AssetType config.AssetType
	Config    config.AccountConfig
	Log       *logger.Logger
	Channels  *BrokerChannels
	*requester.Requester
}

func (b *Base) GetName() string                { return b.Broker }
func (b *Base) GetAssetType() config.AssetType { return b.AssetType }

func (b *Base) OnConnect() {
	b.Log.Info("ws connected", "isPaper", b.IsPaper)
	b.Channels.SendStatus(domain.StatusEvent{
		AccountID:  b.AccountID,
		Broker:     b.Broker,
		Status:     domain.StatusConnected,
		Message:    "websocket connected",
		ReceivedAt: time.Now(),
	})
}

func (b *Base) OnDisconnect(err error) {
	msg := "normal shutdown"
	if err != nil {
		msg = err.Error()
		b.Log.Warn("ws disconnected", "err", err)
	} else {
		b.Log.Info("ws disconnected (normal)")
	}
	b.Channels.SendStatus(domain.StatusEvent{
		AccountID:  b.AccountID,
		Broker:     b.Broker,
		Status:     domain.StatusDisconnected,
		Message:    msg,
		ReceivedAt: time.Now(),
	})
}

func (b *Base) OnReconnecting(attempt int) {
	b.Log.Info("ws reconnecting", "attempt", attempt)
	b.Channels.SendStatus(domain.StatusEvent{
		AccountID:  b.AccountID,
		Broker:     b.Broker,
		Status:     domain.StatusReconnecting,
		Message:    "attempt " + strconv.Itoa(attempt),
		ReceivedAt: time.Now(),
	})
}

func (b *Base) OnFatal(err error) {
	b.Log.Error("ws fatal, connection destroyed", "err", err)
	b.Channels.SendError(domain.ErrorEvent{
		AccountID:  b.AccountID,
		Broker:     b.Broker,
		Source:     b.Broker + "_ws",
		Code:       "FATAL",
		Message:    "connection destroyed",
		Err:        err,
		Fatal:      true,
		ReceivedAt: time.Now(),
	})
}

func (b *Base) Start(_ context.Context) {}

func (b *Base) GetPrice(_ context.Context, _ string) (*Price, error) {
	return nil, NewUnsupportedError(b.Broker, "GetPrice")
}

func (b *Base) GetOrderbook(_ context.Context, _ string) (*Orderbook, error) {
	return nil, NewUnsupportedError(b.Broker, "GetOrderbook")
}

func (b *Base) GetCandles(_ context.Context, _ string, _ string, _ int) ([]Candle, error) {
	return nil, NewUnsupportedError(b.Broker, "GetCandles")
}

func (b *Base) PlaceOrder(_ context.Context, _ OrderRequest) (*OrderResponse, error) {
	return nil, NewUnsupportedError(b.Broker, "PlaceOrder")
}

func (b *Base) CancelOrder(_ context.Context, _ string) error {
	return NewUnsupportedError(b.Broker, "CancelOrder")
}

func (b *Base) GetOpenOrders(_ context.Context) ([]Order, error) {
	return nil, NewUnsupportedError(b.Broker, "GetOpenOrders")
}

func (b *Base) GetOrderHistory(_ context.Context, _ HistoryOpts) ([]Order, error) {
	return nil, NewUnsupportedError(b.Broker, "GetOrderHistory")
}

func (b *Base) GetBalance(_ context.Context) (*Balance, error) {
	return nil, NewUnsupportedError(b.Broker, "GetBalance")
}

func (b *Base) SubscribeTrade(_ ...string) error {
	return NewUnsupportedError(b.Broker, "SubscribeTrade")
}

func (b *Base) SubscribeQuote(_ ...string) error {
	return NewUnsupportedError(b.Broker, "SubscribeQuote")
}

func (b *Base) SubscribeExecution(_ string) error {
	return NewUnsupportedError(b.Broker, "SubscribeExecution")
}

func (b *Base) UnsubscribeTrade(_ ...string) error {
	return NewUnsupportedError(b.Broker, "UnsubscribeTrade")
}
