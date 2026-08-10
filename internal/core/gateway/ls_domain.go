package gateway

import (
	"context"
	"errors"

	lsclient "Custom-HTS/internal/adapter/broker/ls/client"
	"Custom-HTS/internal/core/domain"
)

// LSDomainBroker wraps one LS SDK client and exposes domain-level streams.
type LSDomainBroker struct {
	// brokerID is the logical managed broker identifier.
	brokerID string
	// accountID is the logical account identifier.
	accountID string
	// sdk is the native LS client being wrapped.
	sdk *lsclient.Client
}

// NewLSDomainBroker creates a domain wrapper over an existing LS SDK client.
func NewLSDomainBroker(brokerID string, accountID string, sdk *lsclient.Client) *LSDomainBroker {
	return &LSDomainBroker{
		brokerID:  brokerID,
		accountID: accountID,
		sdk:       sdk,
	}
}

// NewLSDomainBrokerFromConfig creates an LS SDK client and wraps it.
func NewLSDomainBrokerFromConfig(brokerID string, cfg lsclient.Config) (*LSDomainBroker, error) {
	sdk, err := lsclient.New(cfg)
	if err != nil {
		return nil, err
	}
	return NewLSDomainBroker(brokerID, cfg.AccountID, sdk), nil
}

// NewLSBrokerSpec creates a lifecycle registration spec for one LS account.
func NewLSBrokerSpec(brokerID string, cfg lsclient.Config) BrokerSpec {
	return BrokerSpec{
		BrokerID:   brokerID,
		AccountID:  cfg.AccountID,
		BrokerType: lsclient.BrokerName,
		Factory: func(_ context.Context) (DomainBroker, error) {
			return NewLSDomainBrokerFromConfig(brokerID, cfg)
		},
	}
}

// BrokerType returns the wrapped broker type.
func (b *LSDomainBroker) BrokerType() string {
	return lsclient.BrokerName
}

// AccountID returns the logical account identifier.
func (b *LSDomainBroker) AccountID() string {
	if b == nil {
		return ""
	}
	return b.accountID
}

// Start starts the wrapped LS SDK client.
func (b *LSDomainBroker) Start(ctx context.Context) {
	if b == nil || b.sdk == nil {
		return
	}
	b.sdk.Start(ctx)
}

// SubscribeTick returns one independent domain tick channel for one issue code.
func (b *LSDomainBroker) SubscribeTick(ctx context.Context, code string) (*Subscription[domain.TickMsg], error) {
	if b == nil || b.sdk == nil {
		return nil, errors.New("ls domain broker sdk is nil")
	}
	nativeSub, err := b.sdk.SubscribeTrade(ctx, code)
	if err != nil {
		return nil, err
	}

	out := make(chan domain.TickMsg, defaultDomainSubscriptionBuf)
	done := make(chan struct{})
	go b.runLSTickStream(done, nativeSub.Channel(), out)

	return NewSubscription(out, func(ctx context.Context) error {
		close(done)
		return nativeSub.Close(ctx)
	}), nil
}

// SubscribeQuote returns one independent domain quote channel for one issue code.
func (b *LSDomainBroker) SubscribeQuote(ctx context.Context, code string) (*Subscription[domain.QuoteMsg], error) {
	if b == nil || b.sdk == nil {
		return nil, errors.New("ls domain broker sdk is nil")
	}
	nativeSub, err := b.sdk.SubscribeQuote(ctx, code)
	if err != nil {
		return nil, err
	}

	out := make(chan domain.QuoteMsg, defaultDomainSubscriptionBuf)
	done := make(chan struct{})
	go b.runLSQuoteStream(done, nativeSub.Channel(), out)

	return NewSubscription(out, func(ctx context.Context) error {
		close(done)
		return nativeSub.Close(ctx)
	}), nil
}

// SubscribeExec returns one independent domain execution channel for one HTS key.
func (b *LSDomainBroker) SubscribeExec(ctx context.Context, key string) (*Subscription[domain.ExecMsg], error) {
	if b == nil || b.sdk == nil {
		return nil, errors.New("ls domain broker sdk is nil")
	}
	nativeSub, err := b.sdk.SubscribeExecution(ctx, key)
	if err != nil {
		return nil, err
	}

	out := make(chan domain.ExecMsg, defaultDomainSubscriptionBuf)
	done := make(chan struct{})
	go b.runLSExecStream(done, nativeSub.Channel(), out)

	return NewSubscription(out, func(ctx context.Context) error {
		close(done)
		return nativeSub.Close(ctx)
	}), nil
}

func (b *LSDomainBroker) runLSTickStream(done <-chan struct{}, in <-chan lsclient.TradeMessage, out chan<- domain.TickMsg) {
	defer close(out)
	for {
		select {
		case <-done:
			return
		case msg, ok := <-in:
			if !ok {
				return
			}
			converted := b.convertLSTick(msg)
			select {
			case <-done:
				return
			case out <- converted:
			}
		}
	}
}

func (b *LSDomainBroker) runLSQuoteStream(done <-chan struct{}, in <-chan lsclient.QuoteMessage, out chan<- domain.QuoteMsg) {
	defer close(out)
	for {
		select {
		case <-done:
			return
		case msg, ok := <-in:
			if !ok {
				return
			}
			converted := b.convertLSQuote(msg)
			select {
			case <-done:
				return
			case out <- converted:
			}
		}
	}
}

func (b *LSDomainBroker) runLSExecStream(done <-chan struct{}, in <-chan lsclient.ExecutionMessage, out chan<- domain.ExecMsg) {
	defer close(out)
	for {
		select {
		case <-done:
			return
		case msg, ok := <-in:
			if !ok {
				return
			}
			converted := b.convertLSExec(msg)
			select {
			case <-done:
				return
			case out <- converted:
			}
		}
	}
}
