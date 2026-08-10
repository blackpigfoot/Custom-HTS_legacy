package gateway

import (
	"context"

	"Custom-HTS/internal/core/domain"
)

// DomainBroker is one broker SDK instance wrapped into domain-level streams.
type DomainBroker interface {
	// BrokerType returns the vendor type such as "ls".
	BrokerType() string
	// AccountID returns the logical account identifier.
	AccountID() string
	// Start starts the wrapped broker SDK.
	Start(ctx context.Context)
	// SubscribeTick returns an independent converted tick channel.
	SubscribeTick(ctx context.Context, code string) (*Subscription[domain.TickMsg], error)
	// SubscribeQuote returns an independent converted quote channel.
	SubscribeQuote(ctx context.Context, code string) (*Subscription[domain.QuoteMsg], error)
	// SubscribeExec returns an independent converted execution channel.
	SubscribeExec(ctx context.Context, key string) (*Subscription[domain.ExecMsg], error)
}
