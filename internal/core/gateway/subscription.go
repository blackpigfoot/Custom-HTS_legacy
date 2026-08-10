package gateway

import (
	"context"
	"sync"
)

const defaultDomainSubscriptionBuf = 64

// Subscription owns one domain-level stream returned to a caller.
type Subscription[T any] struct {
	// ch delivers converted domain messages to this caller.
	ch <-chan T
	// closeFn releases the underlying native broker subscription.
	closeFn func(context.Context) error
	// closeOnce makes Close idempotent for callers.
	closeOnce sync.Once
	// closeErr stores the first close result.
	closeErr error
}

// NewSubscription creates a domain-level subscription handle.
func NewSubscription[T any](ch <-chan T, closeFn func(context.Context) error) *Subscription[T] {
	return &Subscription[T]{
		ch:      ch,
		closeFn: closeFn,
	}
}

// Channel returns the receive-only domain message stream.
func (s *Subscription[T]) Channel() <-chan T {
	if s == nil {
		return nil
	}
	return s.ch
}

// Close releases this subscription and its native broker subscription.
func (s *Subscription[T]) Close(ctx context.Context) error {
	if s == nil || s.closeFn == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.closeErr = s.closeFn(ctx)
	})
	return s.closeErr
}
