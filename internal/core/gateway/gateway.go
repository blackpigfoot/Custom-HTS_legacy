package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"Custom-HTS/internal/core/domain"
)

// BrokerFactory creates one domain-wrapped broker instance.
type BrokerFactory func(context.Context) (DomainBroker, error)

// BrokerSpec describes one broker account to be managed.
type BrokerSpec struct {
	// BrokerID is the unique managed broker identifier.
	BrokerID string
	// AccountID is the logical account identifier.
	AccountID string
	// BrokerType is the vendor type such as "ls".
	BrokerType string
	// Factory creates the domain-wrapped broker instance.
	Factory BrokerFactory
}

// ManagedBroker is one lifecycle-managed domain broker.
type ManagedBroker struct {
	// spec stores immutable registration metadata.
	spec BrokerSpec
	// broker is the active domain-wrapped broker instance.
	broker DomainBroker
	// status stores the current lifecycle state.
	status domain.BrokerStatus
	// cancel stops the broker-local context.
	cancel context.CancelFunc
}

// Gateway manages domain-wrapped broker instances and their lifecycles.
type Gateway struct {
	// mu guards broker map and lifecycle state transitions.
	mu sync.RWMutex
	// brokers stores managed brokers by BrokerID.
	brokers map[string]*ManagedBroker
	// events receives lifecycle messages for observers.
	events chan domain.LifecycleMsg
}

// New creates an empty lifecycle gateway.
func New() *Gateway {
	return &Gateway{
		brokers: make(map[string]*ManagedBroker),
		events:  make(chan domain.LifecycleMsg, 64),
	}
}

// Register creates and stores one managed broker instance.
func (g *Gateway) Register(ctx context.Context, spec BrokerSpec) error {
	if err := validateBrokerSpec(spec); err != nil {
		return err
	}

	g.mu.Lock()
	if _, exists := g.brokers[spec.BrokerID]; exists {
		g.mu.Unlock()
		return fmt.Errorf("broker already registered: %s", spec.BrokerID)
	}
	g.mu.Unlock()

	broker, err := spec.Factory(ctx)
	if err != nil {
		return fmt.Errorf("creating broker %s: %w", spec.BrokerID, err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.brokers[spec.BrokerID]; exists {
		return fmt.Errorf("broker already registered: %s", spec.BrokerID)
	}
	g.brokers[spec.BrokerID] = &ManagedBroker{
		spec:   spec,
		broker: broker,
		status: domain.BrokerStatusCreated,
	}
	g.publishLifecycleLocked(spec.BrokerID, spec.AccountID, domain.BrokerStatusCreated, "broker registered", nil)
	return nil
}

// Replace removes an existing broker and registers a newly created broker.
func (g *Gateway) Replace(ctx context.Context, spec BrokerSpec) error {
	if err := g.Remove(ctx, spec.BrokerID); err != nil && !errors.Is(err, ErrBrokerNotFound) {
		return err
	}
	return g.Register(ctx, spec)
}

// Start starts one managed broker instance.
func (g *Gateway) Start(ctx context.Context, brokerID string) error {
	g.mu.Lock()
	managed, ok := g.brokers[brokerID]
	if !ok {
		g.mu.Unlock()
		return ErrBrokerNotFound
	}
	if managed.status == domain.BrokerStatusRunning {
		g.mu.Unlock()
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	managed.cancel = cancel
	managed.status = domain.BrokerStatusStarting
	g.publishLifecycleLocked(brokerID, managed.spec.AccountID, domain.BrokerStatusStarting, "broker starting", nil)
	broker := managed.broker
	g.mu.Unlock()

	broker.Start(runCtx)

	g.mu.Lock()
	if current, ok := g.brokers[brokerID]; ok && current == managed {
		current.status = domain.BrokerStatusRunning
		g.publishLifecycleLocked(brokerID, managed.spec.AccountID, domain.BrokerStatusRunning, "broker running", nil)
	}
	g.mu.Unlock()
	return nil
}

// Remove stops and removes one managed broker instance.
func (g *Gateway) Remove(_ context.Context, brokerID string) error {
	g.mu.Lock()
	managed, ok := g.brokers[brokerID]
	if !ok {
		g.mu.Unlock()
		return ErrBrokerNotFound
	}
	managed.status = domain.BrokerStatusStopping
	g.publishLifecycleLocked(brokerID, managed.spec.AccountID, domain.BrokerStatusStopping, "broker stopping", nil)
	if managed.cancel != nil {
		managed.cancel()
	}
	delete(g.brokers, brokerID)
	g.publishLifecycleLocked(brokerID, managed.spec.AccountID, domain.BrokerStatusStopped, "broker removed", nil)
	g.mu.Unlock()
	return nil
}

// Status returns the lifecycle state for one managed broker.
func (g *Gateway) Status(brokerID string) (domain.BrokerStatus, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	managed, ok := g.brokers[brokerID]
	if !ok {
		return "", false
	}
	return managed.status, true
}

// Events returns lifecycle messages emitted by the gateway.
func (g *Gateway) Events() <-chan domain.LifecycleMsg {
	if g == nil {
		return nil
	}
	return g.events
}

// SubscribeTick delegates a tick subscription to a managed domain broker.
func (g *Gateway) SubscribeTick(ctx context.Context, brokerID string, code string) (*Subscription[domain.TickMsg], error) {
	broker, err := g.lookupBroker(brokerID)
	if err != nil {
		return nil, err
	}
	return broker.SubscribeTick(ctx, code)
}

// SubscribeQuote delegates a quote subscription to a managed domain broker.
func (g *Gateway) SubscribeQuote(ctx context.Context, brokerID string, code string) (*Subscription[domain.QuoteMsg], error) {
	broker, err := g.lookupBroker(brokerID)
	if err != nil {
		return nil, err
	}
	return broker.SubscribeQuote(ctx, code)
}

// SubscribeExec delegates an execution subscription to a managed domain broker.
func (g *Gateway) SubscribeExec(ctx context.Context, brokerID string, key string) (*Subscription[domain.ExecMsg], error) {
	broker, err := g.lookupBroker(brokerID)
	if err != nil {
		return nil, err
	}
	return broker.SubscribeExec(ctx, key)
}

func (g *Gateway) lookupBroker(brokerID string) (DomainBroker, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	managed, ok := g.brokers[brokerID]
	if !ok {
		return nil, ErrBrokerNotFound
	}
	return managed.broker, nil
}

func (g *Gateway) publishLifecycleLocked(brokerID string, accountID string, status domain.BrokerStatus, message string, err error) {
	event := domain.LifecycleMsg{
		BrokerID:   brokerID,
		AccountID:  accountID,
		Status:     status,
		Message:    message,
		Err:        err,
		ReceivedAt: time.Now(),
	}
	select {
	case g.events <- event:
	default:
	}
}

func validateBrokerSpec(spec BrokerSpec) error {
	if spec.BrokerID == "" {
		return errors.New("broker id is required")
	}
	if spec.BrokerType == "" {
		return errors.New("broker type is required")
	}
	if spec.Factory == nil {
		return errors.New("broker factory is required")
	}
	return nil
}
