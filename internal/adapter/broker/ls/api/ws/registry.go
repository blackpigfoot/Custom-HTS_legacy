package ws

import (
	"context"
	"sync"
	"time"

	apierr "Custom-HTS/internal/adapter/broker/ls/api/common/error"
	corews "Custom-HTS/internal/core/pkg/websocket"
	"Custom-HTS/internal/core/pkg/wsrefact_v6"
)

// connRegistry owns websocket connection slot placement and route ownership.
type connRegistry struct {
	// mu guards slots, routeOwners, and idle timer transitions.
	mu sync.Mutex
	// ctx is the registry-owned lifecycle context for all running slots.
	ctx context.Context
	// cancel stops the registry-owned lifecycle context.
	cancel context.CancelFunc
	// handlers receives messages and status events from managed slots.
	handlers connHandlers
	// started reports whether the registry has started its slot run loops.
	started bool
	// closed reports whether the registry lifecycle has permanently ended.
	closed bool
	// factory creates raw reconnecting websocket transports for slots.
	factory *connFactory
	// slots stores websocket connection slots managed by this registry.
	slots []*connSlot
	// routeOwners maps each local route key to the connection slot that owns it.
	routeOwners map[string]*connSlot
	// maxConnSlots limits how many websocket connection slots can be active.
	maxConnSlots int
	// maxRoutesPerSlot limits how many routes one connection slot can own.
	maxRoutesPerSlot int
	// routeFillStep is the per-round route count target before the next slot opens.
	routeFillStep int
	// idleSlotTTL is the duration an empty slot remains open for reuse.
	idleSlotTTL time.Duration
}

// connHandlers forwards slot outputs to the realtime service without giving the registry parser responsibility.
type connHandlers struct {
	// OnMessage handles one websocket payload delivered by a connection slot.
	//
	// OnMessage is called synchronously from the underlying websocket read loop.
	// The data slice is valid only until OnMessage returns.
	OnMessage func(connID string, data []byte)
	// OnStatus handles one websocket lifecycle event delivered by a connection slot.
	//
	// OnStatus is called synchronously from the underlying websocket lifecycle.
	OnStatus func(connID string, status corews.ConnEvent)
}

// slotAcquireResult reports the slot chosen for a route placement request.
type slotAcquireResult struct {
	// slot is the connection slot selected for the route.
	slot *connSlot
	// existed reports whether the route was already owned by the slot.
	existed bool
}

func newConnRegistry(settings Dependencies) (*connRegistry, error) {
	// factory creates each raw websocket connection on demand.
	factory := newConnFactory(settings.Auth, settings.ConnConfig)

	return &connRegistry{
		factory:          factory,
		slots:            make([]*connSlot, 0),
		routeOwners:      make(map[string]*connSlot),
		maxConnSlots:     settings.MaxConnSlots,
		maxRoutesPerSlot: settings.MaxRoutesPerSlot,
		routeFillStep:    settings.RouteFillStep,
		idleSlotTTL:      settings.IdleSlotTTL,
	}, nil
}

func (registry *connRegistry) start(ctx context.Context, handlers connHandlers) {
	registry.mu.Lock()
	if registry.started || registry.closed {
		registry.mu.Unlock()
		return
	}

	registry.ctx, registry.cancel = context.WithCancel(ctx)
	registry.handlers = handlers
	registry.started = true

	if len(registry.slots) == 0 {
		// slot is the initial primary connection slot created when the registry first starts.
		_, _ = registry.createSlotLocked()
	}

	// starts stores prepared slot launches so goroutines begin outside the registry lock.
	starts := make([]slotStart, 0, len(registry.slots))
	for _, slot := range registry.slots {
		if start, ok := registry.prepareSlotStartLocked(slot); ok {
			starts = append(starts, start)
		}
	}
	lifecycleCtx := registry.ctx
	registry.mu.Unlock()

	for _, start := range starts {
		registry.launchSlot(start)
	}
	go registry.shutdownOnContextDone(lifecycleCtx)
}

func (registry *connRegistry) acquire(key string) (slotAcquireResult, error) {
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return slotAcquireResult{}, ErrRealtimeClosed
	}

	if slot, ok := registry.routeOwners[key]; ok {
		registry.mu.Unlock()
		return slotAcquireResult{slot: slot, existed: true}, nil
	}

	slot, created, err := registry.selectSlotLocked()
	if err != nil {
		registry.mu.Unlock()
		return slotAcquireResult{}, err
	}
	slot.cancelIdleTimer()
	slot.addRoute(key)
	registry.routeOwners[key] = slot

	// start prepares a newly created slot only when the registry is already running.
	var start slotStart
	var shouldLaunch bool
	if created {
		start, shouldLaunch = registry.prepareSlotStartLocked(slot)
	}
	registry.mu.Unlock()

	if shouldLaunch {
		registry.launchSlot(start)
	}
	return slotAcquireResult{slot: slot}, nil
}

func (registry *connRegistry) rollback(key string, slot *connSlot) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if registry.routeOwners[key] != slot {
		return
	}
	delete(registry.routeOwners, key)
	slot.removeRoute(key)
	registry.markSlotIdleLocked(slot)
}

func (registry *connRegistry) release(key string, slot *connSlot) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if registry.routeOwners[key] != slot {
		return
	}
	delete(registry.routeOwners, key)
	slot.removeRoute(key)
	registry.markSlotIdleLocked(slot)
}

func (registry *connRegistry) owner(key string) (*connSlot, bool, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if registry.closed {
		return nil, false, ErrRealtimeClosed
	}
	slot, ok := registry.routeOwners[key]
	return slot, ok, nil
}

func (registry *connRegistry) firstConn() *wsrefact_v6.Client {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if len(registry.slots) == 0 {
		return nil
	}
	return registry.slots[0].client
}

func (registry *connRegistry) shutdown() {
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return
	}
	registry.closed = true
	if registry.cancel != nil {
		registry.cancel()
	}

	// slots is a stable snapshot of slots to stop after lifecycle state changes.
	slots := append([]*connSlot(nil), registry.slots...)
	registry.slots = nil
	registry.routeOwners = make(map[string]*connSlot)
	registry.mu.Unlock()

	for _, slot := range slots {
		slot.stop()
	}
}

func (registry *connRegistry) selectSlotLocked() (*connSlot, bool, error) {
	for ceiling := registry.routeFillStep; ceiling < registry.maxRoutesPerSlot; ceiling += registry.routeFillStep {
		for _, slot := range registry.slots {
			if slot.routeCount() < ceiling && slot.routeCount() < registry.maxRoutesPerSlot {
				return slot, false, nil
			}
		}
		if len(registry.slots) < registry.maxConnSlots {
			slot, err := registry.createSlotLocked()
			return slot, true, err
		}
	}
	for _, slot := range registry.slots {
		if slot.routeCount() < registry.maxRoutesPerSlot {
			return slot, false, nil
		}
	}
	if len(registry.slots) < registry.maxConnSlots {
		slot, err := registry.createSlotLocked()
		return slot, true, err
	}
	return nil, false, ErrWSConnectionLimit
}

func (registry *connRegistry) createSlotLocked() (*connSlot, error) {
	// slot is the next websocket connection slot created by the factory.
	slot, err := registry.factory.newSlot()
	if err != nil {
		return nil, &apierr.OperationError{
			Op:  "ls create websocket connection",
			Err: err,
		}
	}

	registry.slots = append(registry.slots, slot)
	return slot, nil
}

func (registry *connRegistry) markSlotIdleLocked(slot *connSlot) {
	if slot.routeCount() > 0 {
		return
	}
	if registry.closed {
		slot.stop()
		return
	}
	slot.cancelIdleTimer()
	if registry.idleSlotTTL <= 0 {
		registry.removeSlotLocked(slot)
		slot.stop()
		return
	}

	// timer closes the empty slot only if it is still idle when the TTL expires.
	var timer *time.Timer
	timer = time.AfterFunc(registry.idleSlotTTL, func() {
		registry.closeIdleSlot(slot, timer)
	})
	slot.idleTimer = timer
}

func (registry *connRegistry) closeIdleSlot(slot *connSlot, timer *time.Timer) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if registry.closed || slot.idleTimer != timer || slot.routeCount() > 0 {
		return
	}
	slot.idleTimer = nil
	registry.removeSlotLocked(slot)
	slot.stop()
}

func (registry *connRegistry) removeSlotLocked(slot *connSlot) {
	for i, existing := range registry.slots {
		if existing != slot {
			continue
		}
		copy(registry.slots[i:], registry.slots[i+1:])
		registry.slots[len(registry.slots)-1] = nil
		registry.slots = registry.slots[:len(registry.slots)-1]
		return
	}
}

// slotStart is one prepared slot launch with an immutable context and handlers snapshot.
type slotStart struct {
	// slot is the connection slot to launch.
	slot *connSlot
	// ctx is the slot-local context.
	ctx context.Context
}

func (registry *connRegistry) prepareSlotStartLocked(slot *connSlot) (slotStart, bool) {
	if !registry.started || registry.closed || slot.ctx != nil {
		return slotStart{}, false
	}

	// ctx is derived from the registry lifecycle and canceled when this slot closes.
	ctx, cancel := context.WithCancel(registry.ctx)
	slot.ctx = ctx
	slot.cancel = cancel
	slot.setHandlers(registry.handlers)
	return slotStart{
		slot: slot,
		ctx:  ctx,
	}, true
}

func (registry *connRegistry) launchSlot(start slotStart) {
	go start.slot.run(start.ctx)
}

func (registry *connRegistry) shutdownOnContextDone(ctx context.Context) {
	<-ctx.Done()
	registry.shutdown()
}
