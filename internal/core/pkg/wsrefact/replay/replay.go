package replay

import (
	"context"
	"errors"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact/common"
	storepkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact/replay/store"
	supervisorpkg "Custom-HTS/_legacy/internal/core/pkg/wsrefact/supervisor"

	"github.com/gorilla/websocket"
)

// WriteRequest reuses the supervisor-owned queued websocket write request.
type WriteRequest = supervisorpkg.WriteRequest

// Config configures the replaying websocket transport.
type Config struct {
	// Supervisor configures the reconnecting lower-level transport.
	Supervisor supervisorpkg.Config
}

// Replay wraps WAL-like subscription replay around a reconnect supervisor.
type Replay struct {
	// supervisor owns reconnect and live-session queueing.
	supervisor *supervisorpkg.Supervisor
	// store keeps WAL-like replay intents and desired state.
	store *storepkg.Store
	// wakeCh nudges the replay worker to inspect the store again.
	wakeCh chan struct{}
}

// New creates a replaying websocket transport that wraps a reconnect supervisor.
func New(config Config) (*Replay, error) {
	// transport owns replay policy and the WAL-like subscription store.
	transport := &Replay{
		store:  storepkg.New(),
		wakeCh: make(chan struct{}, 1),
	}

	// baseHooks preserves caller-provided supervisor hooks beneath replay wiring.
	baseHooks := config.Supervisor.Hooks
	config.Supervisor.Hooks = supervisorpkg.Hooks{
		OnStateChange: baseHooks.OnStateChange,
		OnEvent:       baseHooks.OnEvent,
		OnReady: func(attempt int) {
			transport.Wake()
			if baseHooks.OnReady != nil {
				baseHooks.OnReady(attempt)
			}
		},
	}

	// svc is the lower-level reconnect supervisor wrapped by this replay transport.
	svc, err := supervisorpkg.New(config.Supervisor)
	if err != nil {
		return nil, err
	}
	transport.supervisor = svc
	return transport, nil
}

// State returns the current reconnect supervisor lifecycle state.
func (replay *Replay) State() common.ConnState {
	if replay == nil || replay.supervisor == nil {
		return common.ConnStateStopped
	}
	return replay.supervisor.State()
}

// IsConnected reports whether the replay transport currently owns a live session.
func (replay *Replay) IsConnected() bool {
	return replay.State() == common.ConnStateConnected
}

// Subscriptions returns the logical replay key set after local pending intents.
func (replay *Replay) Subscriptions() []string {
	if replay == nil {
		return nil
	}
	return replay.store.LogicalKeys()
}

// Run starts the replay worker and owns the reconnect lifecycle until ctx is canceled.
func (replay *Replay) Run(ctx context.Context) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	go replay.runWorker(ctx)
	return replay.supervisor.Run(ctx)
}

// Wake nudges the replay worker to re-scan the WAL-like replay store.
func (replay *Replay) Wake() {
	if replay == nil {
		return
	}
	select {
	case replay.wakeCh <- struct{}{}:
	default:
	}
}

// Subscribe records one subscribe intent in the replay store and wakes the replay worker.
func (replay *Replay) Subscribe(ctx context.Context, key string, gen common.GenFunc, opts ...common.WriteOption) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	_ = ctx

	// options stores the normalized per-request websocket write settings.
	options := buildWriteOptions(opts...)
	request := buildReplayRequest(key, gen, options)
	if replay.store.RecordSubscribe(key, request) {
		replay.Wake()
	}
	return nil
}

// Unsubscribe records one unsubscribe intent in the replay store and wakes the replay worker.
func (replay *Replay) Unsubscribe(ctx context.Context, key string, gen common.GenFunc, opts ...common.WriteOption) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	_ = ctx

	// options stores the normalized per-request websocket write settings.
	options := buildWriteOptions(opts...)
	request := buildReplayRequest(key, gen, options)
	if replay.store.RecordUnsubscribe(key, request) {
		replay.Wake()
	}
	return nil
}

// Send writes one direct text websocket message through the active live session.
func (replay *Replay) Send(ctx context.Context, data []byte) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	return replay.supervisor.Send(ctx, data)
}

// SendBinary writes one direct binary websocket message through the active live session.
func (replay *Replay) SendBinary(ctx context.Context, data []byte) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	return replay.supervisor.SendBinary(ctx, data)
}

func (replay *Replay) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-replay.wakeCh:
			replay.drainPending(ctx)
		}
	}
}

func (replay *Replay) drainPending(ctx context.Context) {
	for {
		// op is the next pending WAL-like replay operation.
		op, ok := replay.store.PeekFront()
		if !ok {
			return
		}

		switch op.Kind {
		case storepkg.IntentSubscribe:
			if !replay.processSubscribe(ctx, op) {
				return
			}
		case storepkg.IntentUnsubscribe:
			if !replay.processUnsubscribe(ctx, op) {
				return
			}
		}
	}
}

func (replay *Replay) processSubscribe(ctx context.Context, op storepkg.IntentOp) bool {
	err := replay.supervisor.Enqueue(ctx, op.Request)
	if err == nil {
		replay.store.ConfirmSubscribe(op.Key, op.Request)
		return true
	}
	if isGeneratePayloadError(err) {
		replay.store.DiscardFront()
		return true
	}
	return false
}

func (replay *Replay) processUnsubscribe(ctx context.Context, op storepkg.IntentOp) bool {
	if op.Request.PayloadGen == nil && len(op.Request.Payload) == 0 {
		replay.store.ConfirmUnsubscribe(op.Key)
		return true
	}

	err := replay.supervisor.Enqueue(ctx, op.Request)
	if err == nil {
		replay.store.ConfirmUnsubscribe(op.Key)
		return true
	}
	if isGeneratePayloadError(err) {
		replay.store.ConfirmUnsubscribe(op.Key)
		return true
	}
	if errors.Is(err, common.ErrNotConnected) {
		return false
	}
	replay.store.ConfirmUnsubscribe(op.Key)
	return false
}

func buildWriteOptions(opts ...common.WriteOption) *common.WriteOptions {
	// options stores the final per-request websocket write settings.
	options := &common.WriteOptions{}
	options.Apply(opts...)
	return options
}

func buildReplayRequest(key string, gen common.GenFunc, options *common.WriteOptions) WriteRequest {
	return WriteRequest{
		Key:          key,
		PayloadGen:   gen,
		MessageType:  resolveMessageType(options.MessageType),
		WriteTimeout: options.WriteTimeout,
	}
}

func resolveMessageType(messageType int) int {
	if messageType > 0 {
		return messageType
	}
	return websocket.TextMessage
}

func isGeneratePayloadError(err error) bool {
	// opErr carries contextual websocket transport failures.
	var opErr *common.OperationError
	if !errors.As(err, &opErr) {
		return false
	}
	return opErr.Op == "generate payload"
}
