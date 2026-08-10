package replay

import (
	"context"
	"time"

	"Custom-HTS/internal/core/pkg/websocket/common"
	"Custom-HTS/internal/core/pkg/websocket/supervisor"

	"github.com/gorilla/websocket"
)

// WriteIntent stores the replay metadata for one subscribe or unsubscribe write.
type WriteIntent struct {
	// Gen builds the websocket payload when the live session needs it.
	Gen common.GenFunc
	// WriteTimeout overrides the default websocket write timeout when non-zero.
	WriteTimeout time.Duration
	// MessageType overrides the default websocket message type when non-zero.
	MessageType int
}

// Config configures the replaying websocket transport.
type Config struct {
	// Supervisor configures the reconnecting lower-level transport.
	Supervisor supervisor.Config
}

// Replay wraps replayable subscription intent handling around a reconnect supervisor.
type Replay struct {
	// supervisor owns reconnect and live-session lifecycle.
	supervisor *supervisor.Supervisor
	// store keeps replayable write intents and their logical desired state.
	store *intentStore
}

// New creates a replaying websocket transport that wraps a reconnect supervisor.
func New(config Config) (*Replay, error) {
	// transport owns replay policy and the WAL-like subscription store.
	transport := &Replay{
		store: newIntentStore(),
	}

	// baseHooks preserves caller-provided supervisor hooks beneath replay wiring.
	baseHooks := config.Supervisor.Hooks
	config.Supervisor.Hooks = supervisor.Hooks{
		OnStateChange: baseHooks.OnStateChange,
		OnEvent:       baseHooks.OnEvent,
		OnReady: func(handle supervisor.Handle, attempt int) {
			transport.onReady(handle)
			if baseHooks.OnReady != nil {
				baseHooks.OnReady(handle, attempt)
			}
		},
	}

	// svc is the lower-level reconnect supervisor wrapped by this replay transport.
	svc, err := supervisor.New(config.Supervisor)
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
	return replay.store.logicalKeys()
}

// Run owns the reconnect lifecycle until ctx is canceled.
func (replay *Replay) Run(ctx context.Context) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	return replay.supervisor.Run(ctx)
}

// Subscribe records one replayable subscribe intent and wakes the live session when one exists.
func (replay *Replay) Subscribe(ctx context.Context, key string, gen common.GenFunc, opts ...common.SubOption) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	_ = ctx

	// options stores the normalized per-intent websocket write settings.
	options := buildSubOptions(opts...)
	changed := replay.store.recordSubscribe(key, buildWriteIntent(gen, options))
	if changed {
		replay.wake(replay.supervisor.Active())
	}
	return nil
}

// Unsubscribe records one replayable unsubscribe intent and wakes the live session when one exists.
func (replay *Replay) Unsubscribe(ctx context.Context, key string, gen common.GenFunc, opts ...common.SubOption) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	_ = ctx

	// options stores the normalized per-intent websocket write settings.
	options := buildSubOptions(opts...)
	changed := replay.store.recordUnsubscribe(key, buildWriteIntent(gen, options))
	if changed {
		replay.wake(replay.supervisor.Active())
	}
	return nil
}

// Send writes a text message through the active live session and waits for completion.
func (replay *Replay) Send(ctx context.Context, data []byte) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	return replay.supervisor.Send(ctx, data, websocket.TextMessage)
}

// SendBinary writes a binary message through the active live session and waits for completion.
func (replay *Replay) SendBinary(ctx context.Context, data []byte) error {
	if replay == nil {
		return common.ErrNotConnected
	}
	return replay.supervisor.Send(ctx, data, websocket.BinaryMessage)
}

func (replay *Replay) onReady(handle supervisor.Handle) {
	replay.store.resetForReconnect()
	handle.StartWriter(func() {
		replay.drainPending(handle)
	})
}

func (replay *Replay) wake(handle supervisor.Handle) {
	if handle == nil {
		return
	}
	handle.WakeWriter()
}

func (replay *Replay) drainPending(handle supervisor.Handle) {
	for {
		if handle.Context().Err() != nil {
			return
		}

		// op is the next replayable write intent pending local delivery.
		op, ok := replay.store.peekFront()
		if !ok {
			return
		}

		switch op.kind {
		case intentSubscribe:
			if err := replay.processSubscribe(handle, op); err != nil {
				return
			}
		case intentUnsubscribe:
			if err := replay.processUnsubscribe(handle, op); err != nil {
				return
			}
		}
	}
}

func (replay *Replay) processSubscribe(handle supervisor.Handle, op intentOp) error {
	// payload is the generated websocket subscribe packet for this live session.
	payload, err := op.intent.Gen(handle.Context())
	if err != nil {
		replay.store.discardFront()
		return nil
	}

	messageType := resolveMessageType(op.intent.MessageType)
	if err := handle.WritePayload(payload, messageType, op.intent.WriteTimeout); err != nil {
		return err
	}

	replay.store.confirmSubscribe(op.key, op.intent)
	return nil
}

func (replay *Replay) processUnsubscribe(handle supervisor.Handle, op intentOp) error {
	if op.intent.Gen == nil {
		replay.store.confirmUnsubscribe(op.key)
		return nil
	}

	// payload is the generated websocket unsubscribe packet for this live session.
	payload, err := op.intent.Gen(handle.Context())
	if err != nil {
		replay.store.confirmUnsubscribe(op.key)
		return nil
	}

	messageType := resolveMessageType(op.intent.MessageType)
	if err := handle.WritePayload(payload, messageType, op.intent.WriteTimeout); err != nil {
		replay.store.confirmUnsubscribe(op.key)
		return err
	}

	replay.store.confirmUnsubscribe(op.key)
	return nil
}

func buildSubOptions(opts ...common.SubOption) *common.SubOptions {
	// options stores the final per-intent websocket write settings.
	options := &common.SubOptions{}
	options.Apply(opts...)
	return options
}

func buildWriteIntent(gen common.GenFunc, options *common.SubOptions) WriteIntent {
	return WriteIntent{
		Gen:          gen,
		WriteTimeout: options.WriteTimeout,
		MessageType:  options.MessageType,
	}
}

func resolveMessageType(messageType int) int {
	if messageType > 0 {
		return messageType
	}
	return websocket.TextMessage
}
