package wsrefact_v6

import (
	stdErrors "errors"
	"strings"
)

// Errors returned by the websocket mux and worker runtime.
var (
	// ErrURLRequired reports that the websocket endpoint URL was empty.
	ErrURLRequired = stdErrors.New("ws: URL is required")
	// ErrDialerRequired reports that one worker configuration omitted the websocket dialer.
	ErrDialerRequired = stdErrors.New("ws: dialer is required")
	// ErrWorkersRequired reports that the mux was created without any worker configuration.
	ErrWorkersRequired = stdErrors.New("ws: mux workers are required")
	// ErrInvalidQueueSize reports that one configured command queue size was not positive.
	ErrInvalidQueueSize = stdErrors.New("ws: queue size must be positive")
	// ErrInvalidWriteTimeout reports that one configured write timeout was not positive.
	ErrInvalidWriteTimeout = stdErrors.New("ws: write timeout must be positive")
	// ErrMuxStopped reports that the upper multiplexer is no longer accepting new work.
	ErrMuxStopped = stdErrors.New("ws: mux stopped")
	// ErrNotConnected reports that no live websocket connection is currently available.
	ErrNotConnected = stdErrors.New("ws: not connected")
	// ErrReconnecting reports that the transport worker is reconnecting and temporarily rejecting live writes.
	ErrReconnecting = stdErrors.New("ws: reconnecting")
	// ErrQueueFull reports that the internal mux or worker queue could not accept more work.
	ErrQueueFull = stdErrors.New("ws: queue full")
	// ErrSubscriptionExists reports that one logical subscription key is already registered.
	ErrSubscriptionExists = stdErrors.New("ws: subscription exists")
	// ErrSubscriptionNotFound reports that one logical subscription key is not registered.
	ErrSubscriptionNotFound = stdErrors.New("ws: subscription not found")
	// ErrMuxCapacityReached reports that the placement policy could not place one more logical subscription.
	ErrMuxCapacityReached = stdErrors.New("ws: mux capacity reached")
	// ErrPlacementRejected reports that the placement policy declined to choose one worker.
	ErrPlacementRejected = stdErrors.New("ws: placement rejected")
)

// Internal panic-only configuration and programming errors.
var (
	// errUnknownCommand reports that the worker received one unsupported command type.
	errUnknownCommand = stdErrors.New("ws: unknown io command")
	// errCommandChannelRequired reports that the worker was created without one command channel.
	errCommandChannelRequired = stdErrors.New("ws: command channel is required")
	// errEventSinkRequired reports that the worker was created without one internal event sink.
	errEventSinkRequired = stdErrors.New("ws: event sink is required")
	// errWriteEncoderRequired reports that the worker was created without one shared write encoder.
	errWriteEncoderRequired = stdErrors.New("ws: write encoder is required")
	// errDialContextRequired reports that one connect command arrived without one upper-layer cancellation context.
	errDialContextRequired = stdErrors.New("ws: dial context is required")
	// errWriteIntentRequired reports that a write command arrived without one logical write intent.
	errWriteIntentRequired = stdErrors.New("ws: write intent is required")
	// errSubscriptionRequired reports that one live caller-visible write arrived without one completion handle.
	errSubscriptionRequired = stdErrors.New("ws: subscription is required")
	// errEncodedMessageTypeRequired reports that the shared encoder produced one invalid websocket opcode.
	errEncodedMessageTypeRequired = stdErrors.New("ws: encoded message type is required")
)

// OperationError carries contextual websocket operation failure information.
type OperationError struct {
	// Op stores the logical websocket operation that failed.
	Op string
	// Err stores the wrapped lower-level failure.
	Err error
}

// EncodeWriteError reports that one logical write intent could not be encoded into one websocket frame.
type EncodeWriteError struct {
	// Err stores the wrapped lower-level encoder failure.
	Err error
}

// Error formats the encode failure for callers.
func (err *EncodeWriteError) Error() string {
	if err == nil || err.Err == nil {
		return "ws: encode write"
	}
	return "ws: encode write: " + err.Err.Error()
}

// Unwrap returns the wrapped lower-level encoder failure for errors.Is and errors.As traversal.
func (err *EncodeWriteError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// Error formats the operation failure for callers.
func (err *OperationError) Error() string {
	// builder accumulates one readable websocket failure string.
	var builder strings.Builder
	builder.WriteString("ws")
	if err != nil && err.Op != "" {
		builder.WriteString(": ")
		builder.WriteString(err.Op)
	}
	if err != nil && err.Err != nil {
		builder.WriteString(": ")
		builder.WriteString(err.Err.Error())
	}
	return builder.String()
}

// Unwrap returns the wrapped lower-level failure for errors.Is and errors.As traversal.
func (err *OperationError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}
