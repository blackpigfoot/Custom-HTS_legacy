package wsrefact_v5

import (
	"errors"
	"strings"
)

// Errors returned by the websocket transport.
var (
	// ErrURLRequired reports that the websocket endpoint URL was empty.
	ErrURLRequired = errors.New("ws: URL is required")
	// ErrAlreadyStarted reports that a lower runtime was started more than once.
	ErrAlreadyStarted = errors.New("ws: already started")
	// ErrAlreadyRunning reports that Client.Run was invoked more than once.
	ErrAlreadyRunning = errors.New("ws: already running")
	// ErrNotConnected reports that no live websocket transport is available.
	ErrNotConnected = errors.New("ws: connection not running")
	// ErrBackoff reports that the transport is reconnecting or write-blocked.
	ErrBackoff = errors.New("ws: transport in backoff")
	// ErrAlreadySubscribed reports that Subscribe saw an already registered key.
	ErrAlreadySubscribed = errors.New("ws: already subscribed")
	// ErrNotSubscribed reports that Unsubscribe saw an unknown key.
	ErrNotSubscribed = errors.New("ws: not subscribed")
	// ErrClientStopped reports that the client is not running anymore.
	ErrClientStopped = errors.New("ws: client stopped")
	// ErrQueueFull reports that one internal queue could not accept more work.
	ErrQueueFull = errors.New("ws: queue full")
	// ErrHandlerContinue reports that a handler error should be surfaced but the session should continue.
	ErrHandlerContinue = errors.New("ws: handler continue")
	// ErrHandlerBackoff reports that a handler error should transition the transport into reconnect/backoff.
	ErrHandlerBackoff = errors.New("ws: handler backoff")
	// ErrHandlerStop reports that a handler error should stop the websocket pipeline permanently.
	ErrHandlerStop = errors.New("ws: handler stop")
)

// errInvalidOptions reports that SubscribeOptions had missing generators.
var errInvalidOptions = errors.New("ws: SubGen and UnsubGen are required")

// OperationError carries contextual websocket operation failure information.
type OperationError struct {
	// Op is the logical websocket operation that failed.
	Op string
	// Err is the wrapped lower-level failure.
	Err error
}

// Error formats the operation failure for callers.
func (err *OperationError) Error() string {
	// builder accumulates the final diagnostic string.
	var builder strings.Builder
	builder.WriteString("ws")
	if err.Op != "" {
		builder.WriteString(": ")
		builder.WriteString(err.Op)
	}
	if err.Err != nil {
		builder.WriteString(": ")
		builder.WriteString(err.Err.Error())
	}
	return builder.String()
}

// Unwrap returns the wrapped lower-level failure for errors.Is and errors.As traversal.
func (err *OperationError) Unwrap() error {
	return err.Err
}
