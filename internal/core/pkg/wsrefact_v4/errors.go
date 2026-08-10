package wsrefact_v4

import (
	"errors"
	"strings"
)

// Errors returned by Client.
var (
	// ErrURLRequired reports that the websocket endpoint URL was empty.
	ErrURLRequired = errors.New("ws: URL is required")
	// ErrAlreadyStarted reports that serve was invoked more than once on
	// the same session.
	ErrAlreadyStarted = errors.New("ws: already started")
	// ErrNotConnected reports that no live websocket session is
	// available. Returned when Send / Subscribe / Unsubscribe is
	// invoked while the client is dialing, replaying, or stopped.
	ErrNotConnected = errors.New("ws: connection not running")
	// ErrAlreadyRunning reports that Run was invoked more than once on
	// the same client.
	ErrAlreadyRunning = errors.New("ws: already running")
	// ErrAlreadySubscribed reports that Subscribe was invoked with a
	// key that is already registered.
	ErrAlreadySubscribed = errors.New("ws: already subscribed")
	// ErrNotSubscribed reports that Unsubscribe was invoked with a key
	// that is not registered.
	ErrNotSubscribed = errors.New("ws: not subscribed")
	// ErrClientStopped reports that an operation could not be queued
	// because the client has stopped.
	ErrClientStopped = errors.New("ws: client stopped")
)

// errInvalidOptions reports that SubscribeOptions had a missing
// required field. Internal — Subscribe surfaces this directly.
var errInvalidOptions = errors.New("ws: SubGen and UnsubGen are required")

// OperationError carries contextual transport operation failure
// information. Returned by Send and other write operations to
// identify which low-level step failed.
type OperationError struct {
	// Op is the logical websocket operation that failed.
	Op string
	// Err is the wrapped lower-level failure.
	Err error
}

// Error formats the operation failure for callers.
func (err *OperationError) Error() string {
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

// Unwrap returns the wrapped lower-level failure for errors.Is and
// errors.As traversal.
func (err *OperationError) Unwrap() error {
	return err.Err
}
