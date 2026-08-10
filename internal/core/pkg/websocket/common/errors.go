package common

import (
	"errors"
	"strings"
)

var (
	// ErrURLRequired reports that the websocket endpoint URL was empty.
	ErrURLRequired = errors.New("websocket: URL is required")
	// ErrNotConnected reports that no live websocket session is available.
	ErrNotConnected = errors.New("websocket: connection not running")
	// ErrAlreadyRunning reports that Run was invoked more than once.
	ErrAlreadyRunning = errors.New("websocket: already running")
	// ErrResponseTimeout reports that a direct send waited too long for completion.
	ErrResponseTimeout = errors.New("websocket: response timeout")
	// ErrQueueFull reports that the direct send queue is full.
	ErrQueueFull = errors.New("websocket: send queue full")
)

// OperationError carries contextual transport operation failure information.
type OperationError struct {
	// Op is the logical websocket operation that failed.
	Op string
	// Err is the underlying failure.
	Err error
}

// Error formats the operation failure for callers.
func (err *OperationError) Error() string {
	var builder strings.Builder
	builder.WriteString("websocket")
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

// Unwrap returns the wrapped lower-level failure.
func (err *OperationError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}
