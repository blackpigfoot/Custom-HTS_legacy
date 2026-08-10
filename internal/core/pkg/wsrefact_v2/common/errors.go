package common

import (
	"errors"
	"strings"
)

var (
	// ErrURLRequired reports that the websocket endpoint URL was empty.
	ErrURLRequired = errors.New("wsrefact: URL is required")
	// ErrNotConnected reports that no live websocket session is available.
	ErrNotConnected = errors.New("wsrefact: connection not running")
	// ErrAlreadyRunning reports that Run was invoked more than once.
	ErrAlreadyRunning = errors.New("wsrefact: already running")
	// ErrResponseTimeout reports that a queued write waited too long for completion.
	ErrResponseTimeout = errors.New("wsrefact: response timeout")
	// ErrQueueFull reports that the session request queue is full.
	ErrQueueFull = errors.New("wsrefact: request queue full")
)

// OperationError carries contextual transport operation failure information.
type OperationError struct {
	// Op is the logical websocket operation that failed.
	Op string
	// Err is the wrapped lower-level failure.
	Err error
}

// Error formats the operation failure for callers.
func (err *OperationError) Error() string {
	var builder strings.Builder
	builder.WriteString("wsrefact")
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
