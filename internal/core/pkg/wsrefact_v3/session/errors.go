package session

import "strings"

// OperationError carries contextual session operation failure information.
type OperationError struct {
	// Op is the logical websocket operation that failed.
	Op string
	// Err is the wrapped lower-level failure.
	Err error
}

// Error formats the operation failure for callers.
func (err *OperationError) Error() string {
	// builder accumulates the stable error prefix and wrapped message.
	var builder strings.Builder
	builder.WriteString("session")
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
