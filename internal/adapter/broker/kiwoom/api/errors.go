package api

import (
	"errors"
	"strconv"
)

var (
	// ErrMissingValue reports that a required value was empty.
	ErrMissingValue = errors.New("missing value")

	// ErrNilRequester reports that an API service was built without a requester.
	ErrNilRequester = errors.New("kiwoom api requester is nil")

	// ErrNilAuth reports that an API service was built without auth.
	ErrNilAuth = errors.New("kiwoom api auth is nil")
)

// MissingValueError adds field context to ErrMissingValue.
type MissingValueError struct {
	// Field identifies the missing logical field.
	Field string
}

func (e *MissingValueError) Error() string {
	if e == nil {
		return ""
	}
	return "missing value: " + e.Field
}

func (e *MissingValueError) Is(target error) bool {
	return target == ErrMissingValue
}

// OperationError wraps a lower-level error with a short Kiwoom operation label.
type OperationError struct {
	// Op identifies the Kiwoom operation that failed.
	Op string
	// Err is the wrapped lower-level error.
	Err error
}

func (e *OperationError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Op
	}
	if e.Op == "" {
		return e.Err.Error()
	}
	return e.Op + ": " + e.Err.Error()
}

func (e *OperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// KiwoomError is the common Kiwoom REST business error shape.
type KiwoomError struct {
	// ReturnCode is the Kiwoom response code.
	ReturnCode int
	// ReturnMsg is the Kiwoom response message.
	ReturnMsg string
}

func (e *KiwoomError) Error() string {
	if e == nil {
		return ""
	}

	// msg is the human-readable Kiwoom API error message.
	msg := "kiwoom api error"
	msg += " [" + strconv.Itoa(e.ReturnCode) + "]"
	if e.ReturnMsg != "" {
		msg += ": " + e.ReturnMsg
	}
	return msg
}
