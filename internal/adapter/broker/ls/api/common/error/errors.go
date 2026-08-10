package apierr

import (
	"errors"
	"strconv"
)

var (
	// ErrMissingValue reports that a required value was empty.
	ErrMissingValue = errors.New("missing value")

	// ErrInvalidIssueCode reports that a stock code could not be normalized.
	ErrInvalidIssueCode = errors.New("invalid issue code")

	// ErrNilRequester reports that an API service was built without a requester.
	ErrNilRequester = errors.New("ls api requester is nil")

	// ErrNilAuth reports that an API service was built without auth.
	ErrNilAuth = errors.New("ls api auth is nil")
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

// InvalidIssueCodeError reports the original code value that failed normalization.
type InvalidIssueCodeError struct {
	// TRCode is the LS TR code that received the invalid issue code.
	TRCode string
	// Value is the original caller-provided issue code.
	Value string
}

func (e *InvalidIssueCodeError) Error() string {
	if e == nil {
		return ""
	}

	msg := "invalid issue code"
	if e.TRCode != "" {
		msg = "ls " + e.TRCode + " invalid issue code"
	}
	if e.Value != "" {
		msg += ": " + strconv.Quote(e.Value)
	}
	return msg
}

func (e *InvalidIssueCodeError) Is(target error) bool {
	return target == ErrInvalidIssueCode
}

// DecodePathError reports a logical JSON path that failed to decode.
type DecodePathError struct {
	// Path is the logical JSON path that failed.
	Path string
	// Err is the wrapped decoder error.
	Err error
}

func (e *DecodePathError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return "decode path error: " + e.Path
	}
	return "decode path error: " + e.Path + ": " + e.Err.Error()
}

func (e *DecodePathError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// FieldParseError reports a field-level parse failure with raw value context.
type FieldParseError struct {
	// Field is the logical field name that failed parsing.
	Field string
	// Kind is the requested parse kind such as int64 or float64.
	Kind string
	// Value is the raw source value that failed parsing.
	Value string
	// Err is the wrapped parser error.
	Err error
}

func (e *FieldParseError) Error() string {
	if e == nil {
		return ""
	}

	msg := "parse field " + e.Field
	if e.Kind != "" {
		msg += " as " + e.Kind
	}
	if e.Value != "" {
		msg += " value=" + strconv.Quote(e.Value)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *FieldParseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// OperationError wraps a lower-level error with a short LS operation label.
type OperationError struct {
	// Op identifies the LS operation that failed.
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

// LSError is the common LS REST/WebSocket business error shape.
type LSError struct {
	// RspCd is the LS response code.
	RspCd string
	// RspMsg is the LS response message.
	RspMsg string
}

func (e *LSError) Error() string {
	if e == nil {
		return ""
	}
	msg := "ls api error"
	if e.RspCd != "" {
		msg += " [" + e.RspCd + "]"
	}
	if e.RspMsg != "" {
		msg += ": " + e.RspMsg
	}
	return msg
}
