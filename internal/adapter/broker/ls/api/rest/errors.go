package rest

import (
	"errors"
	"strconv"
)

var (
	// ErrT8407CodesRequired reports that t8407 was called without any codes.
	ErrT8407CodesRequired = errors.New("t8407 requires at least one code")

	// ErrTooManyCodes reports that the caller provided more codes than a TR allows.
	ErrTooManyCodes = errors.New("too many codes")

	// ErrInvalidDecimalScale reports that a decimal scale must be a positive power of 10.
	ErrInvalidDecimalScale = errors.New("invalid decimal scale")
)

// CodeLimitError reports that a request exceeded the TR-specific code limit.
type CodeLimitError struct {
	// TRCode is the LS TR code that owns the limit.
	TRCode string
	// Limit is the maximum allowed item count.
	Limit int
	// Count is the caller-provided item count.
	Count int
}

func (e *CodeLimitError) Error() string {
	if e == nil {
		return ""
	}

	msg := "too many codes"
	if e.TRCode != "" {
		msg = "ls " + e.TRCode + " allows up to " + strconv.Itoa(e.Limit) + " codes"
	} else if e.Limit > 0 {
		msg = "allows up to " + strconv.Itoa(e.Limit) + " codes"
	}
	if e.Count > 0 {
		msg += ": got " + strconv.Itoa(e.Count)
	}
	return msg
}

func (e *CodeLimitError) Is(target error) bool {
	return target == ErrTooManyCodes
}
