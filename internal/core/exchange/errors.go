package exchange

import "errors"

var ErrUnsupported = errors.New("unsupported")

type UnsupportedError struct {
	Broker string
	Op     string
}

func (e *UnsupportedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Broker == "" {
		return e.Op + " not supported"
	}
	return e.Broker + ": " + e.Op + " not supported"
}

func (e *UnsupportedError) Is(target error) bool {
	return target == ErrUnsupported
}

func NewUnsupportedError(broker, op string) error {
	return &UnsupportedError{
		Broker: broker,
		Op:     op,
	}
}

type OpError struct {
	Op  string
	Err error
}

func (e *OpError) Error() string {
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

func (e *OpError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func WrapOp(op string, err error) error {
	if err == nil {
		return nil
	}
	return &OpError{
		Op:  op,
		Err: err,
	}
}
