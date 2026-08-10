package gateway

import "errors"

// ErrBrokerNotFound reports that a managed broker ID is unknown.
var ErrBrokerNotFound = errors.New("broker not found")
