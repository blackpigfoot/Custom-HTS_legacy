package wsrefact

import "Custom-HTS/_legacy/internal/core/pkg/wsrefact/common"

var (
	// ErrURLRequired reports that the websocket endpoint URL was empty.
	ErrURLRequired = common.ErrURLRequired
	// ErrNotConnected reports that no live websocket session is available.
	ErrNotConnected = common.ErrNotConnected
	// ErrAlreadyRunning reports that Run was invoked more than once.
	ErrAlreadyRunning = common.ErrAlreadyRunning
	// ErrResponseTimeout reports that a queued write waited too long for completion.
	ErrResponseTimeout = common.ErrResponseTimeout
	// ErrQueueFull reports that the session request queue is full.
	ErrQueueFull = common.ErrQueueFull
)

// OperationError carries contextual transport operation failure information.
type OperationError = common.OperationError
