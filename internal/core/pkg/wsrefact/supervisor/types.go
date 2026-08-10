package supervisor

import (
	"time"

	"Custom-HTS/_legacy/internal/core/pkg/wsrefact/common"
)

// WriteRequest is one supervisor-owned queued websocket write intent.
type WriteRequest struct {
	// Key identifies the logical replay key when the request belongs to a subscription.
	Key string
	// Payload is the static payload written when PayloadGen is nil.
	Payload []byte
	// PayloadGen builds the payload at session send time when non-nil.
	PayloadGen common.GenFunc
	// MessageType is the websocket opcode for this request.
	MessageType int
	// WriteTimeout overrides the default websocket write timeout when non-zero.
	WriteTimeout time.Duration
}
