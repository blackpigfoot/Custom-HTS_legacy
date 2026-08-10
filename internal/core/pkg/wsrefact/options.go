package wsrefact

import "time"

// WithWriteTimeout overrides the websocket write timeout for one replayed request.
func WithWriteTimeout(timeout time.Duration) WriteOption {
	return func(options *WriteOptions) {
		options.WriteTimeout = timeout
	}
}

// WithMessageType overrides the websocket opcode for one replayed request.
func WithMessageType(messageType int) WriteOption {
	return func(options *WriteOptions) {
		options.MessageType = messageType
	}
}
