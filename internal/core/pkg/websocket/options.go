package websocket

import "time"

// WithWriteTimeout overrides the websocket write timeout for one replayed write.
func WithWriteTimeout(timeout time.Duration) SubOption {
	return func(options *SubOptions) {
		options.WriteTimeout = timeout
	}
}

// WithMessageType overrides the websocket opcode for one replayed write.
func WithMessageType(messageType int) SubOption {
	return func(options *SubOptions) {
		options.MessageType = messageType
	}
}
