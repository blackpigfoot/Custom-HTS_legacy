package common

import "github.com/gorilla/websocket"

// MessageType identifies a websocket frame opcode.
//
// The values match RFC 6455 opcodes and gorilla/websocket constants
// (TextMessage=1, BinaryMessage=2). Defined here so upper layers
// (brokers, strategies) can express message types without importing
// gorilla/websocket directly.
type MessageType int

// Valid MessageType values are defined here as a convenience for upper layers and to avoid importing gorilla/websocket at all in some cases.
const (
	TextMessage   MessageType = websocket.TextMessage
	BinaryMessage MessageType = websocket.BinaryMessage
	CloseMessage  MessageType = websocket.CloseMessage
	PingMessage   MessageType = websocket.PingMessage
	PongMessage   MessageType = websocket.PongMessage
)
