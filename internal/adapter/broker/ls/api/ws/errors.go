package ws

import "errors"

var (
	// ErrNilConnection reports that the websocket service could not build a transport.
	ErrNilConnection = errors.New("ls api websocket connection is nil")
	// ErrWSConnectionLimit reports that every websocket connection slot is full.
	ErrWSConnectionLimit = errors.New("ls api websocket connection slot limit reached")
	// ErrRealtimeClosed reports that the realtime websocket service lifecycle has ended.
	ErrRealtimeClosed = errors.New("ls api realtime websocket service is closed")
)
