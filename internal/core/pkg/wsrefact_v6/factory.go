package wsrefact_v6

import (
	"context"

	"github.com/gorilla/websocket"
)

// Dial opens one fresh websocket connection and returns the raw gorilla websocket connection.
func (factory *dialFactory) Dial(dialCtx context.Context) (*websocket.Conn, error) {
	if factory.URL == "" {
		return nil, ErrURLRequired
	}

	// conn stores the raw connected websocket returned by gorilla.
	conn, _, err := factory.Dialer.DialContext(dialCtx, factory.URL, factory.Header)
	if err != nil {
		return nil, &OperationError{
			Op:  "dial",
			Err: err,
		}
	}

	return conn, nil
}
