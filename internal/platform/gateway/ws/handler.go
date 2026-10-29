package ws

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/logging"
)

// --------------------------------------------------------------- Contract

type (
	// Handler receives authenticated messages whose type the package does not handle. A returned error is sent to the client as TypeError.
	Handler interface {
		Handle(ctx context.Context, conn *Conn, msg Message) error
	}

	// HandlerFunc adapts a function to Handler.
	HandlerFunc func(ctx context.Context, conn *Conn, msg Message) error
)

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, conn *Conn, msg Message) error {
	return f(ctx, conn, msg)
}

// --------------------------------------------------------------- Implementation

func loggingHandler(log logging.Logger) Handler {
	return HandlerFunc(func(ctx context.Context, conn *Conn, msg Message) error {
		log.With("connID", string(conn.ID())).
			With("subject", conn.Identity().Subject).
			With("msgType", msg.Type).
			With("msgID", msg.ID).
			Info(ctx, "websocket message received: %s", msg.Payload)

		return nil
	})
}
