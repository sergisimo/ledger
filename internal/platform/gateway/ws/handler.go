package ws

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/logging"
)

// --------------------------------------------------------------- Contract

type (
	Handler interface {
		Handle(ctx context.Context, conn *Conn, msg Message) error
	}

	HandlerFunc func(ctx context.Context, conn *Conn, msg Message) error
)

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
