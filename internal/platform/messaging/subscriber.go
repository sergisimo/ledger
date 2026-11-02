package messaging

import (
	"context"
	"errors"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

// FieldNameHandler names the handler field in validation errors, e.g.
// fields.NewErrNil(FieldNameHandler) for a nil handler.
const FieldNameHandler fields.Name = "handler"

var (
	// ErrAlreadySubscribed is returned by Subscribe on an active subscriber.
	ErrAlreadySubscribed = errors.New("subscriber is already subscribed")
	// ErrNotSubscribed is returned by Unsubscribe on an inactive subscriber.
	ErrNotSubscribed = errors.New("subscriber is not subscribed")
)

type (
	// HandlerFunc processes one message. Returning nil acknowledges it; an
	// error asks the broker to redeliver where it can (the in-memory broker
	// only logs it).
	HandlerFunc func(ctx context.Context, msg Message) error

	// OnErrorFunc receives subscription failures that surface after Subscribe
	// returned and are not plain handler errors, such as dropped messages or
	// handler panics.
	OnErrorFunc func(ctx context.Context, err error)

	// Subscriber is one subscription. The topic and handler are fixed when the
	// broker package constructs it; Subscribe and Unsubscribe start and stop
	// the worker delivering the topic's messages to the handler, and the cycle
	// may repeat. Most applications wire subscribers with SubscriberFx.
	Subscriber interface {
		// Subscribe starts the worker. onError may be nil to only log the
		// failures it would receive.
		Subscribe(ctx context.Context, onError OnErrorFunc) error
		// Unsubscribe stops the worker, waiting for an in-flight handler until
		// ctx expires.
		Unsubscribe(ctx context.Context) error
	}
)

// Handle adapts a typed handler to a HandlerFunc, decoding JSON payloads. A
// payload that fails to decode is returned as the handler error.
func Handle[T any](h func(ctx context.Context, payload T, msg Message) error) HandlerFunc {
	return func(ctx context.Context, msg Message) error {
		var payload T
		if err := msg.Bind(&payload); err != nil {
			return err
		}

		return h(ctx, payload, msg)
	}
}
