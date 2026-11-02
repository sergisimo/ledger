package messaging

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

type (
	// MessagePublisher delivers raw messages. It is implemented by broker
	// packages and by the outbox; use cases depend on Publisher[T] instead.
	// On error nothing is assumed delivered, so retrying the same batch must
	// be safe (delivery is at least once).
	MessagePublisher interface {
		Publish(ctx context.Context, msgs ...Message) error
	}

	// Publisher publishes payloads of type T to the topic fixed at
	// construction. Each payload becomes one JSON message carrying the trace
	// context of ctx.
	Publisher[T any] interface {
		Publish(ctx context.Context, payloads ...T) error
	}

	jsonPublisher[T any] struct {
		pub   MessagePublisher
		topic string
	}
)

// NewPublisher returns a Publisher for payloads of type T on topic, delivered
// through pub. Most applications wire it with PublisherFx instead.
func NewPublisher[T any](pub MessagePublisher, topic string) (Publisher[T], error) {
	if topic == "" {
		return nil, fields.NewErrInvalidEmptyString(FieldNameTopic)
	}

	return jsonPublisher[T]{pub: pub, topic: topic}, nil
}

func (p jsonPublisher[T]) Publish(ctx context.Context, payloads ...T) error {
	if len(payloads) == 0 {
		return nil
	}

	msgs := make([]Message, 0, len(payloads))
	for _, payload := range payloads {
		msg, err := NewJSONMessage(p.topic, payload)
		if err != nil {
			return err
		}
		msgs = append(msgs, InjectTrace(ctx, msg))
	}

	return p.pub.Publish(ctx, msgs...)
}
