package messaging

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

type (
	// MessagePublisher delivers raw messages. It is implemented by broker
	// packages and by the outbox; use cases depend on Publisher[T] instead.
	// Implementations validate every message (Message.Validate) before
	// accepting the batch. On a plain error nothing is assumed delivered, so
	// retrying the same batch must be safe (delivery is at least once); an
	// implementation that delivered part of the batch reports the failed rest
	// with a *BatchError instead.
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
		pub    MessagePublisher
		topic  string
		system string
	}
)

// NewPublisher returns a Publisher for payloads of type T on topic, delivered
// through pub. Most applications wire it with PublisherFx instead.
func NewPublisher[T any](pub MessagePublisher, topic, system string) (Publisher[T], error) {
	if topic == "" {
		return nil, fields.NewErrInvalidEmptyString(FieldNameTopic)
	}

	return jsonPublisher[T]{pub: pub, topic: topic, system: system}, nil
}

func (p jsonPublisher[T]) Publish(ctx context.Context, payloads ...T) (err error) {
	if len(payloads) == 0 {
		return nil
	}

	attributes := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(p.system),
		semconv.MessagingDestinationName(p.topic),
		semconv.MessagingOperationName("publish"),
		semconv.MessagingOperationTypeSend,
	}
	if len(payloads) > 1 {
		attributes = append(attributes, semconv.MessagingBatchMessageCount(len(payloads)))
	}

	ctx, span := tracer.Start(
		ctx,
		"publish "+p.topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attributes...),
	)

	defer span.End()

	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}()

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
