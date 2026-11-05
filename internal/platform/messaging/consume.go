package messaging

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// Consume runs h on msg with the pipeline every broker package shares: it
// extracts the publishing trace from the headers, opens a consumer span named
// after the topic with the messaging semantic conventions for system (the
// broker, e.g. "kafka") plus any broker-specific attrs, recovers handler
// panics into a Permanent ErrHandlerPanic and records failures on the span.
// Broker packages call it instead of invoking the handler directly and map
// the returned error to their acknowledgement calls: nil acknowledges,
// IsPermanent rejects without redelivery, anything else asks for redelivery.
func Consume(ctx context.Context, system string, msg Message, h HandlerFunc, attrs ...attribute.KeyValue) (err error) {
	ctx = ExtractTrace(ctx, msg)
	ctx, span := tracer.Start(
		ctx,
		"process "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String(system),
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingMessageID(msg.ID),
			semconv.MessagingOperationName("process"),
			semconv.MessagingOperationTypeProcess,
		),
		trace.WithAttributes(attrs...),
	)

	defer span.End()

	defer func() {
		if r := recover(); r != nil {
			err = Permanent(fmt.Errorf("%w on topic %s: %v", ErrHandlerPanic, msg.Topic, r))
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}()

	return h(ctx, msg)
}
