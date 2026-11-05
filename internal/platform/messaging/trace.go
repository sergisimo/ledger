package messaging

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	propagator = propagation.TraceContext{}
	tracer     = otel.Tracer("messaging", trace.WithSchemaURL(semconv.SchemaURL))
)

// InjectTrace returns a copy of msg whose headers carry the trace context of
// ctx. The typed Publisher does this automatically; call it when publishing
// raw messages through a MessagePublisher.
func InjectTrace(ctx context.Context, msg Message) Message {
	headers := make(map[string]string, len(msg.Headers)+2)
	for key, value := range msg.Headers {
		headers[key] = value
	}

	propagator.Inject(ctx, propagation.MapCarrier(headers))
	msg.Headers = headers

	return msg
}

// ExtractTrace returns a context carrying the trace context stored in msg's
// headers, so spans and logs produced while handling msg join the publishing
// trace. The shared Consume pipeline calls it before invoking a handler.
func ExtractTrace(ctx context.Context, msg Message) context.Context {
	if len(msg.Headers) == 0 {
		return ctx
	}

	return propagator.Extract(ctx, propagation.MapCarrier(msg.Headers))
}
