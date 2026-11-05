package messaging_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

func TestTraceInjectExtract(t *testing.T) {
	t.Run("round-trips the trace context through the headers", func(t *testing.T) {
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID{0xab},
			SpanID:     trace.SpanID{0xcd},
			TraceFlags: trace.FlagsSampled,
		})
		ctx := trace.ContextWithSpanContext(t.Context(), sc)

		original := messaging.NewMessage("acme.accounts", nil)
		injected := messaging.InjectTrace(ctx, original)

		assert.NotEmpty(t, injected.Headers[messaging.HeaderTraceParent])
		assert.Empty(t, original.Headers, "the original message should not be mutated")

		got := trace.SpanContextFromContext(messaging.ExtractTrace(t.Context(), injected))
		assert.Equal(t, sc.TraceID(), got.TraceID())
	})

	t.Run("is a no-op without a trace or headers", func(t *testing.T) {
		msg := messaging.InjectTrace(t.Context(), messaging.NewMessage("acme.accounts", nil))
		assert.Empty(t, msg.Headers[messaging.HeaderTraceParent])

		ctx := messaging.ExtractTrace(t.Context(), messaging.Message{})
		assert.False(t, trace.SpanContextFromContext(ctx).IsValid())
	})
}
