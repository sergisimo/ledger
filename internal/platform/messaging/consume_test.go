package messaging_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/inmem"
)

func TestConsume(t *testing.T) {
	msg := messaging.NewMessage("acme.accounts", []byte(`{}`))

	t.Run("acknowledges by returning nil and runs on the publishing trace", func(t *testing.T) {
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID{0xab},
			SpanID:     trace.SpanID{0xcd},
			TraceFlags: trace.FlagsSampled,
		})
		published := messaging.InjectTrace(trace.ContextWithSpanContext(t.Context(), sc), msg)

		var gotTraceID trace.TraceID
		err := messaging.Consume(
			t.Context(),
			inmem.SystemName,
			published,
			func(ctx context.Context, got messaging.Message) error {
				gotTraceID = trace.SpanContextFromContext(ctx).TraceID()
				assert.Equal(t, published.ID, got.ID)
				return nil
			},
		)

		require.NoError(t, err)
		assert.Equal(t, sc.TraceID(), gotTraceID, "the handler should run inside the publishing trace")
	})

	t.Run("propagates handler errors untouched", func(t *testing.T) {
		err := messaging.Consume(
			t.Context(),
			inmem.SystemName,
			msg,
			func(context.Context, messaging.Message) error { return assert.AnError },
		)

		require.ErrorIs(t, err, assert.AnError)
		assert.False(t, messaging.IsPermanent(err), "a plain handler error should stay retryable")
	})

	t.Run("recovers panics into a permanent handler-panic error", func(t *testing.T) {
		err := messaging.Consume(
			t.Context(),
			inmem.SystemName,
			msg,
			func(context.Context, messaging.Message) error { panic("kaboom") },
		)

		require.ErrorIs(t, err, messaging.ErrHandlerPanic)
		assert.True(t, messaging.IsPermanent(err), "a panic should never be redelivered")
		assert.ErrorContains(t, err, "kaboom")
		assert.ErrorContains(t, err, "acme.accounts")
	})
}
