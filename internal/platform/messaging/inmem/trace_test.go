package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/tracing"
)

func TestTracePropagation(t *testing.T) {
	broker := messagingtest.NewBroker(t)
	pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")

	traceIDs := make(chan string, 1)
	messagingtest.Subscribe(t, broker, "ledger.accounts", func(ctx context.Context, _ messaging.Message) error {
		traceIDs <- tracing.GetTraceID(ctx)
		return nil
	})

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0xab},
		SpanID:     trace.SpanID{0xcd},
		TraceFlags: trace.FlagsSampled,
	})
	require.NoError(t, pub.Publish(trace.ContextWithSpanContext(t.Context(), sc), event{Name: "checking"}))

	select {
	case got := <-traceIDs:
		assert.Equal(t, sc.TraceID().String(), got, "the handler should run inside the publishing trace")
	case <-time.After(messagingtest.Timeout):
		t.Fatal("the handler never ran")
	}
}
