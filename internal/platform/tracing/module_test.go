package tracing_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/orlangure/gnomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.uber.org/fx/fxtest"

	"github.com/sergisimo/ledger/internal/platform/tracing"
)

func startJaegerContainer(t *testing.T) string {
	container, err := gnomock.StartCustom(
		"jaegertracing/jaeger:latest",
		gnomock.NamedPorts{"otlp-grpc": gnomock.TCP(4317)},
		gnomock.WithContainerName("jaeger-jsonapi-test"),
	)
	if err != nil {
		t.Fatalf("failed to start jaeger container: %v", err)
	}

	t.Cleanup(func() { _ = gnomock.Stop(container) })

	jaegerAddr := fmt.Sprintf("%s:4317", container.Host)
	return jaegerAddr
}

func TestModule(t *testing.T) {
	jaegerAddr := startJaegerContainer(t)

	app := fxtest.New(
		t,
		tracing.Module("test-service", "1.0.0", jaegerAddr),
	)

	err := app.Start(context.Background())
	require.NoError(t, err, "failed to start module")

	tp := otel.GetTracerProvider()
	require.NotNil(t, tp, "tracer provider should not be nil")

	tracer := tp.Tracer("test-tracer")
	require.NotNil(t, tracer, "tracer should not be nil")

	traceID := tracing.GetTraceID(t.Context())
	assert.Empty(t, traceID)

	ctx, span := tracer.Start(t.Context(), "test-span")
	traceID = tracing.GetTraceID(ctx)
	assert.True(t, span.SpanContext().IsValid())
	assert.NotEmpty(t, traceID)
	assert.Len(t, traceID, 32)
	span.End()

	err = app.Stop(context.Background())
	require.NoError(t, err, "failed to stop module")
}
