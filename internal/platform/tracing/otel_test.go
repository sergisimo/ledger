package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestGetTraceID(t *testing.T) {
	t.Run("missing span returns empty string", func(t *testing.T) {
		assert.Empty(t, GetTraceID(context.Background()))
	})

	t.Run("active span returns trace id", func(t *testing.T) {
		tp := sdktrace.NewTracerProvider()
		tracer := tp.Tracer("test")

		ctx, span := tracer.Start(context.Background(), "operation")
		defer span.End()

		got := GetTraceID(ctx)
		require.NotEmpty(t, got)
		assert.Len(t, got, 32)
	})
}
