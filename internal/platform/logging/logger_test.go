package logging_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
)

func TestNewLogger(t *testing.T) {
	var (
		buf = &bytes.Buffer{}
		ctx = t.Context()
	)

	logger, err := logging.NewLogger()
	require.ErrorIs(t, err, fields.NewErrInvalidEmptyString(fields.NameService.Merge(fields.NameName)))

	handler := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger, err = logging.NewLogger(
		logging.WithServiceName("test-service"),
		logging.WithHandler(handler),
		logging.WithLoggerType(logging.LoggerTypeText),
	)
	require.NoError(t, err)

	buf.Reset()
	logger.Debug(ctx, "debug message: %s", "test")
	assert.Contains(t, buf.String(), "level=DEBUG")
	assert.Contains(t, buf.String(), "debug message: test")

	buf.Reset()
	logger.Info(ctx, "info message: %s", "test")
	assert.Contains(t, buf.String(), "level=INFO")
	assert.Contains(t, buf.String(), "info message: test")

	buf.Reset()
	logger.Warn(ctx, "warn message: %s", "test")
	assert.Contains(t, buf.String(), "level=WARN")
	assert.Contains(t, buf.String(), "warn message: test")

	buf.Reset()
	logger.Error(ctx, "error message: %s", "test")
	assert.Contains(t, buf.String(), "level=ERROR")
	assert.Contains(t, buf.String(), "error message: test")

	buf.Reset()
	logger.With("user_id", "12345").Info(ctx, "with custom field")
	output := buf.String()
	assert.Contains(t, output, "level=INFO")
	assert.Contains(t, output, "with custom field")
	assert.Contains(t, output, "user_id=12345")

	tp := trace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	tracer := otel.Tracer("test")
	ctxWithTrace, trace := tracer.Start(ctx, "test-span")
	defer trace.End()

	buf.Reset()
	logger.Info(ctxWithTrace, "message with trace")
	assert.Contains(t, buf.String(), "level=INFO")
	assert.Contains(t, buf.String(), "trace_id")
}
