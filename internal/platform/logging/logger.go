package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/tracing"
	"go.uber.org/fx"
)

// --------------------------------------------------------------- Contract
type (
	Logger interface {
		Debug(ctx context.Context, tmpl string, args ...any)
		Info(ctx context.Context, tmpl string, args ...any)
		Warn(ctx context.Context, tmpl string, args ...any)
		Error(ctx context.Context, tmpl string, args ...any)

		With(key string, value any) Logger
	}

	LoggerType string
)

const (
	LoggerTypeJSON LoggerType = "JSON"
	LoggerTypeText LoggerType = "TEXT"
)

// --------------------------------------------------------------- Fx

func Module() fx.Option {
	return fx.Module(
		"logging",
		fx.Provide(fx.Annotate(
			NewLogger,
			fx.ParamTags(`group:"loggerOpts"`),
			fx.As(new(Logger)),
		)),
	)
}

// --------------------------------------------------------------- Implementation

type (
	slogLogger struct {
		*slog.Logger
	}

	loggerConfig struct {
		svcName string
		level   slog.Level
		kind    LoggerType
		handler slog.Handler
	}

	loggerOption func(*loggerConfig)
)

func WithServiceName(name string) loggerOption {
	return func(cfg *loggerConfig) {
		cfg.svcName = name
	}
}

func WithLogLevel(level slog.Level) loggerOption {
	return func(cfg *loggerConfig) {
		cfg.level = level
	}
}

func WithLoggerType(kind LoggerType) loggerOption {
	return func(cfg *loggerConfig) {
		cfg.kind = kind
	}
}

func WithHandler(handler slog.Handler) loggerOption {
	return func(cfg *loggerConfig) {
		cfg.handler = handler
	}
}

func loggerDefaultOpts() []loggerOption {
	return []loggerOption{
		WithServiceName(os.Getenv("SERVICE_NAME")),
		WithLogLevel(parseLevel(os.Getenv("LOG_LEVEL"))),
		WithLoggerType(parseLoggerType(os.Getenv("LOG_TYPE"))),
	}
}

func NewLogger(opts ...loggerOption) (*slogLogger, error) {
	cfg := &loggerConfig{}

	for _, opt := range append(loggerDefaultOpts(), opts...) {
		opt(cfg)
	}

	if err := fields.NotEmptyStringValidator(fields.NameService.Merge(fields.NameName))(cfg.svcName); err != nil {
		return nil, err
	}

	if cfg.handler == nil {
		if cfg.kind == LoggerTypeText {
			cfg.handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.level})
		} else {
			cfg.handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.level})
		}
	}

	return &slogLogger{
		Logger: slog.New(cfg.handler).With("svcName", cfg.svcName),
	}, nil
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func parseLoggerType(kind string) LoggerType {
	switch strings.ToLower(kind) {
	case "json":
		return LoggerTypeJSON
	case "text":
		return LoggerTypeText
	default:
		return LoggerTypeJSON
	}
}

func (l *slogLogger) Debug(ctx context.Context, tmpl string, args ...any) {
	l.injectTraceID(ctx).Debug(fmt.Sprintf(tmpl, args...))
}

func (l *slogLogger) Info(ctx context.Context, tmpl string, args ...any) {
	l.injectTraceID(ctx).Info(fmt.Sprintf(tmpl, args...))
}

func (l *slogLogger) Warn(ctx context.Context, tmpl string, args ...any) {
	l.injectTraceID(ctx).Warn(fmt.Sprintf(tmpl, args...))
}

func (l *slogLogger) Error(ctx context.Context, tmpl string, args ...any) {
	l.injectTraceID(ctx).Error(fmt.Sprintf(tmpl, args...))
}

func (l *slogLogger) With(key string, value any) Logger {
	return &slogLogger{Logger: l.Logger.With(key, value)}
}

func (l *slogLogger) injectTraceID(ctx context.Context) *slog.Logger {
	traceID := tracing.GetTraceID(ctx)
	if traceID != "" {
		return l.Logger.With("trace_id", traceID)
	}
	return l.Logger
}
