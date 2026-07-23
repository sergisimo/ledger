package logger

import (
	"log/slog"
	"os"

	"go.uber.org/fx"
)

// Module provides the logger to the fx container.
func Module(serviceName string, traceIDFn TraceIDFn) fx.Option {
	return fx.Module(
		"logger",
		fx.Provide(func() (*Logger, error) {
			level, err := getLogLevel()
			if err != nil {
				return nil, err
			}

			return New(os.Stdout, Level(level), serviceName, traceIDFn), nil
		}),
	)
}

func getLogLevel() (slog.Level, error) {
	levelEnv := os.Getenv("LOG_LEVEL")
	if levelEnv == "" {
		levelEnv = "INFO"
	}

	var level slog.Level
	err := level.UnmarshalText([]byte(levelEnv))
	return level, err
}
