package logger

import (
	"os"

	"go.uber.org/fx"
)

// Module provides the logger to the fx container.
func Module(level Level, serviceName string, traceIDFn TraceIDFn) fx.Option {
	return fx.Provide(func() *Logger {
		return New(os.Stdout, level, serviceName, traceIDFn)
	})
}
