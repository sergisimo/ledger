package tracing

import (
	"context"

	"go.uber.org/fx"
)

func Module(svcName, svcVersion, jaegerAddr string) fx.Option {
	return fx.Module(
		"tracing",
		fx.Invoke(func(lc fx.Lifecycle) {
			var shutdown func(context.Context) error

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					var err error
					shutdown, err = initTracerProvider(ctx, svcName, svcVersion, jaegerAddr)
					return err
				},
				OnStop: func(ctx context.Context) error {
					if shutdown == nil {
						return nil
					}
					return shutdown(ctx)
				},
			})
		}),
	)
}
