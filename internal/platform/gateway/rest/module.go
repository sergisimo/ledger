package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/fx"
)

const (
	controllersTag = "restControllers"
	serverOptsTag  = "restServerOpts"
)

func Module(shutdownTimeout time.Duration) fx.Option {
	return fx.Module(
		"rest",
		fx.Provide(
			fx.Annotate(
				func(controllers []Controller) serverOpt { return ServerWithControllers(controllers...) },
				fx.ParamTags(fmt.Sprintf(`group:"%s"`, controllersTag)),
				fx.ResultTags(fmt.Sprintf(`group:"%s"`, serverOptsTag)),
			),
			fx.Annotate(
				NewServer,
				fx.ParamTags(``, fmt.Sprintf(`group:"%s"`, serverOptsTag)),
			),
		),
		fx.Invoke(
			func(lc fx.Lifecycle, s *http.Server) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						go func() {
							if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
								panic(fmt.Sprintf("failed to start REST server: %v", err))
							}
						}()
						return nil
					},
					OnStop: func(ctx context.Context) error {
						ctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
						defer cancel()

						if err := s.Shutdown(ctx); err != nil {
							s.Close()
							return err
						}
						return nil
					},
				})
			},
		),
	)
}

func ControllerFx(constructor any) fx.Option {
	return fx.Provide(
		fx.Annotate(
			constructor,
			fx.ResultTags(fmt.Sprintf(`group:"%s"`, controllersTag)),
			fx.As(new(Controller)),
		),
	)
}
