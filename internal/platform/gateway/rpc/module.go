package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/fx"
	"google.golang.org/grpc"
)

const (
	controllersTag = "rpcControllers"
	ServerOptsTag  = "rpcServerOpts"
)

func Module(shutdownTimeout time.Duration) fx.Option {
	return fx.Module(
		"rpc",
		fx.Provide(
			fx.Annotate(
				func(controllers []Controller) ServerOpt { return ServerWithControllers(controllers...) },
				fx.ParamTags(fmt.Sprintf(`group:"%s"`, controllersTag)),
				fx.ResultTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag)),
			),
			fx.Annotate(
				NewServer,
				fx.ParamTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag)),
			),
		),
		fx.Invoke(
			func(lc fx.Lifecycle, s *Server) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						go func() {
							if err := s.ListenAndServe(); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
								panic(fmt.Sprintf("failed to start gRPC server: %v", err))
							}
						}()
						return nil
					},
					OnStop: func(ctx context.Context) error {
						ctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
						defer cancel()

						stopped := make(chan struct{})
						go func() {
							s.GracefulStop()
							close(stopped)
						}()

						select {
						case <-stopped:
							return nil
						case <-ctx.Done():
							s.Stop()
							return ctx.Err()
						}
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
