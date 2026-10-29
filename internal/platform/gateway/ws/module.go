package ws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/fx"
)

const (
	// ServerOptsTag is the fx group that collects ServerOpt values.
	ServerOptsTag = "wsServerOpts"
)

// Module provides a Server built from the ServerOptsTag group and runs it with the fx lifecycle.
func Module(shutdownTimeout time.Duration) fx.Option {
	return fx.Module(
		"ws",
		fx.Provide(
			fx.Annotate(ServerWithLogger, fx.ResultTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag))),
			fx.Annotate(ServerWithAuthenticator, fx.ResultTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag))),
			fx.Annotate(
				NewServer,
				fx.ParamTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag)),
			),
		),
		fx.Invoke(
			func(lc fx.Lifecycle, s *Server, sd fx.Shutdowner) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						ctx = context.WithoutCancel(ctx)
						go func() {
							if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
								s.log.Error(ctx, "WebSocket server failed: %v", err)
								_ = sd.Shutdown(fx.ExitCode(1))
							}
						}()
						return nil
					},
					OnStop: func(ctx context.Context) error {
						ctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
						defer cancel()

						return s.Shutdown(ctx)
					},
				})
			},
		),
	)
}

// HandlerFx provides constructor as the Handler.
func HandlerFx(constructor any) fx.Option {
	return fx.Provide(
		fx.Annotate(ServerWithHandler, fx.ResultTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag))),
		fx.Annotate(constructor, fx.As(new(Handler))),
	)
}

// SubscriptionHandlerFx provides constructor as the SubscriptionHandler.
func SubscriptionHandlerFx(constructor any) fx.Option {
	return fx.Provide(
		fx.Annotate(ServerWithSubscriptionHandler, fx.ResultTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag))),
		fx.Annotate(constructor, fx.As(new(SubscriptionHandler))),
	)
}

// PublisherFx provides a Publisher[T] backed by the Server.
func PublisherFx[T any]() fx.Option {
	return fx.Provide(NewPublisher[T])
}
