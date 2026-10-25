package ws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/authx"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

const (
	ServerOptsTag = "wsServerOpts"
)

func Module(shutdownTimeout time.Duration) fx.Option {
	return fx.Module(
		"ws",
		fx.Provide(
			fx.Annotate(
				func(log logging.Logger) ServerOpt { return ServerWithLogger(log) },
				fx.ResultTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag)),
			),
			fx.Annotate(
				func(authenticator authx.Authenticator) ServerOpt { return ServerWithAuthenticator(authenticator) },
				fx.ResultTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag)),
			),
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

func HandlerFx(constructor any) fx.Option {
	return fx.Provide(
		fx.Annotate(
			func(handler Handler) ServerOpt { return ServerWithHandler(handler) },
			fx.ResultTags(fmt.Sprintf(`group:"%s"`, ServerOptsTag)),
		),
		fx.Annotate(constructor, fx.As(new(Handler))),
	)
}
