package rest

import (
	"context"
	"errors"
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
				func(controllers []Controller) ServerOpt { return ServerWithControllers(controllers...) },
				fx.ParamTags(fmt.Sprintf(`group:"%s"`, controllersTag)),
				fx.ResultTags(fmt.Sprintf(`group:"%s"`, serverOptsTag)),
			),
			fx.Annotate(
				NewServer,
				fx.ParamTags(fmt.Sprintf(`group:"%s"`, serverOptsTag)),
			),
		),
		fx.Invoke(
			func(lc fx.Lifecycle, s *http.Server, sd fx.Shutdowner) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						go func() {
							if err := ListenAndServe(s); err != nil && !errors.Is(err, http.ErrServerClosed) {
								_ = sd.Shutdown(fx.ExitCode(1))
							}
						}()
						return nil
					},
					OnStop: func(ctx context.Context) error {
						ctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
						defer cancel()

						if err := s.Shutdown(ctx); err != nil {
							_ = s.Close()
							return err
						}
						return nil
					},
				})
			},
		),
	)
}

func ListenAndServe(s *http.Server) error {
	if s.TLSConfig != nil {
		return s.ListenAndServeTLS("", "")
	}
	return s.ListenAndServe()
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
