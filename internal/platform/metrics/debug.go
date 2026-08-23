// Package metrics provides support for exposing metrics and debug information.
package metrics

import (
	"context"
	"expvar"
	"net/http"
	"net/http/pprof"

	"github.com/arl/statsviz"
	"github.com/sergisimo/ledger/internal/platform/logger"
	"go.uber.org/fx"
)

// Mux registers all the debug routes from the standard library into a new mux
// bypassing the use of the DefaultServerMux. Using the DefaultServerMux would
// be a security risk since a dependency could inject a handler into our service
// without us knowing it.
func DebugMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/vars/", expvar.Handler())

	statsviz.Register(mux)

	return mux
}

func Module(debugHost string) fx.Option {
	return fx.Module(
		"metrics",
		fx.Provide(fx.Annotate(DebugMux, fx.ResultTags(`name:"debugMux"`))),
		fx.Invoke(
			fx.Annotate(
				func(lc fx.Lifecycle, log *logger.Logger, mux *http.ServeMux) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							go func() {
								log.Info(ctx, "startup", "status", "debug router started", "host", debugHost)

								if err := http.ListenAndServe(debugHost, mux); err != nil {
									log.Error(ctx, "shutdown", "status", "debug router closed", "host", debugHost, "msg", err)
								}
							}()
							return nil
						},
						OnStop: func(ctx context.Context) error {
							return nil
						},
					})
				},
				fx.ParamTags(``, ``, `name:"debugMux"`),
			),
		),
	)
}
