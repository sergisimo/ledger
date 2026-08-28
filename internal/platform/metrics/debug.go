// Package metrics provides support for exposing metrics and debug information.
package metrics

import (
	"context"
	"expvar"
	"net/http"
	"net/http/pprof"

	"github.com/arl/statsviz"
	"github.com/sergisimo/ledger/internal/platform/logging"
	"go.uber.org/fx"
)

func debugMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/vars/", expvar.Handler())

	_ = statsviz.Register(mux)

	return mux
}

func Module(debugHost string) fx.Option {
	return fx.Module(
		"metrics",
		fx.Invoke(
			func(lc fx.Lifecycle, log logging.Logger) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						go func() {
							log.Info(ctx, "debug router started on host %s", debugHost)

							if err := http.ListenAndServe(debugHost, debugMux()); err != nil {
								log.Error(ctx, "debug router failed to start: %v", err)
							}
						}()
						return nil
					},
				})
			},
		),
	)
}
