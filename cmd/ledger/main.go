package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/ardanlabs/conf/v3"

	"github.com/sergisimo/ledger/internal/ledger"
	"github.com/sergisimo/ledger/internal/platform/gateway/rest"
	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/metrics"
	"github.com/sergisimo/ledger/internal/platform/tracing"
	"go.uber.org/fx"
)

var tag = "develop"

func main() {
	const svcName = "ledger"

	// Configuration
	cfg := struct {
		conf.Version
		Rest struct {
			ReadTimeout     time.Duration `conf:"default:5s,env:REST_READ_TIMEOUT"`
			WriteTimeout    time.Duration `conf:"default:10s,env:REST_WRITE_TIMEOUT"`
			IdleTimeout     time.Duration `conf:"default:120s,env:REST_IDLE_TIMEOUT"`
			ShutdownTimeout time.Duration `conf:"default:20s,env:REST_SHUTDOWN_TIMEOUT"`
			Address         string        `conf:"default:localhost:8080,env:REST_ADDRESS"`
			DebugHost       string        `conf:"default:0.0.0.0:8081"`
		}
		Trace struct {
			Address string `conf:"default:localhost:4317,env:TRACE_ADDRESS"`
		}
		Log struct {
			Level string `conf:"default:INFO,env:LOG_LEVEL"`
		}
	}{
		Version: conf.Version{
			Build: tag,
			Desc:  "Ledger Service",
		},
	}

	help, err := conf.Parse("", &cfg)
	if err != nil {
		if errors.Is(err, conf.ErrHelpWanted) {
			fmt.Println(help)
			os.Exit(0)
		}
		fmt.Println(err)
		os.Exit(1)
	}

	// Dependency Injection
	app := fx.New(
		logging.Module(),
		tracing.Module(svcName, tag, cfg.Trace.Address),
		metrics.Module(cfg.Rest.DebugHost),
		rest.Module(cfg.Rest.ShutdownTimeout),
		ledger.Module(),
		fx.Invoke(func(lc fx.Lifecycle, log logging.Logger) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					log.With("GOMAXPROCS", runtime.GOMAXPROCS(0)).
						With("version", tag).
						Info(ctx, "starting ledger service...")

					out, err := conf.String(&cfg)
					if err != nil {
						return fmt.Errorf("generating config for output: %w", err)
					}
					log.With("config", out).Info(ctx, "service configuration")
					return nil
				},
				OnStop: func(ctx context.Context) error {
					log.Info(ctx, "stopping ledger service...")
					return nil
				},
			})
		}),
	)
	app.Run()
}
