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
	"github.com/sergisimo/ledger/internal/platform/logger"
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
		logger.Module(svcName, tracing.GetTraceID),
		tracing.Module(svcName, tag, cfg.Trace.Address),
		metrics.Module(cfg.Rest.DebugHost),
		rest.Module(cfg.Rest.ShutdownTimeout),
		ledger.Module(),
		fx.Invoke(func(lc fx.Lifecycle, log *logger.Logger) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					// GOMAXPROCS
					log.Info(ctx, "startup", "GOMAXPROCS", runtime.GOMAXPROCS(0))
					log.Info(ctx, "startup", "version", tag)
					log.Info(ctx, "trace", "jaeger", cfg.Trace.Address)

					out, err := conf.String(&cfg)
					if err != nil {
						return fmt.Errorf("generating config for output: %w", err)
					}
					log.Info(ctx, "startup", "config", out)
					return nil
				},
				OnStop: func(ctx context.Context) error {
					log.Info(ctx, "Stopping Ledger Service...")
					return nil
				},
			})
		}),
	)
	app.Run()
}
