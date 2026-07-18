package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"

	"github.com/ardanlabs/conf/v3"

	"github.com/sergisimo/ledger/internal/platform/logger"
	"github.com/sergisimo/ledger/internal/platform/metrics"
	"go.uber.org/fx"
)

var tag = "develop"

func main() {
	const svcName = "ledger"

	// Configuration
	cfg := struct {
		conf.Version
		Web struct {
			DebugHost string `conf:"default:0.0.0.0:8081"`
		}
	}{
		Version: conf.Version{
			Build: tag,
			Desc:  "Ledger Service",
		},
	}

	help, err := conf.Parse(svcName, &cfg)
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
		logger.Module(logger.LevelInfo, svcName, func(ctx context.Context) string { return "" }),
		metrics.Module(cfg.Web.DebugHost),
		fx.Invoke(func(lc fx.Lifecycle, log *logger.Logger) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					// GOMAXPROCS
					log.Info(ctx, "startup", "GOMAXPROCS", runtime.GOMAXPROCS(0))
					log.Info(ctx, "startup", "version", tag)

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
