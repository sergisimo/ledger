package main

import (
	"context"
	"fmt"

	"go.uber.org/fx"
)

var tag = "develop"

func main() {
	app := fx.New(fx.Invoke(func(lc fx.Lifecycle) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				fmt.Printf("Starting Ledger Service... (tag: %s)\n", tag)
				return nil
			},
			OnStop: func(ctx context.Context) error {
				fmt.Printf("Stopping Ledger Service... (tag: %s)\n", tag)
				return nil
			},
		})
	}))
	app.Run()
}
