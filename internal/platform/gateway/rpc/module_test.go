package rpc_test

import (
	"testing"
	"time"

	"github.com/sergisimo/ledger/internal/platform/gateway/rpc"
	"github.com/sergisimo/ledger/internal/platform/usecase/usecasetest"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestModule(t *testing.T) {
	t.Setenv("GRPC_ADDRESS", "127.0.0.1:0")

	app := fxtest.New(t,
		rpc.Module(time.Second),
		rpc.ControllerFx(func() *testCtrl {
			return newTestCtrl(usecasetest.New[testEntity](t))
		}),
		fx.NopLogger,
	)

	app.RequireStart()
	app.RequireStop()
}
