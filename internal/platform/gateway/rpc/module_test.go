package rpc_test

import (
	"testing"
	"time"

	"github.com/sergisimo/ledger/internal/platform/gateway/rpc"
	"github.com/sergisimo/ledger/internal/platform/usecase/usecasetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestModule(t *testing.T) {
	t.Run("starts and stops", func(t *testing.T) {
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
	})

	t.Run("shuts the app down when it cannot listen", func(t *testing.T) {
		t.Setenv("GRPC_ADDRESS", "invalid:address:99999")

		app := fx.New(
			rpc.Module(time.Second),
			rpc.ControllerFx(func() *testCtrl {
				return newTestCtrl(usecasetest.New[testEntity](t))
			}),
			fx.NopLogger,
		)
		require.NoError(t, app.Start(t.Context()))
		t.Cleanup(func() { _ = app.Stop(t.Context()) })

		assert.Equal(t, 1, (<-app.Wait()).ExitCode)
	})
}
