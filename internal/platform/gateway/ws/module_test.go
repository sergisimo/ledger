package ws_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/sergisimo/ledger/internal/platform/authx"
	"github.com/sergisimo/ledger/internal/platform/authx/authxtest"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws/wstest"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

func TestModule(t *testing.T) {
	t.Run("starts and stops", func(t *testing.T) {
		t.Setenv("WS_ADDRESS", "127.0.0.1:0")
		t.Setenv("SERVICE_NAME", "ws-test")

		app := fxtest.New(t,
			logging.Module(),
			ws.Module(time.Second),
			fx.Provide(fx.Annotate(func() authx.Authenticator { return authxtest.NewAuthenticator(0) }, fx.As(new(authx.Authenticator)))),
			fx.NopLogger,
		)

		app.RequireStart()
		app.RequireStop()
	})

	t.Run("wires the subscription handler and typed publishers", func(t *testing.T) {
		t.Setenv("WS_ADDRESS", "127.0.0.1:0")
		t.Setenv("SERVICE_NAME", "ws-test")

		type event struct {
			Name string `json:"name"`
		}

		var (
			server    *ws.Server
			publisher ws.Publisher[event]
		)
		app := fxtest.New(t,
			logging.Module(),
			ws.Module(time.Second),
			ws.SubscriptionHandlerFx(wstest.NewSubscriptionHandler),
			ws.PublisherFx[event](),
			fx.Provide(fx.Annotate(func() authx.Authenticator { return authxtest.NewAuthenticator(0) }, fx.As(new(authx.Authenticator)))),
			fx.Populate(&server, &publisher),
			fx.NopLogger,
		)

		app.RequireStart()
		defer app.RequireStop()

		require.NotNil(t, publisher)
		delivered, err := publisher.Publish(t.Context(), "nobody", event{Name: "hello"})
		require.NoError(t, err)
		assert.Equal(t, 0, delivered)
	})

	t.Run("shuts the app down when it cannot listen", func(t *testing.T) {
		t.Setenv("WS_ADDRESS", "invalid:address:99999")
		t.Setenv("SERVICE_NAME", "ws-test")

		app := fx.New(
			logging.Module(),
			ws.Module(time.Second),
			fx.Provide(fx.Annotate(func() authx.Authenticator { return authxtest.NewAuthenticator(0) }, fx.As(new(authx.Authenticator)))),
			fx.NopLogger,
		)
		require.NoError(t, app.Start(t.Context()))
		t.Cleanup(func() { _ = app.Stop(t.Context()) })

		assert.Equal(t, 1, (<-app.Wait()).ExitCode)
	})
}
