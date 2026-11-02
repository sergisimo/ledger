package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/inmem"
)

func TestModule(t *testing.T) {
	t.Run("starts and stops", func(t *testing.T) {
		t.Setenv("SERVICE_NAME", "inmem-test")

		app := fxtest.New(t,
			logging.Module(),
			inmem.Module(time.Second),
			fx.NopLogger,
		)

		app.RequireStart()
		app.RequireStop()
	})

	t.Run("wires subscribers and typed publishers end to end", func(t *testing.T) {
		t.Setenv("SERVICE_NAME", "inmem-test")

		received := make(chan event, 1)
		newSubscriber := func(b *inmem.Broker) (*inmem.Subscriber, error) {
			return inmem.NewSubscriber(b, "ledger.accounts", messaging.Handle(
				func(_ context.Context, payload event, _ messaging.Message) error {
					received <- payload
					return nil
				},
			))
		}

		var pub messaging.Publisher[event]
		app := fxtest.New(t,
			logging.Module(),
			inmem.Module(time.Second),
			messaging.SubscriberFx(newSubscriber, "ledger.accounts"),
			messaging.PublisherFx[event]("ledger.accounts", inmem.PublisherName),
			fx.Populate(&pub),
			fx.NopLogger,
		)

		app.RequireStart()
		defer app.RequireStop()

		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))

		select {
		case got := <-received:
			assert.Equal(t, event{Name: "checking"}, got)
		case <-time.After(time.Second):
			t.Fatal("the subscriber never received the published event")
		}
	})
}
