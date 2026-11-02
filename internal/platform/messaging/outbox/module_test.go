package outbox_test

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
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox/outboxtest"
	"github.com/sergisimo/ledger/internal/platform/persistence"
)

func TestModule(t *testing.T) {
	t.Setenv("SERVICE_NAME", "outbox-test")

	store := outboxtest.NewStore(t)

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
		outbox.Module(outbox.RelayConfig{PollInterval: 2 * time.Millisecond, Retention: time.Hour}, inmem.PublisherName),
		fx.Supply(
			fx.Annotate(store.Store, fx.As(new(outbox.Store))),
			fx.Annotate(store.Transactioner, fx.As(new(persistence.Transactioner))),
		),
		messaging.SubscriberFx(newSubscriber, "ledger.accounts"),
		messaging.PublisherFx[event]("ledger.accounts", outbox.PublisherName),
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
		t.Fatal("the subscriber never received the event relayed through the outbox")
	}

	require.Eventually(t, func() bool {
		return store.DispatchedCount(t) == 1
	}, time.Second, time.Millisecond, "the outbox row should be marked dispatched")
}
