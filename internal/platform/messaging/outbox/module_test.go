package outbox_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging/inmem"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox/outboxtest"
	"github.com/sergisimo/ledger/internal/platform/persistence"
)

func TestModule(t *testing.T) {
	store := outboxtest.NewStore(t)

	messagingtest.AssertModule(t, messagingtest.ModuleCase{
		Topic:         "acme.accounts",
		ConsumerName:  "account-mailer",
		PublisherName: outbox.PublisherName,
		System:        inmem.SystemName,
		Options: []fx.Option{
			inmem.Module(time.Second),
			outbox.Module(outbox.RelayConfig{PollInterval: 2 * time.Millisecond, Retention: time.Hour}, inmem.PublisherName),
			fx.Supply(
				fx.Annotate(store.Store, fx.As(new(outbox.Store))),
				fx.Annotate(store.Transactioner, fx.As(new(persistence.Transactioner))),
			),
			inmem.SubscriberFx("account-mailer", "acme.accounts"),
		},
	})

	require.Eventually(t, func() bool {
		return store.DispatchedCount(t) == 1
	}, time.Second, time.Millisecond, "the outbox row should be marked dispatched")
}
