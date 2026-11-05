package inmem_test

import (
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging/inmem"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
)

func TestModule(t *testing.T) {
	messagingtest.AssertModule(t, messagingtest.ModuleCase{
		Topic:         "acme.accounts",
		ConsumerName:  "account-mailer",
		PublisherName: inmem.PublisherName,
		System:        inmem.SystemName,
		Options: []fx.Option{
			inmem.Module(time.Second),
			inmem.SubscriberFx("account-mailer", "acme.accounts", inmem.SubscriberWithBuffer(8)),
		},
	})
}
