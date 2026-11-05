package nats_test

import (
	"testing"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/messaging/nats"
)

func TestModule(t *testing.T) {
	messagingtest.AssertModule(t, messagingtest.ModuleCase{
		Topic:         "acme.module.accounts",
		ConsumerName:  "account-mailer",
		PublisherName: nats.PublisherName,
		System:        nats.SystemName,
		Options: []fx.Option{
			nats.Module(nats.Config{URL: testURL, Name: "nats-test"}),
			nats.SubscriberFx("account-mailer", "acme.module.accounts", nats.SubscriberWithMaxAttempts(3)),
		},
	})
}
