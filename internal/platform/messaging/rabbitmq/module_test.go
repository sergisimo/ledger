package rabbitmq_test

import (
	"testing"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/messaging/rabbitmq"
)

func TestModule(t *testing.T) {
	messagingtest.AssertModule(t, messagingtest.ModuleCase{
		Topic:         "acme.module.accounts",
		ConsumerName:  "account-mailer",
		PublisherName: rabbitmq.PublisherName,
		System:        rabbitmq.SystemName,
		Options: []fx.Option{
			rabbitmq.Module(rabbitmq.Config{URL: testURL, ContainerID: "rabbitmq-test"}),
			rabbitmq.SubscriberFx("account-mailer", "acme.module.accounts", rabbitmq.SubscriberWithMaxAttempts(3)),
		},
	})
}
