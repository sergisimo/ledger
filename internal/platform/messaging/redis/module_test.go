package redis_test

import (
	"testing"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/messaging/redis"
)

func TestModule(t *testing.T) {
	messagingtest.AssertModule(t, messagingtest.ModuleCase{
		Topic:         "acme.module.accounts",
		ConsumerName:  "account-mailer",
		PublisherName: redis.PublisherName,
		System:        redis.SystemName,
		Options: []fx.Option{
			redis.Module(redis.Config{URL: testURL}),
			redis.SubscriberFx("account-mailer", "acme.module.accounts", redis.SubscriberWithMaxAttempts(3)),
		},
	})
}
