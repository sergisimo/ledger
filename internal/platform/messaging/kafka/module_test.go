package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging/kafka"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
)

func TestModule(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "acme.module.accounts"))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	messagingtest.AssertModule(t, messagingtest.ModuleCase{
		Topic:         "acme.module.accounts",
		ConsumerName:  "account-mailer",
		PublisherName: kafka.PublisherName,
		System:        kafka.SystemName,
		Options: []fx.Option{
			kafka.Module(kafka.Config{Brokers: cluster.ListenAddrs(), ClientID: "kafka-test"}),
			kafka.SubscriberFx("account-mailer", "acme.module.accounts", kafka.SubscriberWithMaxAttempts(3)),
		},
	})
}
