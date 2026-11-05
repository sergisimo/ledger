package kafka

import (
	"context"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

const (
	// SystemName is the messaging.system reported on this package's spans
	// (semconv.MessagingSystemKafka); pass it to messaging.PublisherFx.
	SystemName = "kafka"

	// PublisherName is the fx name tag under which Module provides the
	// Publisher as a messaging.MessagePublisher.
	PublisherName = "kafkaPublisher"
)

// SubscriberFx provides a Subscriber consuming topic as the consumer group
// consumerName, driven by the HandlerFunc provided under name:consumerName
// (see messaging.HandlerFx), subscribed and unsubscribed with the fx
// lifecycle. The *Client comes from Module. List it after Module.
func SubscriberFx(consumerName, topic string, opts ...SubscriberOpt) fx.Option {
	return messaging.SubscriberFx(
		func(client *Client, h messaging.HandlerFunc) (*Subscriber, error) {
			return NewSubscriber(client, topic, consumerName, h, opts...)
		},
		consumerName,
		fx.ParamTags("", messaging.NameTag(consumerName)),
	)
}

// Module provides a *Client for subscribers plus the Publisher as the
// messaging.MessagePublisher named PublisherName, verifying the connection on
// start and flushing it on stop. List it before outbox.Module and any
// SubscriberFx entries.
func Module(cfg Config) fx.Option {
	return fx.Module(
		"kafka",
		fx.Supply(cfg),
		fx.Provide(
			NewClient,
			NewPublisher,
			fx.Annotate(
				func(pub *Publisher) messaging.MessagePublisher { return pub },
				fx.ResultTags(messaging.NameTag(PublisherName)),
			),
		),
		fx.Invoke(func(pub *Publisher, lf fx.Lifecycle) {
			lf.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return pub.Ping(ctx)
				},
				OnStop: func(ctx context.Context) error {
					return pub.Close(ctx)
				},
			})
		}),
	)
}
