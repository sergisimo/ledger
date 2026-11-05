package rabbitmq

import (
	"context"
	"errors"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

const (
	// SystemName is the messaging.system reported on this package's spans
	// (semconv.MessagingSystemRabbitMQ); pass it to messaging.PublisherFx.
	SystemName = "rabbitmq"

	// PublisherName is the fx name tag under which Module provides the
	// Publisher as a messaging.MessagePublisher.
	PublisherName = "rabbitmqPublisher"
)

// SubscriberFx provides a Subscriber consuming topic as the consumer named
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

// Module provides the *Client owning the service's shared connection plus the
// Publisher as the messaging.MessagePublisher named PublisherName, dialing
// the connection and attaching the publisher on start and releasing both on
// stop. List it before outbox.Module and any SubscriberFx entries.
func Module(cfg Config) fx.Option {
	return fx.Module(
		"rabbitmq",
		fx.Supply(cfg),
		fx.Provide(
			NewClient,
			NewPublisher,
			fx.Annotate(
				func(pub *Publisher) messaging.MessagePublisher { return pub },
				fx.ResultTags(messaging.NameTag(PublisherName)),
			),
		),
		fx.Invoke(func(client *Client, pub *Publisher, lf fx.Lifecycle) {
			lf.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					if err := client.Open(ctx); err != nil {
						return err
					}
					return pub.Open(ctx)
				},
				OnStop: func(ctx context.Context) error {
					return errors.Join(pub.Close(ctx), client.Close(ctx))
				},
			})
		}),
	)
}
