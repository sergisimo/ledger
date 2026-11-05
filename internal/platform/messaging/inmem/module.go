package inmem

import (
	"context"
	"time"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// SubscriberFx provides a Subscriber for topic driven by the HandlerFunc
// provided under name:consumerName (see messaging.HandlerFx), subscribed and
// unsubscribed with the fx lifecycle. The *Broker comes from Module. List it
// after Module.
func SubscriberFx(consumerName, topic string, opts ...SubscriberOpt) fx.Option {
	return messaging.SubscriberFx(
		func(broker *Broker, h messaging.HandlerFunc) (*Subscriber, error) {
			return NewSubscriber(broker, topic, h, opts...)
		},
		consumerName,
		fx.ParamTags("", messaging.NameTag(consumerName)),
	)
}

// PublisherName is the fx name tag under which Module provides the broker as a
// messaging.MessagePublisher.
const PublisherName = "inmemPublisher"
const SystemName = "inmem"

// Module provides a *Broker plus the broker as the messaging.MessagePublisher
// named PublisherName, and starts and stops it with the fx lifecycle; Stop
// waits for in-flight handlers up to shutdownTimeout. List it before
// outbox.Module and any SubscriberFx entries.
func Module(shutdownTimeout time.Duration) fx.Option {
	return fx.Module(
		"inmem",
		fx.Provide(
			NewBroker,
			fx.Annotate(
				func(broker *Broker) messaging.MessagePublisher { return broker },
				fx.ResultTags(messaging.NameTag(PublisherName)),
			),
		),
		fx.Invoke(func(broker *Broker, lf fx.Lifecycle) {
			lf.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return broker.Start()
				},
				OnStop: func(ctx context.Context) error {
					ctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
					defer cancel()
					return broker.Stop(ctx)
				},
			})
		}),
	)
}
