package messaging

import (
	"context"
	"fmt"

	"go.uber.org/fx"
)

// PublisherFx provides a Publisher[T] for topic backed by the
// MessagePublisher named publisherName (e.g. inmem.PublisherName to publish
// straight to the broker, outbox.PublisherName to publish through the outbox).
func PublisherFx[T any](topic, publisherName string) fx.Option {
	return fx.Provide(fx.Annotate(
		func(pub MessagePublisher) (Publisher[T], error) {
			return NewPublisher[T](pub, topic)
		},
		fx.ParamTags(fmt.Sprintf("name:%q", publisherName)),
	))
}

// SubscriberFx provides constructor's Subscriber under name:consumerName and
// subscribes it on application start and unsubscribes it on stop. The
// constructor can take any provided dependency (typically the broker) and must
// return a Subscriber implementation. List it after the broker module.
func SubscriberFx(constructor any, consumerName string) fx.Option {
	return fx.Module(
		fmt.Sprintf("%s-subscriber", consumerName),
		fx.Provide(fx.Annotate(
			constructor,
			fx.ResultTags(fmt.Sprintf("name:%q", consumerName)),
			fx.As(new(Subscriber)),
		)),
		fx.Invoke(fx.Annotate(func(sub Subscriber, lf fx.Lifecycle) {
			lf.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return sub.Subscribe(ctx, nil)
				},
				OnStop: func(ctx context.Context) error {
					return sub.Unsubscribe(ctx)
				},
			})
		}, fx.ParamTags(fmt.Sprintf("name:%q", consumerName)))),
	)
}
