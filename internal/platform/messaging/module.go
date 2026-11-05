package messaging

import (
	"context"
	"fmt"
	"slices"

	"go.uber.org/fx"
)

// NameTag returns the fx `name:"..."` tag for name. The fx helpers of this
// package and of the broker packages use it to tag the values they provide
// and consume, so callers can use it to inject or provide them by hand.
func NameTag(name string) string {
	return fmt.Sprintf("name:%q", name)
}

// PublisherFx provides a Publisher[T] for topic backed by the
// MessagePublisher named publisherName (e.g. inmem.PublisherName to publish
// straight to the broker, outbox.PublisherName to publish through the outbox).
func PublisherFx[T any](topic, publisherName, system string) fx.Option {
	return fx.Provide(fx.Annotate(
		func(pub MessagePublisher) (Publisher[T], error) {
			return NewPublisher[T](pub, topic, system)
		},
		fx.ParamTags(NameTag(publisherName)),
	))
}

// HandlerFx provides constructor's HandlerFunc under name:consumerName, where
// the broker packages' SubscriberFx helpers look for the handler of the
// subscription named consumerName. The constructor can take any provided
// dependency and must return a HandlerFunc, typically built with Handle:
//
//	messaging.HandlerFx(func(mailer *Mailer) messaging.HandlerFunc {
//		return messaging.Handle(mailer.OnAccountCreated)
//	}, "account-mailer")
//	kafka.SubscriberFx("account-mailer", "acme.accounts")
func HandlerFx(constructor any, consumerName string) fx.Option {
	return fx.Provide(fx.Annotate(constructor, fx.ResultTags(NameTag(consumerName))))
}

// SubscriberFx provides constructor's Subscriber under name:consumerName and
// subscribes it on application start and unsubscribes it on stop. The
// constructor can take any provided dependency (typically the broker) and must
// return a Subscriber implementation; annotations (e.g. fx.ParamTags) are
// applied to it. The broker packages offer SubscriberFx helpers that build
// the constructor for you from a handler provided with HandlerFx. List it
// after the broker module.
//
// consumerName is also the identity networked brokers group instances by
// (consumer group, durable, queue): replicas sharing the name compete for the
// topic's messages. If the application provides an unnamed OnErrorFunc, every
// subscription reports its failures to it; otherwise they are only logged.
func SubscriberFx(constructor any, consumerName string, annotations ...fx.Annotation) fx.Option {
	annotations = append(slices.Clone(annotations),
		fx.ResultTags(NameTag(consumerName)),
		fx.As(new(Subscriber)),
	)

	return fx.Module(
		fmt.Sprintf("%s-subscriber", consumerName),
		fx.Provide(fx.Annotate(constructor, annotations...)),
		fx.Invoke(fx.Annotate(func(sub Subscriber, onError OnErrorFunc, lf fx.Lifecycle) {
			lf.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return sub.Subscribe(ctx, onError)
				},
				OnStop: func(ctx context.Context) error {
					return sub.Unsubscribe(ctx)
				},
			})
		}, fx.ParamTags(NameTag(consumerName), `optional:"true"`))),
	)
}
