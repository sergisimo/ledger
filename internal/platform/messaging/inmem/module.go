package inmem

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// PublisherName is the fx name tag under which Module provides the broker as a
// messaging.MessagePublisher.
const PublisherName = "inmemPublisher"

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
				fx.ResultTags(fmt.Sprintf("name:%q", PublisherName)),
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
