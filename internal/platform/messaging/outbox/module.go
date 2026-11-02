package outbox

import (
	"context"
	"fmt"

	"go.uber.org/fx"
)

// PublisherName is the fx name tag under which Module provides the outbox
// publisher as a messaging.MessagePublisher.
const PublisherName = "outboxPublisher"

// Module provides the outbox publisher as the messaging.MessagePublisher named
// PublisherName and runs a Relay delivering to the MessagePublisher named
// brokerPublisherName (e.g. inmem.PublisherName). It requires an outbox.Store
// (e.g. outboxsql.StoreFx) and a persistence.Transactioner. List it after the
// broker module.
func Module(cfg RelayConfig, brokerPublisherName string) fx.Option {
	return fx.Module(
		"outbox",
		fx.Supply(cfg),
		fx.Provide(
			fx.Annotate(NewPublisher, fx.ResultTags(fmt.Sprintf("name:%q", PublisherName))),
			fx.Annotate(NewRelay, fx.ParamTags("", fmt.Sprintf("name:%q", brokerPublisherName))),
		),
		fx.Invoke(func(relay *Relay, lf fx.Lifecycle) {
			lf.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return relay.Start()
				},
				OnStop: func(ctx context.Context) error {
					return relay.Stop(ctx)
				},
			})
		}),
	)
}
