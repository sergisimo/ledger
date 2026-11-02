package outbox

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

type publisher struct {
	store Store
}

// NewPublisher returns a messaging.MessagePublisher that saves messages to
// store instead of delivering them; the Relay delivers them later. Most
// applications wire it with Module.
func NewPublisher(store Store) messaging.MessagePublisher {
	return &publisher{store: store}
}

func (p *publisher) Publish(ctx context.Context, msgs ...messaging.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	for _, msg := range msgs {
		if msg.Topic == "" {
			return fields.NewErrInvalidEmptyString(messaging.FieldNameTopic)
		}
	}

	return p.store.Save(ctx, msgs...)
}
