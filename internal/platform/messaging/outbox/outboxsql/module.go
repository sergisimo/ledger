package outboxsql

import (
	"go.uber.org/fx"

	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
)

// StoreFx provides a Store as the outbox.Store, built from the application's *gorm.DB.
func StoreFx() fx.Option {
	return fx.Provide(fx.Annotate(NewStore, fx.As(new(outbox.Store))))
}
