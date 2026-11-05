package messaging_test

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/fx"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/inmem"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox/outboxsql"
	"github.com/sergisimo/ledger/internal/platform/persistence"
	"github.com/sergisimo/ledger/internal/platform/persistence/sqldb"
)

type AccountCreated struct {
	ID string `json:"id"`
}

// Example wires the in-memory broker behind a transactional outbox: use cases
// publish through a Publisher[AccountCreated] inside a database transaction,
// and the subscriber handles the event only after the transaction commits. To
// publish straight to the broker instead, drop outbox.Module and
// outboxsql.StoreFx and build the publisher from inmem.PublisherName.
func Example() {
	newAccountMailer := func() messaging.HandlerFunc {
		return messaging.Handle(func(_ context.Context, payload AccountCreated, _ messaging.Message) error {
			fmt.Println("account created:", payload.ID)
			return nil
		})
	}

	fx.New(
		logging.Module(),
		fx.Provide(
			func() (*gorm.DB, error) {
				return sqldb.NewClient(sqldb.WithDialector(postgres.Open("postgres://acme@localhost/acme")))
			},
			fx.Annotate(sqldb.NewTransactioner, fx.As(new(persistence.Transactioner))),
		),

		inmem.Module(10*time.Second),
		outboxsql.StoreFx(),
		outbox.Module(outbox.RelayConfig{Retention: 24 * time.Hour}, inmem.PublisherName),

		messaging.PublisherFx[AccountCreated]("acme.accounts", outbox.PublisherName, inmem.SystemName),
		messaging.HandlerFx(newAccountMailer, "account-mailer"),
		inmem.SubscriberFx("account-mailer", "acme.accounts"),

		fx.Invoke(func(txer persistence.Transactioner, pub messaging.Publisher[AccountCreated]) error {
			return txer.Exec(context.Background(), func(txCtx context.Context) error {
				// ... business writes sharing txCtx ...
				return pub.Publish(txCtx, AccountCreated{ID: "42"})
			})
		}),
	).Run()
}
