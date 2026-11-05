package outboxsql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/orlangure/gnomock"
	mysqlpreset "github.com/orlangure/gnomock/preset/mysql"
	pgpreset "github.com/orlangure/gnomock/preset/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox/outboxsql"
	persistencesqldb "github.com/sergisimo/ledger/internal/platform/persistence/sqldb"
)

type backend struct {
	name  string
	start func(t *testing.T) gorm.Dialector
}

func backends() []backend {
	return []backend{
		{name: "postgres", start: startPostgres},
		{name: "mysql", start: startMySQL},
	}
}

func startPostgres(t *testing.T) gorm.Dialector {
	t.Helper()

	container, err := gnomock.Start(
		pgpreset.Preset(
			pgpreset.WithUser("acme", "acme"),
			pgpreset.WithDatabase("acme"),
			pgpreset.WithVersion("18.6"),
		),
	)
	require.NoError(t, err, "failed to start postgres container")

	t.Cleanup(func() {
		require.NoError(t, gnomock.Stop(container), "failed to stop postgres container")
	})

	dsn := fmt.Sprintf(
		"host=%s port=%d user=acme password=acme dbname=acme sslmode=disable",
		container.Host, container.DefaultPort(),
	)

	return postgres.Open(dsn)
}

func startMySQL(t *testing.T) gorm.Dialector {
	t.Helper()

	container, err := gnomock.Start(
		mysqlpreset.Preset(
			mysqlpreset.WithUser("acme", "acme"),
			mysqlpreset.WithDatabase("acme"),
		),
	)
	require.NoError(t, err, "failed to start mysql container")

	t.Cleanup(func() {
		require.NoError(t, gnomock.Stop(container), "failed to stop mysql container")
	})

	dsn := fmt.Sprintf(
		"acme:acme@tcp(%s:%d)/acme?charset=utf8mb4&parseTime=True",
		container.Host, container.DefaultPort(),
	)

	return mysql.Open(dsn)
}

func newMessage(topic string) messaging.Message {
	msg := messaging.NewMessage(topic, []byte(`{"name":"checking"}`))
	msg.Key = "account-1"
	msg.Headers[messaging.HeaderContentType] = messaging.ContentTypeJSON

	return msg
}

func rowCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()

	var count int64
	require.NoError(t, db.Table(outboxsql.TableName).Count(&count).Error)

	return count
}

func TestStore(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			db, err := persistencesqldb.NewClient(persistencesqldb.WithDialector(b.start(t)))
			require.NoError(t, err)
			require.NoError(t, outboxsql.AutoMigrate(db))

			store := outboxsql.NewStore(db)
			txer := persistencesqldb.NewTransactioner(db)

			cleanup := func(t *testing.T) {
				t.Helper()
				require.NoError(t, db.Exec(fmt.Sprintf("DELETE FROM %s", outboxsql.TableName)).Error)
			}

			t.Run("save joins the ambient transaction", func(t *testing.T) {
				t.Cleanup(func() { cleanup(t) })

				err := txer.Exec(t.Context(), func(txCtx context.Context) error {
					require.NoError(t, store.Save(txCtx, newMessage("acme.accounts")))
					return assert.AnError
				})
				require.ErrorIs(t, err, assert.AnError)
				assert.Zero(t, rowCount(t, db), "a rolled back transaction should discard the outbox rows")

				require.NoError(t, txer.Exec(t.Context(), func(txCtx context.Context) error {
					return store.Save(txCtx, newMessage("acme.accounts"), newMessage("acme.entries"))
				}))
				assert.Equal(t, int64(2), rowCount(t, db))
			})

			t.Run("save accepts a message without payload or headers", func(t *testing.T) {
				t.Cleanup(func() { cleanup(t) })

				msg := messaging.Message{ID: uuid.NewString(), Topic: "acme.accounts"}
				require.NoError(t, store.Save(t.Context(), msg))

				require.NoError(t, txer.Exec(t.Context(), func(txCtx context.Context) error {
					pending, err := store.FetchPending(txCtx, time.Now(), 10, 0)
					require.NoError(t, err)
					require.Len(t, pending, 1)

					assert.Equal(t, msg.ID, pending[0].ID)
					assert.Empty(t, pending[0].Payload)
					assert.Empty(t, pending[0].Headers)

					return nil
				}))
			})

			t.Run("fetch pending requires a transaction", func(t *testing.T) {
				_, err := store.FetchPending(t.Context(), time.Now(), 10, 0)
				require.ErrorIs(t, err, fields.NewErrNil(outboxsql.FieldNameTransaction))
			})

			t.Run("fetch pending returns due messages in insertion order", func(t *testing.T) {
				t.Cleanup(func() { cleanup(t) })

				first := newMessage("acme.accounts")
				delayed := newMessage("acme.accounts")
				third := newMessage("acme.entries")
				require.NoError(t, store.Save(t.Context(), first, delayed, third))
				require.NoError(t, store.Reschedule(t.Context(), delayed.ID, 1, time.Now().Add(time.Hour), "broker down"))

				require.NoError(t, txer.Exec(t.Context(), func(txCtx context.Context) error {
					pending, err := store.FetchPending(txCtx, time.Now(), 10, 0)
					require.NoError(t, err)
					require.Len(t, pending, 2)

					assert.Equal(t, first.ID, pending[0].ID)
					assert.Equal(t, third.ID, pending[1].ID)
					assert.Equal(t, first.Topic, pending[0].Topic)
					assert.Equal(t, first.Key, pending[0].Key)
					assert.Equal(t, first.Headers, pending[0].Headers)
					assert.Equal(t, first.Payload, pending[0].Payload)
					assert.Zero(t, pending[0].Attempts)

					limited, err := store.FetchPending(txCtx, time.Now(), 1, 0)
					require.NoError(t, err)
					require.Len(t, limited, 1)
					assert.Equal(t, first.ID, limited[0].ID)

					return nil
				}))
			})

			t.Run("fetch pending honours maxAttempts", func(t *testing.T) {
				t.Cleanup(func() { cleanup(t) })

				msg := newMessage("acme.accounts")
				require.NoError(t, store.Save(t.Context(), msg))
				require.NoError(t, store.Reschedule(t.Context(), msg.ID, 3, time.Now().Add(-time.Hour), "broker down"))

				require.NoError(t, txer.Exec(t.Context(), func(txCtx context.Context) error {
					exhausted, err := store.FetchPending(txCtx, time.Now(), 10, 3)
					require.NoError(t, err)
					assert.Empty(t, exhausted)

					unlimited, err := store.FetchPending(txCtx, time.Now(), 10, 0)
					require.NoError(t, err)
					require.Len(t, unlimited, 1)
					assert.Equal(t, 3, unlimited[0].Attempts)

					return nil
				}))
			})

			t.Run("skip locked partitions concurrent relays", func(t *testing.T) {
				t.Cleanup(func() { cleanup(t) })

				first := newMessage("acme.accounts")
				second := newMessage("acme.accounts")
				require.NoError(t, store.Save(t.Context(), first, second))

				require.NoError(t, txer.Exec(t.Context(), func(txCtx context.Context) error {
					locked, err := store.FetchPending(txCtx, time.Now(), 1, 0)
					require.NoError(t, err)
					require.Len(t, locked, 1)
					assert.Equal(t, first.ID, locked[0].ID)

					return txer.Exec(context.WithoutCancel(t.Context()), func(otherCtx context.Context) error {
						remaining, err := store.FetchPending(otherCtx, time.Now(), 10, 0)
						require.NoError(t, err)
						require.Len(t, remaining, 1, "the second relay should only see unlocked rows")
						assert.Equal(t, second.ID, remaining[0].ID)

						return nil
					})
				}))
			})

			t.Run("mark dispatched hides rows and retention prunes them", func(t *testing.T) {
				t.Cleanup(func() { cleanup(t) })

				dispatched := newMessage("acme.accounts")
				pending := newMessage("acme.accounts")
				require.NoError(t, store.Save(t.Context(), dispatched, pending))
				require.NoError(t, store.MarkDispatched(t.Context(), time.Now(), dispatched.ID))

				require.NoError(t, txer.Exec(t.Context(), func(txCtx context.Context) error {
					due, err := store.FetchPending(txCtx, time.Now(), 10, 0)
					require.NoError(t, err)
					require.Len(t, due, 1)
					assert.Equal(t, pending.ID, due[0].ID)

					return nil
				}))

				deleted, err := store.DeleteDispatchedBefore(t.Context(), time.Now().Add(time.Hour))
				require.NoError(t, err)
				assert.Equal(t, int64(1), deleted)
				assert.Equal(t, int64(1), rowCount(t, db), "pending rows must survive the janitor")
			})

			t.Run("delete removes rows by id", func(t *testing.T) {
				t.Cleanup(func() { cleanup(t) })

				msg := newMessage("acme.accounts")
				keep := newMessage("acme.accounts")
				require.NoError(t, store.Save(t.Context(), msg, keep))

				require.NoError(t, store.Delete(t.Context(), msg.ID))
				assert.Equal(t, int64(1), rowCount(t, db))
			})
		})
	}
}
