// Package outboxtest helps write outbox tests against a real database: it
// starts a Postgres container, migrates the outbox schema and exposes the row
// state for assertions. Code that only needs to avoid a database can mock
// messaging.MessagePublisher instead.
package outboxtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/orlangure/gnomock"
	pgpreset "github.com/orlangure/gnomock/preset/postgres"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox/outboxsql"
	"github.com/sergisimo/ledger/internal/platform/persistence"
	persistencesqldb "github.com/sergisimo/ledger/internal/platform/persistence/sqldb"
)

type (
	// Store is a database-backed outbox.Store for tests, bundling the gorm
	// database and a real Transactioner to run transactional assertions.
	Store struct {
		outbox.Store

		DB            *gorm.DB
		Transactioner persistence.Transactioner
	}

	// Row is the stored state of one outbox message.
	Row struct {
		Position      int64
		ID            string
		Topic         string
		Attempts      int
		NextAttemptAt time.Time
		LastError     string
		DispatchedAt  *time.Time
	}
)

// NewStore returns a Store on a fresh Postgres container, migrated and torn
// down at cleanup. Containers are slow to start: call it once per test
// function and Reset between subtests.
func NewStore(t *testing.T) *Store {
	t.Helper()

	return NewStoreWithDB(t, NewDB(t))
}

// NewStoreWithDB returns a Store on db — bring your own database or dialect —
// migrating the outbox schema.
func NewStoreWithDB(t *testing.T, db *gorm.DB) *Store {
	t.Helper()

	require.NoError(t, outboxsql.AutoMigrate(db), "failed to migrate the outbox schema")

	return &Store{
		Store:         outboxsql.NewStore(db),
		DB:            db,
		Transactioner: persistencesqldb.NewTransactioner(db),
	}
}

// NewDB returns a gorm connection to a fresh Postgres container stopped at
// cleanup.
func NewDB(t *testing.T) *gorm.DB {
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

	db, err := persistencesqldb.NewClient(persistencesqldb.WithDialector(postgres.Open(dsn)))
	require.NoError(t, err, "failed to connect to the postgres container")

	return db
}

// Reset empties the outbox table.
func (s *Store) Reset(t *testing.T) {
	t.Helper()

	require.NoError(t, s.DB.Exec(fmt.Sprintf("DELETE FROM %s", outboxsql.TableName)).Error)
}

// Rows returns every stored message in insertion order.
func (s *Store) Rows(t *testing.T) []Row {
	t.Helper()

	var rows []Row
	require.NoError(t, s.DB.Table(outboxsql.TableName).Order("position").Find(&rows).Error)

	return rows
}

// Row returns the stored message with id and whether it exists.
func (s *Store) Row(t *testing.T, id string) (Row, bool) {
	t.Helper()

	for _, row := range s.Rows(t) {
		if row.ID == id {
			return row, true
		}
	}

	return Row{}, false
}

// Count returns the number of stored messages, dispatched or not.
func (s *Store) Count(t *testing.T) int {
	t.Helper()

	return len(s.Rows(t))
}

// DispatchedCount returns the number of stored messages marked dispatched.
func (s *Store) DispatchedCount(t *testing.T) int {
	t.Helper()

	count := 0
	for _, row := range s.Rows(t) {
		if row.DispatchedAt != nil {
			count++
		}
	}

	return count
}
