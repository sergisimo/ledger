package sqldb_test

import (
	"fmt"
	"testing"

	"github.com/orlangure/gnomock"
	mysqlpreset "github.com/orlangure/gnomock/preset/mysql"
	pgpreset "github.com/orlangure/gnomock/preset/postgres"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/sergisimo/ledger/internal/platform/persistence/sqldb"
)

const (
	dialectPostgres = "postgres"
	dialectMySQL    = "mysql"
)

type backend struct {
	name    string
	dialect string
	start   func(t *testing.T) gorm.Dialector
	setup   func(t *testing.T, db *gorm.DB)
}

func backends() []backend {
	return []backend{
		{
			name:    dialectPostgres,
			dialect: dialectPostgres,
			start:   startPostgres,
			setup: func(t *testing.T, db *gorm.DB) {
				t.Helper()
				err := db.Exec("ALTER TABLE users ALTER COLUMN tags TYPE text[] USING tags::text[]").Error
				require.NoError(t, err, "failed to turn the tags column into an array")
			},
		},
		{
			name:    dialectMySQL,
			dialect: dialectMySQL,
			start:   startMySQL,
		},
	}
}

func unsupportedOn(dialects ...string) map[string]error {
	errs := make(map[string]error, len(dialects))
	for _, dialect := range dialects {
		errs[dialect] = sqldb.ErrUnsupportedOperator
	}

	return errs
}

func caseErr(b backend, wantErr error, wantErrOn map[string]error) error {
	if err, ok := wantErrOn[b.dialect]; ok {
		return err
	}

	return wantErr
}

func startPostgres(t *testing.T) gorm.Dialector {
	t.Helper()

	container, err := gnomock.Start(
		pgpreset.Preset(
			pgpreset.WithUser("ledger", "ledger"),
			pgpreset.WithDatabase("ledger"),
			pgpreset.WithVersion("18.6"),
		),
	)
	require.NoError(t, err, "failed to start postgres container")

	t.Cleanup(func() {
		err := gnomock.Stop(container)
		require.NoError(t, err, "failed to stop postgres container")
	})

	dsn := fmt.Sprintf(
		"host=%s port=%d user=ledger password=ledger dbname=ledger sslmode=disable",
		container.Host, container.DefaultPort(),
	)

	return postgres.Open(dsn)
}

func startMySQL(t *testing.T) gorm.Dialector {
	t.Helper()

	container, err := gnomock.Start(
		mysqlpreset.Preset(
			mysqlpreset.WithUser("ledger", "ledger"),
			mysqlpreset.WithDatabase("ledger"),
		),
	)
	require.NoError(t, err, "failed to start mysql container")

	t.Cleanup(func() {
		err := gnomock.Stop(container)
		require.NoError(t, err, "failed to stop mysql container")
	})

	dsn := fmt.Sprintf(
		"ledger:ledger@tcp(%s:%d)/ledger?charset=utf8mb4&parseTime=True",
		container.Host, container.DefaultPort(),
	)

	return mysql.Open(dsn)
}
