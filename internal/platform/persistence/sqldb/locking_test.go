package sqldb_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/persistence"
	"github.com/sergisimo/ledger/internal/platform/persistence/sqldb"
	"github.com/sergisimo/ledger/internal/platform/query"
)

// waitTimeout bounds every step that is expected to make progress on its own, so a lock
// that is never released fails the test instead of hanging it.
const waitTimeout = 10 * time.Second

func lockFirstUser(opts ...query.SrchOption) []query.SrchOption {
	return append([]query.SrchOption{query.Filter(query.Where(fields.NameID, filter.OpEq, 1))}, opts...)
}

// --------------------------------------------------------------- Translation

func TestRepoApplySearchLocking(t *testing.T) {
	users := seedUsers()

	tests := []struct {
		name    string
		opts    []query.SrchOption
		want    []user
		wantErr error
	}{
		{
			name: "for update",
			opts: lockFirstUser(query.Lock(query.LockStrengthUpdate)),
			want: []user{users[0]},
		},
		{
			name: "for share",
			opts: lockFirstUser(query.Lock(query.LockStrengthShare)),
			want: []user{users[0]},
		},
		{
			name: "for update nowait",
			opts: lockFirstUser(query.Lock(query.LockStrengthUpdate, query.LockWaitOn(query.LockWaitError))),
			want: []user{users[0]},
		},
		{
			name: "for update skip locked",
			opts: lockFirstUser(query.Lock(query.LockStrengthUpdate, query.LockWaitOn(query.LockWaitSkip))),
			want: []user{users[0]},
		},
		{
			name: "for share skip locked",
			opts: lockFirstUser(query.Lock(query.LockStrengthShare, query.LockWaitOn(query.LockWaitSkip))),
			want: []user{users[0]},
		},
		{
			name: "locking combines with sorting and pagination",
			opts: []query.SrchOption{
				query.Filter(query.Where(fieldCountry, filter.OpEq, "US")),
				query.SortBy(fieldAge, query.SortAsc),
				query.Pagination(1, 0),
				query.Lock(query.LockStrengthUpdate),
			},
			want: []user{users[4]},
		},
		{
			name:    "out of range strength is rejected",
			opts:    lockFirstUser(query.Lock(query.LockStrength(99))),
			wantErr: sqldb.ErrInvalidLocking,
		},
		{
			name:    "out of range wait is rejected",
			opts:    lockFirstUser(query.Lock(query.LockStrengthUpdate, query.LockWaitOn(query.LockWait(99)))),
			wantErr: sqldb.ErrInvalidLocking,
		},
	}

	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			repo := setupRepo(t, b)
			transactioner := sqldb.NewTransactioner(repo.DB)

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					var got []user
					err := transactioner.Exec(t.Context(), func(txCtx context.Context) error {
						return repo.ApplySearch(txCtx, query.NewSearch(tc.opts...)).Find(&got).Error
					})

					require.ErrorIs(t, err, tc.wantErr)
					assert.Equal(t, tc.want, got)
				})
			}
		})
	}
}

// --------------------------------------------------------------- Contention

func holdLock(t *testing.T, repo *sqldb.Repo) (release func() error) {
	t.Helper()

	locked := make(chan struct{})
	releaseCh := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- sqldb.NewTransactioner(repo.DB).Exec(context.WithoutCancel(t.Context()), func(txCtx context.Context) error {
			var got []user
			if err := repo.ApplySearch(
				txCtx,
				query.NewSearch(lockFirstUser(query.Lock(query.LockStrengthUpdate))...),
			).Find(&got).Error; err != nil {
				return err
			}

			close(locked)
			<-releaseCh

			return repo.ApplyPatch(txCtx, &user{}, query.NewPatch(
				query.Patch(fields.NameName, "holder"),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
			)).Error
		})
	}()

	select {
	case <-locked:
	case err := <-done:
		require.FailNow(t, "the lock holder finished before taking the lock", "err: %v", err)
	case <-time.After(waitTimeout):
		require.FailNow(t, "timed out waiting for the lock to be taken")
	}

	var (
		once    sync.Once
		holdErr error
	)
	release = func() error {
		once.Do(func() {
			close(releaseCh)
			holdErr = <-done
		})

		return holdErr
	}
	t.Cleanup(func() { _ = release() })

	return release
}

func TestRepoApplySearchWithLock(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			repo := setupRepo(t, b)
			transactioner := sqldb.NewTransactioner(repo.DB)

			// A locking read is refused outside a transaction, it would be released before
			// the caller could rely on it.
			var got []user
			err := repo.ApplySearch(
				t.Context(),
				query.NewSearch(lockFirstUser(query.Lock(query.LockStrengthUpdate))...),
			).Find(&got).Error
			require.ErrorIs(t, err, sqldb.ErrLockingWithoutTx)

			release := holdLock(t, repo)

			// NOWAIT fails instead of waiting for the held lock.
			err = transactioner.Exec(t.Context(), func(txCtx context.Context) error {
				var users []user
				return repo.ApplySearch(txCtx, query.NewSearch(
					lockFirstUser(query.Lock(query.LockStrengthUpdate, query.LockWaitOn(query.LockWaitError)))...,
				)).Find(&users).Error
			})
			require.Error(t, err, "locking a row held by another transaction must fail with NOWAIT")

			// SKIP LOCKED leaves the held row out and returns the rest.
			var skipped []user
			err = transactioner.Exec(t.Context(), func(txCtx context.Context) error {
				return repo.ApplySearch(txCtx, query.NewSearch(
					query.Filter(query.Where(fields.NameID, filter.OpIn, []int{1, 2})),
					query.SortBy(fields.NameID, query.SortAsc),
					query.Lock(query.LockStrengthUpdate, query.LockWaitOn(query.LockWaitSkip)),
				)).Find(&skipped).Error
			})
			require.NoError(t, err)
			assert.Equal(t, []user{seedUsers()[1]}, skipped)

			// A blocking read waits for the lock instead of failing.
			waiterDone := make(chan error, 1)
			waiterGot := make(chan []user, 1)
			go func() {
				var users []user
				waiterDone <- sqldb.NewTransactioner(repo.DB).Exec(
					context.WithoutCancel(t.Context()),
					func(txCtx context.Context) error {
						err := repo.ApplySearch(txCtx, query.NewSearch(
							lockFirstUser(query.Lock(query.LockStrengthUpdate))...,
						)).Find(&users).Error
						waiterGot <- users

						return err
					},
				)
			}()

			select {
			case err := <-waiterDone:
				require.FailNow(t, "the waiter acquired a lock still held by another transaction", "err: %v", err)
			case <-time.After(100 * time.Millisecond):
			}

			require.NoError(t, release(), "the lock holder failed")

			select {
			case err := <-waiterDone:
				require.NoError(t, err)
			case <-time.After(waitTimeout):
				require.FailNow(t, "timed out waiting for the lock to be released")
			}

			// Once unblocked the waiter reads the row as the holder left it, which is the
			// lock-read-then-update flow the locking configuration exists for.
			waiterUsers := <-waiterGot
			require.Len(t, waiterUsers, 1)
			assert.Equal(t, "holder", waiterUsers[0].Name)
		})
	}
}

// --------------------------------------------------------------- Transactioner

func txOf(t *testing.T, ctx context.Context) *gorm.DB {
	t.Helper()

	tx, found := persistence.TxFromCtx[*gorm.DB](ctx)
	require.True(t, found, "the context must carry a transaction")

	return tx
}

func renameFirstUser(name string) query.PatchQuery {
	return query.NewPatch(
		query.Patch(fields.NameName, name),
		query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
	)
}

func TestTransactioner(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			repo := setupRepo(t, b)
			transactioner := sqldb.NewTransactioner(repo.DB)

			t.Run("commits when the function succeeds", func(t *testing.T) {
				reseedUsers(t, repo)

				err := transactioner.Exec(t.Context(), func(txCtx context.Context) error {
					return repo.ApplyPatch(txCtx, &user{}, renameFirstUser("committed")).Error
				})
				require.NoError(t, err)

				var got user
				require.NoError(t, repo.DB.First(&got, 1).Error)
				assert.Equal(t, "committed", got.Name)
			})

			t.Run("rolls back when the function fails", func(t *testing.T) {
				reseedUsers(t, repo)

				err := transactioner.Exec(t.Context(), func(txCtx context.Context) error {
					if err := repo.ApplyPatch(txCtx, &user{}, renameFirstUser("rolled back")).Error; err != nil {
						return err
					}

					return assert.AnError
				})
				require.ErrorIs(t, err, assert.AnError)

				var got user
				require.NoError(t, repo.DB.First(&got, 1).Error)
				assert.Equal(t, seedUsers()[0].Name, got.Name)
			})

			t.Run("reuses the ongoing transaction", func(t *testing.T) {
				var outer, inner *gorm.DB
				err := transactioner.Exec(t.Context(), func(txCtx context.Context) error {
					outer = txOf(t, txCtx)

					return transactioner.Exec(txCtx, func(innerCtx context.Context) error {
						inner = txOf(t, innerCtx)

						return nil
					})
				})

				require.NoError(t, err)
				assert.Same(t, outer, inner, "the nested call must run on the transaction already open")
			})

			t.Run("runs on the client when the context has no transaction", func(t *testing.T) {
				reseedUsers(t, repo)

				_, found := persistence.TxFromCtx[*gorm.DB](t.Context())
				assert.False(t, found)

				var got []user
				require.NoError(t, repo.ApplySearch(t.Context(), query.NewSearch()).Find(&got).Error)
				assert.Len(t, got, len(seedUsers()))
			})
		})
	}
}
