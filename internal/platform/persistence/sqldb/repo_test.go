package sqldb_test

import (
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/persistence/sqldb"
	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/utils/varutils"
)

// --------------------------------------------------------------- Fixtures

type user struct {
	ID       uint `gorm:"primaryKey"`
	Name     string
	Email    string
	Age      int
	Country  string
	Nickname *string
	Active   bool
	Tags     pq.StringArray `gorm:"type:text"`
}

const (
	fieldAge      fields.Name = "age"
	fieldCountry  fields.Name = "country"
	fieldNickname fields.Name = "nickname"
	fieldActive   fields.Name = "active"
	fieldTags     fields.Name = "tags"
	fieldUnknown  fields.Name = "unknown"
)

func fieldMapper() map[fields.Name]string {
	return map[fields.Name]string{
		fields.NameID:    "id",
		fields.NameName:  "name",
		fields.NameEmail: "email",
		fieldAge:         "age",
		fieldCountry:     "country",
		fieldNickname:    "nickname",
		fieldActive:      "active",
		fieldTags:        "tags",
	}
}

func seedUsers() []user {
	return []user{
		{
			ID:       1,
			Name:     "Ada",
			Email:    "ada@ledger.dev",
			Age:      36,
			Country:  "ES",
			Nickname: nil,
			Active:   true,
			Tags:     pq.StringArray{"go", "sql"},
		},
		{
			ID:       2,
			Name:     "Grace",
			Email:    "grace@ledger.dev",
			Age:      45,
			Country:  "US",
			Nickname: varutils.Ptr("amazing"),
			Active:   true,
			Tags:     pq.StringArray{"go"},
		},
		{
			ID:       3,
			Name:     "Alan",
			Email:    "alan@ledger.dev",
			Age:      41,
			Country:  "UK",
			Nickname: nil,
			Active:   false,
			Tags:     pq.StringArray{"math"},
		},
		{
			ID:       4,
			Name:     "Edsger",
			Email:    "edsger@ledger.dev",
			Age:      29,
			Country:  "NL",
			Nickname: varutils.Ptr("dijkstra"),
			Active:   true,
			Tags:     pq.StringArray{"sql"},
		},
		{
			ID:       5,
			Name:     "Barbara",
			Email:    "barbara@ledger.dev",
			Age:      36,
			Country:  "US",
			Nickname: varutils.Ptr("barb"),
			Active:   false,
			Tags:     pq.StringArray{"bio"},
		},
	}
}

func reseedUsers(t *testing.T, repo *sqldb.Repo) {
	t.Helper()

	require.NoError(t, repo.DB.Exec("DELETE FROM users").Error, "failed to clear the users table")
	require.NoError(t, repo.DB.Create(seedUsers()).Error, "failed to reseed the users table")
}

func setupRepo(t *testing.T, b backend) *sqldb.Repo {
	t.Helper()

	db, err := sqldb.NewClient(sqldb.WithDialector(b.start(t)))
	require.NoError(t, err, "failed to open the database")

	require.NoError(t, db.AutoMigrate(&user{}), "failed to migrate the schema")
	if b.setup != nil {
		b.setup(t, db)
	}
	require.NoError(t, db.Create(seedUsers()).Error, "failed to seed the database")

	return sqldb.NewRepo(db, fieldMapper())
}

// --------------------------------------------------------------- Tests

func TestRepoApplySearch(t *testing.T) {
	users := seedUsers()

	tests := []struct {
		name      string
		opts      []query.SrchOption
		want      []user
		wantErr   error
		wantErrOn map[string]error
	}{
		{
			name: "no filters returns every row",
			opts: nil,
			want: users,
		},
		{
			name: "empty search returns all rows",
			opts: []query.SrchOption{},
			want: users,
		},
		{
			name: "eq",
			opts: []query.SrchOption{query.Filter(query.Where(fields.NameName, filter.OpEq, "Ada"))},
			want: []user{users[0]},
		},
		{
			name: "neq",
			opts: []query.SrchOption{query.Filter(query.Where(fieldCountry, filter.OpNEq, "US"))},
			want: []user{users[0], users[2], users[3]},
		},
		{
			name: "gt",
			opts: []query.SrchOption{query.Filter(query.Where(fieldAge, filter.OpGT, 40))},
			want: []user{users[1], users[2]},
		},
		{
			name: "gteq",
			opts: []query.SrchOption{query.Filter(query.Where(fieldAge, filter.OpGTEq, 41))},
			want: []user{users[1], users[2]},
		},
		{
			name: "lt",
			opts: []query.SrchOption{query.Filter(query.Where(fieldAge, filter.OpLT, 36))},
			want: []user{users[3]},
		},
		{
			name: "lteq",
			opts: []query.SrchOption{query.Filter(query.Where(fieldAge, filter.OpLTEq, 36))},
			want: []user{users[0], users[3], users[4]},
		},
		{
			name: "in",
			opts: []query.SrchOption{query.Filter(query.Where(fieldCountry, filter.OpIn, []string{"US", "UK"}))},
			want: []user{users[1], users[2], users[4]},
		},
		{
			name: "not in",
			opts: []query.SrchOption{query.Filter(query.Where(fieldCountry, filter.OpNotIn, []string{"US", "UK"}))},
			want: []user{users[0], users[3]},
		},
		{
			name: "like",
			opts: []query.SrchOption{query.Filter(query.Where(fields.NameName, filter.OpLike, "A%"))},
			want: []user{users[0], users[2]},
		},
		{
			name: "between",
			opts: []query.SrchOption{query.Filter(query.Where(fieldAge, filter.OpBetween, []any{30, 40}))},
			want: []user{users[0], users[4]},
		},
		{
			name: "is null",
			opts: []query.SrchOption{query.Filter(query.Where[any](fieldNickname, filter.OpIs, nil))},
			want: []user{users[0], users[2]},
		},
		{
			name: "is not null",
			opts: []query.SrchOption{query.Filter(query.Where[any](fieldNickname, filter.OpIsNot, nil))},
			want: []user{users[1], users[3], users[4]},
		},
		{
			name: "is true",
			opts: []query.SrchOption{query.Filter(query.Where(fieldActive, filter.OpIs, true))},
			want: []user{users[0], users[1], users[3]},
		},
		{
			name: "eq against null renders is null",
			opts: []query.SrchOption{query.Filter(query.Where[any](fieldNickname, filter.OpEq, nil))},
			want: []user{users[0], users[2]},
		},
		{
			name:      "contains",
			opts:      []query.SrchOption{query.Filter(query.Where(fieldTags, filter.OpContains, []string{"go"}))},
			want:      []user{users[0], users[1]},
			wantErrOn: unsupportedOn(dialectMySQL),
		},
		{
			name:      "not contains",
			opts:      []query.SrchOption{query.Filter(query.Where(fieldTags, filter.OpNotContains, []string{"go"}))},
			want:      []user{users[2], users[3], users[4]},
			wantErrOn: unsupportedOn(dialectMySQL),
		},
		{
			name: "and groups every condition",
			opts: []query.SrchOption{query.Filter(query.And(
				query.Where(fieldCountry, filter.OpEq, "US"),
				query.Where(fieldAge, filter.OpGT, 40),
			))},
			want: []user{users[1]},
		},
		{
			name: "or groups every condition",
			opts: []query.SrchOption{query.Filter(query.Or(
				query.Where(fieldCountry, filter.OpEq, "NL"),
				query.Where(fieldCountry, filter.OpEq, "UK"),
			))},
			want: []user{users[2], users[3]},
		},
		{
			name: "nested and inside or keeps its grouping",
			opts: []query.SrchOption{query.Filter(query.Or(
				query.And(
					query.Where(fieldCountry, filter.OpEq, "US"),
					query.Where(fieldAge, filter.OpGT, 40),
				),
				query.Where(fields.NameName, filter.OpEq, "Ada"),
			))},
			want: []user{users[0], users[1]},
		},
		{
			name: "nested or inside and keeps its grouping",
			opts: []query.SrchOption{query.Filter(query.And(
				query.Or(
					query.Where(fieldCountry, filter.OpEq, "US"),
					query.Where(fieldCountry, filter.OpEq, "ES"),
				),
				query.Where(fieldAge, filter.OpEq, 36),
			))},
			want: []user{users[0], users[4]},
		},
		{
			name: "successive filter options are combined with and",
			opts: []query.SrchOption{
				query.Filter(query.Where(fieldCountry, filter.OpEq, "US")),
				query.Filter(query.Where(fieldActive, filter.OpIs, false)),
			},
			want: []user{users[4]},
		},
		{
			name: "sorting by several fields",
			opts: []query.SrchOption{
				query.SortBy(fieldAge, query.SortDesc),
				query.SortBy(fields.NameName, query.SortAsc),
			},
			want: []user{users[1], users[2], users[0], users[4], users[3]},
		},
		{
			name: "pagination limits and offsets the sorted result",
			opts: []query.SrchOption{
				query.SortBy(fields.NameID, query.SortAsc),
				query.Pagination(2, 1),
			},
			want: []user{users[1], users[2]},
		},
		{
			name: "filters sorting and pagination combined",
			opts: []query.SrchOption{
				query.Filter(query.Where(fieldAge, filter.OpGTEq, 36)),
				query.SortBy(fieldAge, query.SortAsc),
				query.SortBy(fields.NameName, query.SortAsc),
				query.Pagination(2, 1),
			},
			want: []user{users[4], users[2]},
		},
		{
			name:    "unmapped filter field is rejected",
			opts:    []query.SrchOption{query.Filter(query.Where(fieldUnknown, filter.OpEq, "x"))},
			wantErr: sqldb.ErrUnmappedField,
		},
		{
			name:    "unmapped sorting field is rejected",
			opts:    []query.SrchOption{query.SortBy(fieldUnknown, query.SortAsc)},
			wantErr: sqldb.ErrUnmappedField,
		},
		{
			name:    "between without two bounds is rejected",
			opts:    []query.SrchOption{query.Filter(query.Where(fieldAge, filter.OpBetween, []any{30}))},
			wantErr: sqldb.ErrInvalidFilterValue,
		},
		{
			name:    "in without a collection is rejected",
			opts:    []query.SrchOption{query.Filter(query.Where(fieldCountry, filter.OpIn, "US"))},
			wantErr: sqldb.ErrInvalidFilterValue,
		},
		{
			name:    "is against a non boolean is rejected",
			opts:    []query.SrchOption{query.Filter(query.Where(fieldAge, filter.OpIs, 36))},
			wantErr: sqldb.ErrInvalidFilterValue,
		},
	}

	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			repo := setupRepo(t, b)

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					wantErr := caseErr(b, tc.wantErr, tc.wantErrOn)

					var got []user
					err := repo.ApplySearch(t.Context(), query.NewSearch(tc.opts...)).Find(&got).Error
					require.ErrorIs(t, err, wantErr)

					if wantErr != nil {
						assert.Empty(t, got)
						return
					}
					assert.Equal(t, tc.want, got)
				})
			}
		})
	}
}

func TestRepoApplyPatch(t *testing.T) {
	patched := func(mutate func(us []user)) []user {
		us := seedUsers()
		mutate(us)
		return us
	}

	tests := []struct {
		name      string
		opts      []query.PatchOption
		want      []user
		wantErr   error
		wantErrOn map[string]error
	}{
		{
			name:    "nil patch is rejected",
			opts:    nil,
			want:    seedUsers(),
			wantErr: sqldb.ErrEmptyPatch,
		},
		{
			name:    "empty patch is rejected",
			opts:    []query.PatchOption{},
			want:    seedUsers(),
			wantErr: sqldb.ErrEmptyPatch,
		},
		{
			name: "set a single field",
			opts: []query.PatchOption{
				query.Patch(fields.NameName, "Countess"),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
			},
			want: patched(func(us []user) { us[0].Name = "Countess" }),
		},
		{
			name: "set several fields at once",
			opts: []query.PatchOption{
				query.Patch(fieldCountry, "CH"),
				query.Patch(fieldActive, true),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 3))),
			},
			want: patched(func(us []user) {
				us[2].Country = "CH"
				us[2].Active = true
			}),
		},
		{
			name: "set a field to null",
			opts: []query.PatchOption{
				query.Patch(fieldNickname, nil),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 2))),
			},
			want: patched(func(us []user) { us[1].Nickname = nil }),
		},
		{
			name: "set updates every filtered row",
			opts: []query.PatchOption{
				query.Patch(fieldActive, false),
				query.PatchSearchOpts(query.Filter(query.Where(fieldCountry, filter.OpEq, "US"))),
			},
			want: patched(func(us []user) {
				us[1].Active = false
				us[4].Active = false
			}),
		},
		{
			name: "add a number increments the column",
			opts: []query.PatchOption{
				query.Patch(fieldAge, 10, query.PatchFieldAdd),
				query.PatchSearchOpts(query.Filter(query.Where(fieldCountry, filter.OpEq, "US"))),
			},
			want: patched(func(us []user) {
				us[1].Age += 10
				us[4].Age += 10
			}),
		},
		{
			name: "add a collection appends to the array",
			opts: []query.PatchOption{
				query.Patch(fieldTags, pq.StringArray{"pioneer"}, query.PatchFieldAdd),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
			},
			want:      patched(func(us []user) { us[0].Tags = pq.StringArray{"go", "sql", "pioneer"} }),
			wantErrOn: unsupportedOn(dialectMySQL),
		},
		{
			name: "remove a number decrements the column",
			opts: []query.PatchOption{
				query.Patch(fieldAge, 5, query.PatchFieldRemove),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 3))),
			},
			want: patched(func(us []user) { us[2].Age -= 5 }),
		},
		{
			name: "remove a collection drops the elements from the array",
			opts: []query.PatchOption{
				query.Patch(fieldTags, pq.StringArray{"go"}, query.PatchFieldRemove),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
			},
			want:      patched(func(us []user) { us[0].Tags = pq.StringArray{"sql"} }),
			wantErrOn: unsupportedOn(dialectMySQL),
		},
		{
			name: "remove with null clears the column",
			opts: []query.PatchOption{
				query.Patch(fieldNickname, nil, query.PatchFieldRemove),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 4))),
			},
			want: patched(func(us []user) { us[3].Nickname = nil }),
		},
		{
			name: "complex filters select the patched rows",
			opts: []query.PatchOption{
				query.Patch(fieldCountry, "EU"),
				query.PatchSearchOpts(query.Filter(query.Or(
					query.Where(fieldCountry, filter.OpEq, "ES"),
					query.Where(fieldCountry, filter.OpEq, "NL"),
				))),
			},
			want: patched(func(us []user) {
				us[0].Country = "EU"
				us[3].Country = "EU"
			}),
		},
		{
			name: "patch without fields is rejected",
			opts: []query.PatchOption{
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
			},
			want:    seedUsers(),
			wantErr: sqldb.ErrEmptyPatch,
		},
		{
			name: "patch without filters is rejected",
			opts: []query.PatchOption{
				query.Patch(fieldActive, false),
			},
			want:    seedUsers(),
			wantErr: gorm.ErrMissingWhereClause,
		},
		{
			name: "unmapped patched field is rejected",
			opts: []query.PatchOption{
				query.Patch(fieldUnknown, "x"),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
			},
			want:    seedUsers(),
			wantErr: sqldb.ErrUnmappedField,
		},
		{
			name: "unmapped filter field is rejected",
			opts: []query.PatchOption{
				query.Patch(fieldActive, false),
				query.PatchSearchOpts(query.Filter(query.Where(fieldUnknown, filter.OpEq, "x"))),
			},
			want:    seedUsers(),
			wantErr: sqldb.ErrUnmappedField,
		},
		{
			name: "add with a non numeric non collection value is rejected",
			opts: []query.PatchOption{
				query.Patch(fields.NameName, "suffix", query.PatchFieldAdd),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
			},
			want:    seedUsers(),
			wantErr: sqldb.ErrInvalidPatchValue,
		},
		{
			name: "remove with a non numeric non collection value is rejected",
			opts: []query.PatchOption{
				query.Patch(fields.NameName, "suffix", query.PatchFieldRemove),
				query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, 1))),
			},
			want:    seedUsers(),
			wantErr: sqldb.ErrInvalidPatchValue,
		},
	}

	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			repo := setupRepo(t, b)

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					reseedUsers(t, repo)
					wantErr := caseErr(b, tc.wantErr, tc.wantErrOn)

					op := repo.ApplyPatch(t.Context(), &user{}, query.NewPatch(tc.opts...))
					require.ErrorIs(t, op.Error, wantErr)

					// A rejected patch must leave the table untouched.
					want := tc.want
					if wantErr != nil {
						want = seedUsers()
					}
					var got []user
					require.NoError(t, repo.DB.Order("id").Find(&got).Error)
					assert.Equal(t, want, got)
				})
			}
		})
	}
}
