package query_test

import (
	"testing"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSearch(t *testing.T) {
	const (
		field1 fields.Name = "field1"
		field2 fields.Name = "field2"
		field3 fields.Name = "field3"
	)

	s1 := query.NewSearch(
		query.FilterBy(field1, filter.OpEq, "value1"),
		query.FilterBy(field2, filter.OpGT, 100),
		query.SortBy(field1, query.SortAsc),
		query.SortBy(field3, query.SortDesc),
		query.Pagination(10, 20),
		query.LoadRelated(field1, field2),
	)
	require.NotNil(t, s1)

	filters := s1.Filters()
	assert.Len(t, filters, 2)
	assert.True(t, filters.Exists(field1))
	assert.True(t, filters.Exists(field2))
	assert.Equal(t, filter.OpEq, filters.Get(field1).Operator())
	assert.Equal(t, "value1", filters.Get(field1).Value())
	assert.Equal(t, filter.OpGT, filters.Get(field2).Operator())
	assert.Equal(t, 100, filters.Get(field2).Value())
	assert.Equal(t, "field1 eq value1, field2 gt 100", filters.String())
	filters.Delete(field2)
	assert.Len(t, filters, 1)
	assert.False(t, filters.Exists(field2))
	filters.Rename(field1, field3)
	assert.Len(t, filters, 1)
	assert.False(t, filters.Exists(field1))
	assert.True(t, filters.Exists(field3))

	sorting := s1.Sorting()
	require.NotNil(t, sorting)
	assert.Equal(t, query.SortAsc, sorting.Get(field1))
	assert.Equal(t, query.SortDesc, sorting.Get(field3))
	assert.Equal(t, query.SortDirUndefined, sorting.Get(field2))
	assert.Equal(t, []fields.Name{field1, field3}, sorting.Keys())
	assert.Equal(t, "field1=ASC, field3=DESC", sorting.String())

	pagination := s1.Pagination()
	require.NotNil(t, pagination)
	assert.Equal(t, query.PaginationKindLimit, pagination.Kind())
	limitPag, ok := pagination.(query.LimitPagination)
	require.True(t, ok)
	assert.Equal(t, uint(10), limitPag.Limit())
	assert.Equal(t, uint(20), limitPag.Offset())
	assert.Equal(t, "limit=10, offset=20", limitPag.String())

	assert.Equal(t, []fields.Name{field1, field2}, s1.Load())
}
