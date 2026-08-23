package query_test

import (
	"testing"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPatchQuery(t *testing.T) {
	const (
		field1 fields.Name = "field1"
		field2 fields.Name = "field2"
		field3 fields.Name = "field3"
	)

	pq := query.NewPatch(
		query.Patch(field1, "value1"),
		query.Patch(field2, 100, query.PatchFieldAdd),
		query.Patch(field3, true, query.PatchFieldRemove),
		query.PatchSearchOpts(
			query.FilterBy(field1, filter.OpEq, "search-value"),
		),
	)

	require.NotNil(t, pq)

	searchOpts := pq.SearchOpts()
	assert.Len(t, searchOpts, 1)

	pqFields := pq.Fields()
	assert.Len(t, pqFields, 3)
	assert.True(t, pqFields.Exists(field1))
	assert.True(t, pqFields.Exists(field2))
	assert.True(t, pqFields.Exists(field3))

	f1 := pqFields[field1]
	require.NotNil(t, f1)
	assert.Equal(t, "value1", f1.Value())
	assert.Equal(t, query.PatchFieldOperationSet, f1.Operation())
	assert.Equal(t, "SET", f1.Operation().String())

	f2 := pqFields[field2]
	require.NotNil(t, f2)
	assert.Equal(t, 100, f2.Value())
	assert.Equal(t, query.PatchFieldOperationAdd, f2.Operation())
	assert.Equal(t, "ADD", f2.Operation().String())

	f3 := pqFields[field3]
	require.NotNil(t, f3)
	assert.Equal(t, true, f3.Value())
	assert.Equal(t, query.PatchFieldOperationRemove, f3.Operation())
	assert.Equal(t, "REMOVE", f3.Operation().String())

	val, ok := pqFields.Value(field1)
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	val = pqFields.ValueOrNil(field2)
	assert.Equal(t, 100, val)

	val = pqFields.ValueOrNil("non-existent")
	assert.Nil(t, val)

	pqFields.Set(field1, "new-value")
	assert.Equal(t, "new-value", pqFields.ValueOrNil(field1))
	assert.Equal(t, query.PatchFieldOperationSet, pqFields[field1].Operation())

	pqFields.Set(field1, "another-value", query.PatchFieldAdd)
	assert.Equal(t, "another-value", pqFields.ValueOrNil(field1))
	assert.Equal(t, query.PatchFieldOperationAdd, pqFields[field1].Operation())

	cloned := pqFields.Clone()
	assert.Equal(t, pqFields, cloned)
	cloned.Set(field1, "cloned-value")
	assert.NotEqual(t, pqFields.ValueOrNil(field1), cloned.ValueOrNil(field1))

	pqFields.Reduce(field1, field2)
	assert.Len(t, pqFields, 2)
	assert.True(t, pqFields.Exists(field1))
	assert.True(t, pqFields.Exists(field2))
	assert.False(t, pqFields.Exists(field3))
}

func TestPatchOptions(t *testing.T) {
	const field1 fields.Name = "field1"

	pq1 := query.NewPatch(query.Patch(field1, "val1"))

	pq2 := query.NewPatch(query.WithPatchQuery(pq1))
	assert.Equal(t, pq1.Fields(), pq2.Fields())

	pq4 := query.NewPatch(query.WithPatchFields(query.PatchFields{field1: nil}))
	assert.True(t, pq4.Fields().Exists(field1))
}

func TestPatchFieldOperationString(t *testing.T) {
	tests := []struct {
		op   query.PatchFieldOperation
		want string
	}{
		{
			op:   query.PatchFieldOperationSet,
			want: "SET",
		},
		{
			op:   query.PatchFieldOperationAdd,
			want: "ADD",
		},
		{
			op:   query.PatchFieldOperationRemove,
			want: "REMOVE",
		},
		{
			op:   query.PatchFieldOperation(99),
			want: "UNKNOWN",
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.op.String())
		})
	}
}
