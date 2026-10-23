package rpc_test

import (
	"testing"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc/rpcpb"
	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/query/querytest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaginationFromProto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		page *rpcpb.Page
		want query.SrchOption
	}{
		{
			name: "nil page defaults to zero pagination",
			page: nil,
			want: query.Pagination(0, 0),
		},
		{
			name: "page values",
			page: &rpcpb.Page{Limit: 25, Offset: 50},
			want: query.Pagination(25, 50),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := rpc.PaginationFromProto(tt.page)
			assert.True(t, querytest.SrchOptMatcherFunc(tt.want)([]query.SrchOption{got}))
		})
	}
}

func TestSortingFromProto(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sorts   []*rpcpb.Sort
		want    []query.SrchOption
		wantErr error
	}{
		{
			name:  "no sorts",
			sorts: nil,
			want:  []query.SrchOption{},
		},
		{
			name: "ascending and descending",
			sorts: []*rpcpb.Sort{
				{Field: "name"},
				{Field: "created_at", Desc: true},
			},
			want: []query.SrchOption{
				query.SortBy(fields.Name("name"), query.SortAsc),
				query.SortBy(fields.Name("created_at"), query.SortDesc),
			},
		},
		{
			name:    "empty field",
			sorts:   []*rpcpb.Sort{{Field: ""}},
			wantErr: fields.NewErrInvalidEmptyString(query.FieldNameSorting),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := rpc.SortingFromProto(tt.sorts)
			require.ErrorIs(t, err, tt.wantErr)
			assert.True(t, querytest.SrchOptMatcherFunc(tt.want...)(got))
		})
	}
}
