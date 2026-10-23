package rpc

import (
	"google.golang.org/protobuf/proto"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc/rpcpb"
	"github.com/sergisimo/ledger/internal/platform/query"
)

// --------------------------------------------------------------- Contract

type (
	Identifiable interface {
		proto.Message
		GetId() string
	}
)

// --------------------------------------------------------------- Decoders

func idSrchOpts(req Identifiable) ([]query.SrchOption, error) {
	id := req.GetId()
	if id == "" {
		return nil, fields.NewErrInvalidEmptyString(fields.NameID)
	}
	return []query.SrchOption{query.Filter(query.Where(fields.NameID, filter.OpEq, id))}, nil
}

func PaginationFromProto(page *rpcpb.Page) query.SrchOption {
	if page == nil {
		return query.Pagination(0, 0)
	}
	return query.Pagination(page.GetLimit(), page.GetOffset())
}

func SortingFromProto(sorts []*rpcpb.Sort) ([]query.SrchOption, error) {
	var opts []query.SrchOption
	for _, sort := range sorts {
		if sort.GetField() == "" {
			return nil, fields.NewErrInvalidEmptyString(query.FieldNameSorting)
		}
		sortType := query.SortAsc
		if sort.GetDesc() {
			sortType = query.SortDesc
		}
		opts = append(opts, query.SortBy(fields.Name(sort.GetField()), sortType))
	}
	return opts, nil
}
