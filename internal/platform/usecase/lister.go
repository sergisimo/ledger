package usecase

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/resource"
)

// --------------------------------------------------------------- Contract

type Lister[R resource.Resource] interface {
	List(context.Context, ...query.SrchOption) (resource.List[R], error)
}
