package usecase

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/resource"
)

// --------------------------------------------------------------- Contract

type Getter[R resource.Resource] interface {
	Get(context.Context, ...query.SrchOption) (R, error)
}
