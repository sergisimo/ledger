package usecase

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/query"
)

// --------------------------------------------------------------- Contract

type Patcher[R any] interface {
	Patch(context.Context, ...query.PatchOption) (R, error)
}
