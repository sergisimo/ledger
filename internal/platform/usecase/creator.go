package usecase

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/resource"
)

// --------------------------------------------------------------- Contract

type Creator[R resource.Resource] interface {
	Create(context.Context, R) (R, error)
}
