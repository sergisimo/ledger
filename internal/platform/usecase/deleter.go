package usecase

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/query"
)

// --------------------------------------------------------------- Contract

type Deleter interface {
	Delete(context.Context, query.DeleteType, ...query.SrchOption) error
}
