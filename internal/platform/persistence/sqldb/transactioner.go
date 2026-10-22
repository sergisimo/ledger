package sqldb

import (
	"context"

	"gorm.io/gorm"

	"github.com/sergisimo/ledger/internal/platform/persistence"
)

type Transactioner struct {
	DB *gorm.DB
}

func NewTransactioner(db *gorm.DB) *Transactioner {
	return &Transactioner{DB: db}
}

func (t *Transactioner) Exec(ctx context.Context, f persistence.TxFunc) error {
	if _, ok := persistence.TxFromCtx[*gorm.DB](ctx); ok {
		return f(ctx)
	}

	return t.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return f(persistence.InjectTx(ctx, tx))
	})
}
