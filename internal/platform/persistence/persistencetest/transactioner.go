// Package persistencetest provides persistence test doubles.
package persistencetest

import (
	"context"

	"github.com/sergisimo/ledger/internal/platform/persistence"
)

// Transactioner is a persistence.Transactioner without a database: Exec runs
// the function directly, with no transaction semantics.
type Transactioner struct{}

// NewTransactioner returns a pass-through Transactioner.
func NewTransactioner() *Transactioner {
	return &Transactioner{}
}

// Exec calls f with ctx unchanged.
func (t *Transactioner) Exec(ctx context.Context, f persistence.TxFunc) error {
	return f(ctx)
}
