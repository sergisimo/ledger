package persistence_test

import (
	"testing"

	"github.com/sergisimo/ledger/internal/platform/persistence"
	"github.com/stretchr/testify/assert"
)

func TestContextTxFunctions(t *testing.T) {
	tx := assert.AnError

	ctx := t.Context()
	ctxWithTx := persistence.InjectTx(ctx, tx)
	gotTx, found := persistence.TxFromCtx[error](ctxWithTx)
	assert.True(t, found)
	assert.Equal(t, tx, gotTx)

	ctxWithoutTx := persistence.WithoutTx(ctxWithTx)
	gotTx, found = persistence.TxFromCtx[error](ctxWithoutTx)
	assert.False(t, found)
	assert.Nil(t, gotTx)

	v, found := persistence.TxFromCtx[any](ctxWithoutTx)
	assert.Nil(t, v)
	assert.False(t, found)
	v, found = persistence.TxFromCtx[any](ctxWithTx)
	assert.Equal(t, tx, v)
	assert.True(t, found)
}
