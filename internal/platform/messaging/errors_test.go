package messaging_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

func TestPermanent(t *testing.T) {
	t.Run("marks errors and IsPermanent detects them", func(t *testing.T) {
		err := messaging.Permanent(assert.AnError)

		assert.True(t, messaging.IsPermanent(err))
		assert.ErrorIs(t, err, assert.AnError, "the original error should stay reachable")
	})

	t.Run("nil stays nil", func(t *testing.T) {
		require.NoError(t, messaging.Permanent(nil))
		assert.False(t, messaging.IsPermanent(nil))
	})

	t.Run("survives further wrapping", func(t *testing.T) {
		err := fmt.Errorf("handling failed: %w", messaging.Permanent(assert.AnError))
		assert.True(t, messaging.IsPermanent(err))
	})

	t.Run("a plain error is not permanent", func(t *testing.T) {
		assert.False(t, messaging.IsPermanent(assert.AnError))
	})
}

func TestBatchError(t *testing.T) {
	t.Run("reports the failure count and exposes the per-message errors", func(t *testing.T) {
		wrapped := errors.New("broker refused")
		err := error(&messaging.BatchError{
			Failed: map[int]error{
				1: assert.AnError,
				3: fmt.Errorf("delivery: %w", wrapped),
			},
		})

		assert.ErrorContains(t, err, "2 messages")
		assert.ErrorIs(t, err, assert.AnError)
		assert.ErrorIs(t, err, wrapped)

		var batchErr *messaging.BatchError
		require.ErrorAs(t, fmt.Errorf("publish: %w", err), &batchErr)
		assert.Len(t, batchErr.Unwrap(), 2)
	})
}
