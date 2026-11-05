package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox/outboxtest"
)

func TestPublisher(t *testing.T) {
	store := outboxtest.NewStore(t)

	t.Run("saves messages as pending", func(t *testing.T) {
		store.Reset(t)

		first := messaging.NewMessage("acme.accounts", []byte(`{}`))
		second := messaging.NewMessage("acme.entries", nil)
		require.NoError(t, outbox.NewPublisher(store).Publish(t.Context(), first, second))

		rows := store.Rows(t)
		require.Len(t, rows, 2)
		assert.Equal(t, first.ID, rows[0].ID)
		assert.Equal(t, second.ID, rows[1].ID)
		assert.Zero(t, store.DispatchedCount(t))
	})

	t.Run("is a no-op without messages", func(t *testing.T) {
		store.Reset(t)

		require.NoError(t, outbox.NewPublisher(store).Publish(t.Context()))
		assert.Zero(t, store.Count(t))
	})

	t.Run("rejects an empty topic before saving anything", func(t *testing.T) {
		store.Reset(t)

		err := outbox.NewPublisher(store).Publish(t.Context(),
			messaging.NewMessage("acme.accounts", nil),
			messaging.NewMessage("", nil),
		)
		require.ErrorIs(t, err, fields.NewErrInvalidEmptyString(messaging.FieldNameTopic))
		assert.Zero(t, store.Count(t))
	})

	t.Run("propagates store errors", func(t *testing.T) {
		store.Reset(t)

		failing := &failingStore{Store: store}
		failing.failSaveWith(assert.AnError)

		err := outbox.NewPublisher(failing).Publish(t.Context(), messaging.NewMessage("acme.accounts", nil))
		require.ErrorIs(t, err, assert.AnError)
		assert.Zero(t, store.Count(t))
	})

	t.Run("saved messages round-trip through the typed publisher", func(t *testing.T) {
		store.Reset(t)

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))

		require.NoError(t, store.Transactioner.Exec(t.Context(), func(txCtx context.Context) error {
			pending, err := store.FetchPending(txCtx, time.Now(), 10, 0)
			require.NoError(t, err)
			require.Len(t, pending, 1)

			var got event
			require.NoError(t, pending[0].Bind(&got))
			assert.Equal(t, event{Name: "checking"}, got)
			assert.Equal(t, messaging.ContentTypeJSON, pending[0].Headers[messaging.HeaderContentType])

			return nil
		}))
	})
}
