package messaging_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// The happy paths of the messaging contracts are covered end to end by the
// inmem broker tests; this file only covers the pure error paths that cannot
// be reached through a broker.

type event struct {
	Name string `json:"name"`
}

type pubFunc func(ctx context.Context, msgs ...messaging.Message) error

func (f pubFunc) Publish(ctx context.Context, msgs ...messaging.Message) error {
	return f(ctx, msgs...)
}

func TestMessage(t *testing.T) {
	t.Run("gets a unique ID", func(t *testing.T) {
		msg := messaging.NewMessage("acme.accounts", []byte(`{}`))
		other := messaging.NewMessage("acme.accounts", nil)

		assert.NotEmpty(t, msg.ID)
		assert.NotEqual(t, msg.ID, other.ID)
	})

	t.Run("returns JSON encoding errors", func(t *testing.T) {
		_, err := messaging.NewJSONMessage("acme.accounts", func() {})
		require.Error(t, err)
	})

	t.Run("binding an empty payload is a no-op", func(t *testing.T) {
		got := event{Name: "unchanged"}
		require.NoError(t, messaging.NewMessage("acme.accounts", nil).Bind(&got))
		assert.Equal(t, event{Name: "unchanged"}, got)
	})

	t.Run("returns JSON decoding errors", func(t *testing.T) {
		var got event
		require.Error(t, messaging.NewMessage("acme.accounts", []byte("not-json")).Bind(&got))
	})

	t.Run("validates the topic", func(t *testing.T) {
		require.NoError(t, messaging.NewMessage("acme.accounts", nil).Validate())

		err := messaging.NewMessage("", nil).Validate()
		require.ErrorIs(t, err, fields.NewErrInvalidEmptyString(messaging.FieldNameTopic))
	})
}

func TestNewPublisher(t *testing.T) {
	failIfCalled := pubFunc(func(context.Context, ...messaging.Message) error {
		t.Error("the message publisher should not have been called")
		return nil
	})

	t.Run("rejects an empty topic", func(t *testing.T) {
		_, err := messaging.NewPublisher[event](failIfCalled, "", "test")
		require.ErrorIs(t, err, fields.NewErrInvalidEmptyString(messaging.FieldNameTopic))
	})

	t.Run("is a no-op without payloads", func(t *testing.T) {
		pub, err := messaging.NewPublisher[event](failIfCalled, "acme.accounts", "test")
		require.NoError(t, err)
		require.NoError(t, pub.Publish(t.Context()))
	})

	t.Run("returns encoding errors before publishing anything", func(t *testing.T) {
		pub, err := messaging.NewPublisher[func()](failIfCalled, "acme.accounts", "test")
		require.NoError(t, err)
		require.Error(t, pub.Publish(t.Context(), func() {}))
	})

	t.Run("propagates message publisher errors", func(t *testing.T) {
		failing := pubFunc(func(context.Context, ...messaging.Message) error {
			return assert.AnError
		})
		pub, err := messaging.NewPublisher[event](failing, "acme.accounts", "test")
		require.NoError(t, err)
		require.ErrorIs(t, pub.Publish(t.Context(), event{Name: "checking"}), assert.AnError)
	})
}

func TestHandle(t *testing.T) {
	t.Run("returns decoding errors without invoking the handler", func(t *testing.T) {
		invoked := false
		handler := messaging.Handle(func(context.Context, event, messaging.Message) error {
			invoked = true
			return nil
		})

		err := handler(t.Context(), messaging.NewMessage("acme.accounts", []byte("not-json")))
		require.Error(t, err)
		assert.True(t, messaging.IsPermanent(err), "an undecodable payload should never be redelivered")
		assert.False(t, invoked)
	})

	t.Run("propagates handler errors", func(t *testing.T) {
		handler := messaging.Handle(func(context.Context, event, messaging.Message) error {
			return assert.AnError
		})

		msg, err := messaging.NewJSONMessage("acme.accounts", event{Name: "checking"})
		require.NoError(t, err)
		require.ErrorIs(t, handler(t.Context(), msg), assert.AnError)
	})
}
