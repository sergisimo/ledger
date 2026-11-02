// Package messagingtest helps write end-to-end tests for messaging code on a
// real in-memory broker: publish through the messaging contracts and assert
// what subscribers received.
package messagingtest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/inmem"
)

// Timeout bounds every wait performed by the package helpers.
const Timeout = time.Second

// NewBroker returns a started in-memory broker that is stopped at cleanup.
func NewBroker(t *testing.T) *inmem.Broker {
	t.Helper()

	broker := inmem.NewBroker(NewLogger(t))
	require.NoError(t, broker.Start())

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), Timeout)
		defer cancel()

		assert.NoError(t, broker.Stop(ctx))
	})

	return broker
}

// NewLogger returns a logger that discards all output.
func NewLogger(t *testing.T) logging.Logger {
	t.Helper()

	logger, err := logging.NewLogger(
		logging.WithServiceName("messagingtest"),
		logging.WithHandler(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)

	return logger
}

// NewPublisher returns a typed publisher for topic backed by broker.
func NewPublisher[T any](t *testing.T, broker *inmem.Broker, topic string) messaging.Publisher[T] {
	t.Helper()

	pub, err := messaging.NewPublisher[T](broker, topic)
	require.NoError(t, err)

	return pub
}

// Subscribe builds a subscriber for topic on broker and subscribes it with an
// OnErrorFunc that fails the test. It is unsubscribed at cleanup.
func Subscribe(t *testing.T, broker *inmem.Broker, topic string, h messaging.HandlerFunc, opts ...inmem.SubscriberOpt) *inmem.Subscriber {
	t.Helper()

	sub, err := inmem.NewSubscriber(broker, topic, h, opts...)
	require.NoError(t, err)
	require.NoError(t, sub.Subscribe(t.Context(), FailOnError(t)))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), Timeout)
		defer cancel()

		if err := sub.Unsubscribe(ctx); err != nil && !errors.Is(err, messaging.ErrNotSubscribed) {
			assert.NoError(t, err)
		}
	})

	return sub
}

// FailOnError returns an OnErrorFunc that fails the test.
func FailOnError(t *testing.T) messaging.OnErrorFunc {
	return func(_ context.Context, err error) {
		t.Errorf("subscription failed: %v", err)
	}
}
