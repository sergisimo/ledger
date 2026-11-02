package inmem_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/inmem"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
)

type event struct {
	Name string `json:"name"`
}

// errorRecorder records OnErrorFunc invocations.
type errorRecorder struct {
	mu   sync.Mutex
	errs []error
}

func (r *errorRecorder) onError(_ context.Context, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.errs = append(r.errs, err)
}

func (r *errorRecorder) errors() []error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]error(nil), r.errs...)
}

func TestBroker(t *testing.T) {
	t.Run("delivers typed events to every subscriber of the topic in order", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)

		first := messagingtest.Collect[event](t, broker, "ledger.accounts")
		second := messagingtest.Collect[event](t, broker, "ledger.accounts")
		other := messagingtest.Collect[event](t, broker, "ledger.entries")

		pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}, event{Name: "savings"}))

		want := []event{{Name: "checking"}, {Name: "savings"}}
		assert.Equal(t, want, first.Wait(t, 2))
		assert.Equal(t, want, second.Wait(t, 2))
		other.ExpectNone(t)

		msgs := first.Messages()
		assert.NotEqual(t, msgs[0].ID, msgs[1].ID)
		for _, msg := range msgs {
			assert.Equal(t, "ledger.accounts", msg.Topic)
			assert.Equal(t, messaging.ContentTypeJSON, msg.Headers[messaging.HeaderContentType])
		}
	})

	t.Run("unsubscribe stops the worker and subscribe restarts it", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)
		pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")

		received := make(chan event, 16)
		sub, err := inmem.NewSubscriber(broker, "ledger.accounts", messaging.Handle(
			func(_ context.Context, payload event, _ messaging.Message) error {
				received <- payload
				return nil
			},
		))
		require.NoError(t, err)

		require.NoError(t, sub.Subscribe(t.Context(), messagingtest.FailOnError(t)))
		require.NoError(t, pub.Publish(t.Context(), event{Name: "first"}))
		assert.Equal(t, event{Name: "first"}, <-received)

		require.NoError(t, sub.Unsubscribe(t.Context()))
		require.NoError(t, pub.Publish(t.Context(), event{Name: "missed"}))

		require.NoError(t, sub.Subscribe(t.Context(), messagingtest.FailOnError(t)))
		require.NoError(t, pub.Publish(t.Context(), event{Name: "second"}))
		assert.Equal(t, event{Name: "second"}, <-received, "the message published while unsubscribed should be gone")

		require.NoError(t, sub.Unsubscribe(t.Context()))
	})

	t.Run("handler errors are logged and the worker keeps going", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)
		pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")

		collector := messagingtest.Collect[event](t, broker, "ledger.accounts")
		messagingtest.Subscribe(t, broker, "ledger.accounts", func(context.Context, messaging.Message) error {
			return assert.AnError
		})

		require.NoError(t, pub.Publish(t.Context(), event{Name: "first"}, event{Name: "second"}))
		assert.Len(t, collector.Wait(t, 2), 2)
	})

	t.Run("handler panics are reported to onError and the worker survives", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)
		pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")

		var rec errorRecorder
		received := make(chan event, 16)
		sub, err := inmem.NewSubscriber(broker, "ledger.accounts", messaging.Handle(
			func(_ context.Context, payload event, _ messaging.Message) error {
				if payload.Name == "boom" {
					panic("kaboom")
				}
				received <- payload
				return nil
			},
		))
		require.NoError(t, err)
		require.NoError(t, sub.Subscribe(t.Context(), rec.onError))
		defer func() { assert.NoError(t, sub.Unsubscribe(context.Background())) }()

		require.NoError(t, pub.Publish(t.Context(), event{Name: "boom"}, event{Name: "after"}))

		assert.Equal(t, event{Name: "after"}, <-received, "the worker should survive the panic")
		errs := rec.errors()
		require.Len(t, errs, 1)
		assert.ErrorContains(t, errs[0], "panicked")
	})

	t.Run("a full queue blocks the publisher until its context expires", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)
		pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")

		started := make(chan struct{}, 8)
		release := make(chan struct{})
		defer close(release)
		messagingtest.Subscribe(t, broker, "ledger.accounts", func(context.Context, messaging.Message) error {
			started <- struct{}{}
			<-release
			return nil
		}, inmem.SubscriberWithBuffer(1))

		// First message is picked up by the worker, second fills the queue.
		require.NoError(t, pub.Publish(t.Context(), event{Name: "in-flight"}))
		<-started
		require.NoError(t, pub.Publish(t.Context(), event{Name: "queued"}))

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, pub.Publish(ctx, event{Name: "blocked"}), context.DeadlineExceeded)
	})

	t.Run("a full queue drops and reports with the drop policy", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)
		pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")

		var rec errorRecorder
		started := make(chan struct{}, 8)
		release := make(chan struct{})
		processed := make(chan event, 16)
		sub, err := inmem.NewSubscriber(broker, "ledger.accounts",
			messaging.Handle(func(_ context.Context, payload event, _ messaging.Message) error {
				started <- struct{}{}
				<-release
				processed <- payload
				return nil
			}),
			inmem.SubscriberWithBuffer(1), inmem.SubscriberWithOverflowDrop(),
		)
		require.NoError(t, err)
		require.NoError(t, sub.Subscribe(t.Context(), rec.onError))
		defer func() { assert.NoError(t, sub.Unsubscribe(context.Background())) }()

		// First message is picked up by the worker, second fills the queue,
		// third is dropped.
		require.NoError(t, pub.Publish(t.Context(), event{Name: "in-flight"}))
		<-started
		require.NoError(t, pub.Publish(t.Context(), event{Name: "queued"}))
		require.NoError(t, pub.Publish(t.Context(), event{Name: "dropped"}))

		errs := rec.errors()
		require.Len(t, errs, 1)
		assert.ErrorIs(t, errs[0], inmem.ErrMessageDropped)

		close(release)
		assert.Equal(t, event{Name: "in-flight"}, <-processed)
		assert.Equal(t, event{Name: "queued"}, <-processed)
	})

	t.Run("supports concurrent publishers", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)
		pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")

		var count atomic.Int64
		messagingtest.Subscribe(t, broker, "ledger.accounts", func(context.Context, messaging.Message) error {
			count.Add(1)
			return nil
		})

		const publishers, perPublisher = 8, 50
		var wg sync.WaitGroup
		for range publishers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range perPublisher {
					assert.NoError(t, pub.Publish(t.Context(), event{Name: "n"}))
				}
			}()
		}
		wg.Wait()

		require.Eventually(t, func() bool {
			return count.Load() == int64(publishers*perPublisher)
		}, messagingtest.Timeout, time.Millisecond)
	})
}

func TestBrokerLifecycle(t *testing.T) {
	handler := func(context.Context, messaging.Message) error { return nil }

	t.Run("publishing and subscribing require a running broker", func(t *testing.T) {
		broker := inmem.NewBroker(messagingtest.NewLogger(t))

		err := broker.Publish(t.Context(), messaging.NewMessage("ledger.accounts", nil))
		require.ErrorIs(t, err, inmem.ErrBrokerNotRunning)

		sub, err := inmem.NewSubscriber(broker, "ledger.accounts", handler)
		require.NoError(t, err)
		require.ErrorIs(t, sub.Subscribe(t.Context(), nil), inmem.ErrBrokerNotRunning)
	})

	t.Run("start is idempotent and stop closes for good", func(t *testing.T) {
		broker := inmem.NewBroker(messagingtest.NewLogger(t))
		require.NoError(t, broker.Start())
		require.NoError(t, broker.Start())

		require.NoError(t, broker.Stop(t.Context()))
		require.NoError(t, broker.Stop(t.Context()), "stop should be idempotent")
		require.ErrorIs(t, broker.Start(), inmem.ErrBrokerClosed)

		err := broker.Publish(t.Context(), messaging.NewMessage("ledger.accounts", nil))
		require.ErrorIs(t, err, inmem.ErrBrokerNotRunning)
	})

	t.Run("stop stops active subscriptions", func(t *testing.T) {
		broker := inmem.NewBroker(messagingtest.NewLogger(t))
		require.NoError(t, broker.Start())

		sub, err := inmem.NewSubscriber(broker, "ledger.accounts", handler)
		require.NoError(t, err)
		require.NoError(t, sub.Subscribe(t.Context(), nil))

		require.NoError(t, broker.Stop(t.Context()))
		require.ErrorIs(t, sub.Unsubscribe(t.Context()), messaging.ErrNotSubscribed)
		require.ErrorIs(t, sub.Subscribe(t.Context(), nil), inmem.ErrBrokerNotRunning)
	})

	t.Run("subscribe twice and unsubscribe twice are errors", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)

		sub, err := inmem.NewSubscriber(broker, "ledger.accounts", handler)
		require.NoError(t, err)

		require.NoError(t, sub.Subscribe(t.Context(), nil))
		require.ErrorIs(t, sub.Subscribe(t.Context(), nil), messaging.ErrAlreadySubscribed)

		require.NoError(t, sub.Unsubscribe(t.Context()))
		require.ErrorIs(t, sub.Unsubscribe(t.Context()), messaging.ErrNotSubscribed)
	})

	t.Run("unsubscribe honours its context while a handler is in flight", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)
		pub := messagingtest.NewPublisher[event](t, broker, "ledger.accounts")

		started := make(chan struct{}, 1)
		release := make(chan struct{})
		defer close(release)
		sub := messagingtest.Subscribe(t, broker, "ledger.accounts", func(context.Context, messaging.Message) error {
			started <- struct{}{}
			<-release
			return nil
		})

		require.NoError(t, pub.Publish(t.Context(), event{Name: "slow"}))
		<-started

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, sub.Unsubscribe(ctx), context.DeadlineExceeded)
	})

	t.Run("rejects an empty topic and a nil handler at construction", func(t *testing.T) {
		broker := inmem.NewBroker(messagingtest.NewLogger(t))

		_, err := inmem.NewSubscriber(broker, "", handler)
		require.ErrorIs(t, err, fields.NewErrInvalidEmptyString(messaging.FieldNameTopic))

		_, err = inmem.NewSubscriber(broker, "ledger.accounts", nil)
		require.ErrorIs(t, err, fields.NewErrNil(messaging.FieldNameHandler))
	})

	t.Run("rejects publishing a message with an empty topic", func(t *testing.T) {
		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "ledger.accounts")

		err := broker.Publish(t.Context(),
			messaging.NewMessage("ledger.accounts", nil),
			messaging.NewMessage("", nil),
		)
		require.ErrorIs(t, err, fields.NewErrInvalidEmptyString(messaging.FieldNameTopic))
		collector.ExpectNone(t)
	})
}
