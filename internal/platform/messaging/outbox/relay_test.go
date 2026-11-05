package outbox_test

import (
	"context"
	"expvar"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox/outboxtest"
	"github.com/sergisimo/ledger/internal/platform/tracing"
)

func TestRelay(t *testing.T) {
	store := outboxtest.NewStore(t)

	t.Run("relays published events to the broker end to end", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "acme.accounts")

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}, event{Name: "savings"}))

		startRelay(t, outbox.NewRelay(store, broker, store.Transactioner, messagingtest.NewLogger(t), fastCfg()))

		assert.Equal(t, []event{{Name: "checking"}, {Name: "savings"}}, collector.Wait(t, 2))
		require.Eventually(t, func() bool {
			return store.DispatchedCount(t) == 2
		}, messagingtest.Timeout, time.Millisecond)
		assert.Equal(t, 2, store.Count(t), "dispatched rows should be retained")
	})

	t.Run("drains a backlog bigger than the batch without extra polls", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "acme.accounts")

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(),
			event{Name: "1"}, event{Name: "2"}, event{Name: "3"}, event{Name: "4"}, event{Name: "5"},
		))

		cfg := outbox.RelayConfig{PollInterval: time.Hour, BatchSize: 2, Retention: time.Hour}
		startRelay(t, outbox.NewRelay(store, broker, store.Transactioner, messagingtest.NewLogger(t), cfg))

		assert.Len(t, collector.Wait(t, 5), 5)
	})

	t.Run("retries with backoff until the broker accepts", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "acme.accounts")

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))
		msgID := firstRowID(t, store)

		now := time.Now().UTC()
		clock := outboxtest.NewClock(now)
		delegate := &flakyPublisher{inner: broker, failures: 1}

		cfg := fastCfg()
		cfg.InitialBackoff = time.Minute
		cfg.MaxBackoff = 5 * time.Minute
		startRelay(t, outbox.NewRelay(
			store, delegate, store.Transactioner, messagingtest.NewLogger(t), cfg,
			outbox.RelayWithClock(clock.Now),
		))

		require.Eventually(t, func() bool {
			row, ok := store.Row(t, msgID)
			return ok && row.Attempts == 1
		}, messagingtest.Timeout, time.Millisecond)

		row, ok := store.Row(t, msgID)
		require.True(t, ok)
		assert.WithinDuration(t, now.Add(time.Minute), row.NextAttemptAt, time.Second,
			"the retry should be scheduled one InitialBackoff away")
		assert.NotEmpty(t, row.LastError)
		collector.ExpectNone(t)

		clock.Advance(time.Minute + time.Second)
		assert.Equal(t, []event{{Name: "checking"}}, collector.Wait(t, 1))
	})

	t.Run("a partial batch failure retries only the failed messages", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "acme.accounts")

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "fine"}, event{Name: "flaky"}))

		rows := store.Rows(t)
		require.Len(t, rows, 2)
		flakyID := rows[1].ID

		delegate := &partialPublisher{inner: broker, remaining: 1, match: "flaky"}
		cfg := fastCfg()
		cfg.InitialBackoff = 20 * time.Millisecond
		cfg.MaxBackoff = 20 * time.Millisecond
		startRelay(t, outbox.NewRelay(store, delegate, store.Transactioner, messagingtest.NewLogger(t), cfg))

		assert.Equal(t, []event{{Name: "fine"}}, collector.Wait(t, 1), "the delivered part of the batch should be dispatched immediately")

		require.Eventually(
			t,
			func() bool {
				row, ok := store.Row(t, flakyID)
				return ok && row.Attempts == 1
			},
			messagingtest.Timeout,
			time.Millisecond,
			"only the failed message should be rescheduled",
		)

		assert.Equal(t, []event{{Name: "fine"}, {Name: "flaky"}}, collector.Wait(t, 2), "the failed message should be retried and delivered")
		require.Eventually(
			t,
			func() bool {
				return store.DispatchedCount(t) == 2
			},
			messagingtest.Timeout,
			time.Millisecond,
		)
	})

	t.Run("caps the backoff at MaxBackoff", func(t *testing.T) {
		store.Reset(t)

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))
		msgID := firstRowID(t, store)

		now := time.Now().UTC()
		clock := outboxtest.NewClock(now)

		cfg := fastCfg()
		cfg.InitialBackoff = time.Minute
		cfg.MaxBackoff = 3 * time.Minute
		startRelay(t, outbox.NewRelay(
			store, &flakyPublisher{failures: -1}, store.Transactioner, messagingtest.NewLogger(t), cfg,
			outbox.RelayWithClock(clock.Now),
		))

		waitAttempts := func(n int) outboxtest.Row {
			require.Eventually(t, func() bool {
				row, ok := store.Row(t, msgID)
				return ok && row.Attempts == n
			}, messagingtest.Timeout, time.Millisecond)
			row, _ := store.Row(t, msgID)

			return row
		}

		row := waitAttempts(1)
		assert.WithinDuration(t, now.Add(time.Minute), row.NextAttemptAt, time.Second,
			"attempt 1 should back off InitialBackoff")

		clock.Advance(time.Minute)
		row = waitAttempts(2)
		assert.WithinDuration(t, now.Add(3*time.Minute), row.NextAttemptAt, time.Second,
			"attempt 2 should back off 2×InitialBackoff")

		clock.Advance(2 * time.Minute)
		row = waitAttempts(3)
		assert.WithinDuration(t, now.Add(6*time.Minute), row.NextAttemptAt, time.Second,
			"attempt 3 should be capped at MaxBackoff")
	})

	t.Run("parks messages after MaxAttempts", func(t *testing.T) {
		store.Reset(t)

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))
		msgID := firstRowID(t, store)

		delegate := &flakyPublisher{failures: -1}

		cfg := fastCfg()
		cfg.InitialBackoff = time.Millisecond
		cfg.MaxBackoff = time.Millisecond
		cfg.MaxAttempts = 2
		startRelay(t, outbox.NewRelay(store, delegate, store.Transactioner, messagingtest.NewLogger(t), cfg))

		require.Eventually(t, func() bool {
			row, ok := store.Row(t, msgID)
			return ok && row.Attempts == 2
		}, messagingtest.Timeout, time.Millisecond)

		time.Sleep(50 * time.Millisecond)

		assert.Equal(t, 2, delegate.callCount(), "a parked message should not be retried")
		row, ok := store.Row(t, msgID)
		require.True(t, ok)
		assert.Nil(t, row.DispatchedAt)
		assert.NotEmpty(t, row.LastError)
	})

	t.Run("deletes dispatched rows when retention is zero", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "acme.accounts")

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}, event{Name: "savings"}))

		cfg := fastCfg()
		cfg.Retention = 0
		startRelay(t, outbox.NewRelay(store, broker, store.Transactioner, messagingtest.NewLogger(t), cfg))

		collector.Wait(t, 2)
		require.Eventually(t, func() bool {
			return store.Count(t) == 0
		}, messagingtest.Timeout, time.Millisecond)
	})

	t.Run("janitor prunes dispatched rows after the retention window", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "acme.accounts")

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))

		cfg := fastCfg()
		cfg.Retention = 20 * time.Millisecond
		cfg.CleanupInterval = 5 * time.Millisecond
		startRelay(t, outbox.NewRelay(store, broker, store.Transactioner, messagingtest.NewLogger(t), cfg))

		collector.Wait(t, 1)
		require.Eventually(t, func() bool {
			return store.Count(t) == 0
		}, messagingtest.Timeout, time.Millisecond)
	})

	t.Run("a fetch failure does not stop the relay", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "acme.accounts")

		failing := &failingStore{Store: store}
		failing.failFetchWith(assert.AnError)

		pub := newTypedPublisher(t, store, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))

		startRelay(t, outbox.NewRelay(failing, broker, store.Transactioner, messagingtest.NewLogger(t), fastCfg()))

		collector.ExpectNone(t)
		failing.failFetchWith(nil)
		assert.Len(t, collector.Wait(t, 1), 1)
	})

	t.Run("messages from rolled back transactions are never relayed", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		collector := messagingtest.Collect[event](t, broker, "acme.accounts")
		pub := newTypedPublisher(t, store, "acme.accounts")

		dispatched, ok := expvar.Get("messaging.outbox.dispatched").(*expvar.Int)
		require.True(t, ok)
		before := dispatched.Value()

		startRelay(t, outbox.NewRelay(store, broker, store.Transactioner, messagingtest.NewLogger(t), fastCfg()))

		err := store.Transactioner.Exec(t.Context(), func(txCtx context.Context) error {
			require.NoError(t, pub.Publish(txCtx, event{Name: "ghost"}))
			return assert.AnError
		})
		require.ErrorIs(t, err, assert.AnError)
		collector.ExpectNone(t)
		assert.Zero(t, store.Count(t), "the rollback should discard the outbox row")

		require.NoError(t, store.Transactioner.Exec(t.Context(), func(txCtx context.Context) error {
			return pub.Publish(txCtx, event{Name: "real"})
		}))
		assert.Equal(t, []event{{Name: "real"}}, collector.Wait(t, 1))

		require.Eventually(t, func() bool {
			return dispatched.Value() == before+1
		}, messagingtest.Timeout, time.Millisecond, "the dispatched metric should count only the committed message")
	})

	t.Run("the trace context survives the outbox", func(t *testing.T) {
		store.Reset(t)

		broker := messagingtest.NewBroker(t)
		pub := newTypedPublisher(t, store, "acme.accounts")

		traceIDs := make(chan string, 1)
		messagingtest.Subscribe(t, broker, "acme.accounts", func(ctx context.Context, _ messaging.Message) error {
			traceIDs <- tracing.GetTraceID(ctx)
			return nil
		})

		startRelay(t, outbox.NewRelay(store, broker, store.Transactioner, messagingtest.NewLogger(t), fastCfg()))

		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID{0xab},
			SpanID:     trace.SpanID{0xcd},
			TraceFlags: trace.FlagsSampled,
		})
		require.NoError(t, pub.Publish(trace.ContextWithSpanContext(t.Context(), sc), event{Name: "checking"}))

		select {
		case got := <-traceIDs:
			assert.Equal(t, sc.TraceID().String(), got, "the handler should run inside the publishing trace")
		case <-time.After(messagingtest.Timeout):
			t.Fatal("the handler never ran")
		}
	})

	t.Run("start is idempotent and stop closes for good", func(t *testing.T) {
		store.Reset(t)

		relay := outbox.NewRelay(
			store, &flakyPublisher{}, store.Transactioner, messagingtest.NewLogger(t), fastCfg(),
		)

		require.NoError(t, relay.Start())
		require.NoError(t, relay.Start())

		require.NoError(t, relay.Stop(t.Context()))
		require.NoError(t, relay.Stop(t.Context()), "stop should be idempotent")
		require.ErrorIs(t, relay.Start(), outbox.ErrRelayClosed)
	})
}
