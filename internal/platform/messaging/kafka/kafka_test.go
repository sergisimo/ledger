package kafka_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/kafka"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
)

// waitTimeout bounds every wait: consumer-group joins take a few coordination
// rounds even on the in-process fake cluster.
const waitTimeout = 10 * time.Second

type event struct {
	Name string `json:"name"`
}

func newClient(t *testing.T, topics ...string) *kafka.Client {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, topics...))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	client, err := kafka.NewClient(
		kafka.Config{Brokers: cluster.ListenAddrs(), ClientID: "kafka-test"},
		messagingtest.NewLogger(t),
	)
	require.NoError(t, err)

	return client
}

func newPublisher(t *testing.T, client *kafka.Client) *kafka.Publisher {
	t.Helper()

	pub, err := kafka.NewPublisher(client)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		assert.NoError(t, pub.Close(ctx))
	})

	return pub
}

func newTypedPublisher(t *testing.T, client *kafka.Client, topic string) messaging.Publisher[event] {
	t.Helper()

	pub, err := messaging.NewPublisher[event](newPublisher(t, client), topic, kafka.SystemName)
	require.NoError(t, err)

	return pub
}

func subscribe(
	t *testing.T, client *kafka.Client, onError messaging.OnErrorFunc,
	topic, group string, h messaging.HandlerFunc, opts ...kafka.SubscriberOpt,
) *kafka.Subscriber {
	t.Helper()

	sub, err := kafka.NewSubscriber(client, topic, group, h, opts...)
	require.NoError(t, err)
	require.NoError(t, sub.Subscribe(t.Context(), onError))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		if err := sub.Unsubscribe(ctx); err != nil && !errors.Is(err, messaging.ErrNotSubscribed) {
			assert.NoError(t, err)
		}
	})

	return sub
}

// collector records the typed payloads a group subscription receives.
type collector struct {
	mu       sync.Mutex
	payloads []event
	msgs     []messaging.Message
}

func collect(t *testing.T, client *kafka.Client, topic, group string, opts ...kafka.SubscriberOpt) *collector {
	t.Helper()

	c := &collector{}
	subscribe(t, client, messagingtest.FailOnError(t), topic, group, c.handler(), opts...)

	return c
}

func (c *collector) handler() messaging.HandlerFunc {
	return messaging.Handle(func(_ context.Context, payload event, msg messaging.Message) error {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.payloads = append(c.payloads, payload)
		c.msgs = append(c.msgs, msg)

		return nil
	})
}

func (c *collector) events(t *testing.T) []event {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]event(nil), c.payloads...)
}

func (c *collector) messages(t *testing.T) []messaging.Message {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]messaging.Message(nil), c.msgs...)
}

func (c *collector) wait(t *testing.T, n int) []event {
	t.Helper()

	require.Eventuallyf(t, func() bool {
		return len(c.events(t)) >= n
	}, waitTimeout, 5*time.Millisecond, "expected at least %d events, got %d", n, len(c.events(t)))

	return c.events(t)
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

func TestPubSub(t *testing.T) {
	t.Run("delivers typed events to every group in order", func(t *testing.T) {
		client := newClient(t, "acme.accounts", "acme.entries")
		pub := newTypedPublisher(t, client, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}, event{Name: "savings"}))

		mailer := collect(t, client, "acme.accounts", "account-mailer")
		auditor := collect(t, client, "acme.accounts", "account-auditor")
		other := collect(t, client, "acme.entries", "entry-auditor")

		want := []event{{Name: "checking"}, {Name: "savings"}}
		assert.Equal(t, want, mailer.wait(t, 2))
		assert.Equal(t, want, auditor.wait(t, 2))

		msgs := mailer.messages(t)
		require.Len(t, msgs, 2)
		assert.NotEmpty(t, msgs[0].ID)
		assert.NotEqual(t, msgs[0].ID, msgs[1].ID)
		for _, msg := range msgs {
			assert.Equal(t, "acme.accounts", msg.Topic)
			assert.Equal(t, messaging.ContentTypeJSON, msg.Headers[messaging.HeaderContentType])
			assert.NotContains(t, msg.Headers, messaging.HeaderMessageID, "the ID header should be lifted into Message.ID")
		}

		time.Sleep(100 * time.Millisecond)
		assert.Empty(t, other.events(t), "other topics should not receive the events")
	})

	t.Run("instances sharing a group compete for the messages", func(t *testing.T) {
		client := newClient(t, "acme.accounts")

		first := collect(t, client, "acme.accounts", "account-mailer")
		second := collect(t, client, "acme.accounts", "account-mailer")

		pub := newTypedPublisher(t, client, "acme.accounts")
		const total = 6
		for i := range total {
			require.NoError(t, pub.Publish(t.Context(), event{Name: string(rune('a' + i))}))
		}

		distinct := func() map[string]int {
			seen := map[string]int{}
			for _, msg := range append(first.messages(t), second.messages(t)...) {
				seen[msg.ID]++
			}
			return seen
		}

		require.Eventuallyf(t, func() bool {
			return len(distinct()) == total
		}, waitTimeout, 5*time.Millisecond, "every message should be handled, got %d", len(distinct()))

		time.Sleep(200 * time.Millisecond)
		got := len(first.events(t)) + len(second.events(t))
		assert.Less(t, got, 2*total, "the group should compete for messages, not each receive every one")
	})
}

func TestSubscriberFailures(t *testing.T) {
	t.Run("retries transient errors with backoff until the handler succeeds", func(t *testing.T) {
		client := newClient(t, "acme.accounts")
		pub := newTypedPublisher(t, client, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))

		var mu sync.Mutex
		attempts := 0
		done := make(chan struct{})
		subscribe(t, client, messagingtest.FailOnError(t), "acme.accounts", "account-mailer",
			func(context.Context, messaging.Message) error {
				mu.Lock()
				defer mu.Unlock()
				attempts++
				if attempts < 3 {
					return assert.AnError
				}
				close(done)
				return nil
			},
			kafka.SubscriberWithBackoff(time.Millisecond, time.Millisecond),
		)

		select {
		case <-done:
		case <-time.After(waitTimeout):
			t.Fatal("the handler never succeeded")
		}
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 3, attempts)
	})

	t.Run("dead-letters permanent failures with the origin headers", func(t *testing.T) {
		client := newClient(t, "acme.accounts", "acme.accounts.dead")
		pub := newTypedPublisher(t, client, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "poison"}))

		var calls sync.Map
		subscribe(t, client, messagingtest.FailOnError(t), "acme.accounts", "account-mailer",
			func(_ context.Context, msg messaging.Message) error {
				count, _ := calls.LoadOrStore(msg.ID, new(int))
				*(count.(*int))++
				return messaging.Permanent(assert.AnError)
			},
			kafka.SubscriberWithDeadLetterTopic("acme.accounts.dead"),
		)

		dead := collect(t, client, "acme.accounts.dead", "dead-letter-auditor")
		assert.Equal(t, []event{{Name: "poison"}}, dead.wait(t, 1))

		msg := dead.messages(t)[0]
		assert.Equal(t, "acme.accounts", msg.Headers[messaging.HeaderDeadLetterOrigin])
		assert.Contains(t, msg.Headers[messaging.HeaderDeadLetterReason], assert.AnError.Error())
		assert.Equal(t, messaging.ContentTypeJSON, msg.Headers[messaging.HeaderContentType])

		calls.Range(func(_, count any) bool {
			assert.Equal(t, 1, *(count.(*int)), "a permanent failure should not be retried")
			return true
		})
	})

	t.Run("drops and reports exhausted messages without a dead-letter topic", func(t *testing.T) {
		client := newClient(t, "acme.accounts")
		pub := newTypedPublisher(t, client, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "cursed"}, event{Name: "fine"}))

		var rec errorRecorder
		var mu sync.Mutex
		attempts := 0
		processed := make(chan event, 1)
		subscribe(t, client, rec.onError, "acme.accounts", "account-mailer",
			messaging.Handle(func(_ context.Context, payload event, _ messaging.Message) error {
				if payload.Name == "cursed" {
					mu.Lock()
					defer mu.Unlock()
					attempts++
					return assert.AnError
				}
				processed <- payload
				return nil
			}),
			kafka.SubscriberWithMaxAttempts(2),
			kafka.SubscriberWithBackoff(time.Millisecond, time.Millisecond),
		)

		select {
		case got := <-processed:
			assert.Equal(t, event{Name: "fine"}, got, "the worker should survive the drop")
		case <-time.After(waitTimeout):
			t.Fatal("the message after the dropped one never arrived")
		}

		errs := rec.errors()
		require.Len(t, errs, 1)
		assert.ErrorIs(t, errs[0], assert.AnError)
		assert.ErrorContains(t, errs[0], "dropped")
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 2, attempts, "MaxAttempts should cap the retries")
	})

	t.Run("recovered panics are permanent and reported", func(t *testing.T) {
		client := newClient(t, "acme.accounts")
		pub := newTypedPublisher(t, client, "acme.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "boom"}, event{Name: "after"}))

		var rec errorRecorder
		received := make(chan event, 2)
		subscribe(t, client, rec.onError, "acme.accounts", "account-mailer",
			messaging.Handle(func(_ context.Context, payload event, _ messaging.Message) error {
				if payload.Name == "boom" {
					panic("kaboom")
				}
				received <- payload
				return nil
			}),
		)

		select {
		case got := <-received:
			assert.Equal(t, event{Name: "after"}, got, "the worker should survive the panic")
		case <-time.After(waitTimeout):
			t.Fatal("the message after the panic never arrived")
		}

		errs := rec.errors()
		require.Len(t, errs, 1)
		assert.ErrorIs(t, errs[0], messaging.ErrHandlerPanic)
	})
}

func TestSubscriberLifecycle(t *testing.T) {
	t.Run("commits survive resubscribing", func(t *testing.T) {
		client := newClient(t, "acme.accounts")
		pub := newTypedPublisher(t, client, "acme.accounts")

		received := make(chan event, 4)
		handler := messaging.Handle(func(_ context.Context, payload event, _ messaging.Message) error {
			received <- payload
			return nil
		})

		sub, err := kafka.NewSubscriber(client, "acme.accounts", "account-mailer", handler)
		require.NoError(t, err)

		require.NoError(t, sub.Subscribe(t.Context(), messagingtest.FailOnError(t)))
		require.NoError(t, pub.Publish(t.Context(), event{Name: "first"}))
		select {
		case got := <-received:
			assert.Equal(t, event{Name: "first"}, got)
		case <-time.After(waitTimeout):
			t.Fatal("the first message never arrived")
		}

		require.NoError(t, sub.Unsubscribe(t.Context()))
		require.NoError(t, pub.Publish(t.Context(), event{Name: "second"}))

		require.NoError(t, sub.Subscribe(t.Context(), messagingtest.FailOnError(t)))
		defer func() { assert.NoError(t, sub.Unsubscribe(context.Background())) }()

		select {
		case got := <-received:
			assert.Equal(t, event{Name: "second"}, got, "the committed message should not be redelivered")
		case <-time.After(waitTimeout):
			t.Fatal("the second message never arrived")
		}
	})

	t.Run("subscribe twice and unsubscribe twice are errors", func(t *testing.T) {
		client := newClient(t, "acme.accounts")
		handler := func(context.Context, messaging.Message) error { return nil }

		sub := subscribe(t, client, nil, "acme.accounts", "account-mailer", handler)
		require.ErrorIs(t, sub.Subscribe(t.Context(), nil), messaging.ErrAlreadySubscribed)

		require.NoError(t, sub.Unsubscribe(t.Context()))
		require.ErrorIs(t, sub.Unsubscribe(t.Context()), messaging.ErrNotSubscribed)
	})

	t.Run("subscribe fails fast on an unreachable cluster", func(t *testing.T) {
		client, err := kafka.NewClient(
			kafka.Config{Brokers: []string{"localhost:1"}, ClientID: "kafka-test"},
			messagingtest.NewLogger(t),
		)
		require.NoError(t, err)

		sub, err := kafka.NewSubscriber(client, "acme.accounts", "account-mailer",
			func(context.Context, messaging.Message) error { return nil })
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		require.Error(t, sub.Subscribe(ctx, nil))
		require.ErrorIs(t, sub.Unsubscribe(t.Context()), messaging.ErrNotSubscribed,
			"a failed subscribe should leave the subscriber inactive")
	})

	t.Run("rejects invalid construction", func(t *testing.T) {
		client := newClient(t)
		handler := func(context.Context, messaging.Message) error { return nil }

		_, err := kafka.NewSubscriber(nil, "acme.accounts", "account-mailer", handler)
		require.Error(t, err)

		_, err = kafka.NewSubscriber(client, "", "account-mailer", handler)
		require.Error(t, err)

		_, err = kafka.NewSubscriber(client, "acme.accounts", "", handler)
		require.Error(t, err)

		_, err = kafka.NewSubscriber(client, "acme.accounts", "account-mailer", nil)
		require.Error(t, err)
	})

	t.Run("rejects a config without brokers", func(t *testing.T) {
		_, err := kafka.NewClient(kafka.Config{}, messagingtest.NewLogger(t))
		require.Error(t, err)
	})
}

func TestPublisher(t *testing.T) {
	t.Run("is a no-op without messages", func(t *testing.T) {
		client := newClient(t, "acme.accounts")
		require.NoError(t, newPublisher(t, client).Publish(t.Context()))
	})

	t.Run("validates every message before producing", func(t *testing.T) {
		client := newClient(t, "acme.accounts")
		pub := newPublisher(t, client)

		err := pub.Publish(t.Context(),
			messaging.NewMessage("acme.accounts", nil),
			messaging.NewMessage("", nil),
		)
		require.Error(t, err)

		mailer := collect(t, client, "acme.accounts", "account-mailer")
		time.Sleep(100 * time.Millisecond)
		assert.Empty(t, mailer.events(t), "an invalid batch should publish nothing")
	})

	t.Run("reports partial failures with a BatchError", func(t *testing.T) {
		client := newClient(t, "acme.accounts")
		pub := newPublisher(t, client)

		ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
		defer cancel()
		err := pub.Publish(ctx,
			messaging.NewMessage("acme.accounts", []byte(`{"name":"ok"}`)),
			messaging.NewMessage("acme.missing", []byte(`{"name":"lost"}`)),
		)

		var batchErr *messaging.BatchError
		require.ErrorAs(t, err, &batchErr)
		require.Len(t, batchErr.Failed, 1)
		assert.Contains(t, batchErr.Failed, 1, "only the unknown-topic message should fail")
	})
}
