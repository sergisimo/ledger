package redis_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/orlangure/gnomock"
	redispreset "github.com/orlangure/gnomock/preset/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/messaging/redis"
)

// waitTimeout bounds every wait against the shared server container.
const waitTimeout = 10 * time.Second

// testURL points at the Redis 7 container started by TestMain and shared by
// every test; tests isolate through unique topic names.
var testURL string

func TestMain(m *testing.M) {
	container, err := gnomock.Start(redispreset.Preset(redispreset.WithVersion("7.4")))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start the redis container: %v\n", err)
		os.Exit(1)
	}
	testURL = fmt.Sprintf("redis://%s:%d/0", container.Host, container.DefaultPort())

	code := m.Run()

	if err := gnomock.Stop(container); err != nil {
		fmt.Fprintf(os.Stderr, "failed to stop the redis container: %v\n", err)
	}
	os.Exit(code)
}

type event struct {
	Name string `json:"name"`
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()

	client, err := redis.NewClient(redis.Config{URL: testURL}, messagingtest.NewLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		assert.NoError(t, client.Close(ctx))
	})

	return client
}

func newPublisher(t *testing.T, client *redis.Client) *redis.Publisher {
	t.Helper()

	pub := redis.NewPublisher(client)
	require.NoError(t, pub.Open(t.Context()))

	return pub
}

func newTypedPublisher(t *testing.T, client *redis.Client, topic string) messaging.Publisher[event] {
	t.Helper()

	pub, err := messaging.NewPublisher[event](newPublisher(t, client), topic, redis.SystemName)
	require.NoError(t, err)

	return pub
}

func subscribe(
	t *testing.T, client *redis.Client, onError messaging.OnErrorFunc,
	topic, name string, h messaging.HandlerFunc, opts ...redis.SubscriberOpt,
) *redis.Subscriber {
	t.Helper()

	sub, err := redis.NewSubscriber(client, topic, name, h, opts...)
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

// collector records the typed payloads a subscription receives.
type collector struct {
	mu       sync.Mutex
	payloads []event
	msgs     []messaging.Message
}

func collect(t *testing.T, client *redis.Client, topic, name string, opts ...redis.SubscriberOpt) *collector {
	t.Helper()

	c := &collector{}
	subscribe(t, client, messagingtest.FailOnError(t), topic, name, c.handler(), opts...)

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
	t.Run("delivers typed events to every consumer name in order, even ones published first", func(t *testing.T) {
		client := newClient(t)

		pub := newTypedPublisher(t, client, "acme.pubsub.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}, event{Name: "savings"}))

		mailer := collect(t, client, "acme.pubsub.accounts", "account-mailer")
		auditor := collect(t, client, "acme.pubsub.accounts", "account-auditor")
		other := collect(t, client, "acme.pubsub.entries", "entry-auditor")

		want := []event{{Name: "checking"}, {Name: "savings"}}
		assert.Equal(t, want, mailer.wait(t, 2), "the stream should retain entries published before the group existed")
		assert.Equal(t, want, auditor.wait(t, 2))

		msgs := mailer.messages(t)
		require.Len(t, msgs, 2)
		assert.NotEmpty(t, msgs[0].ID)
		assert.NotEqual(t, msgs[0].ID, msgs[1].ID)
		for _, msg := range msgs {
			assert.Equal(t, "acme.pubsub.accounts", msg.Topic)
			assert.Equal(t, messaging.ContentTypeJSON, msg.Headers[messaging.HeaderContentType])
		}

		time.Sleep(100 * time.Millisecond)
		assert.Empty(t, other.events(t), "other topics should not receive the events")
	})

	t.Run("instances sharing a consumer name compete for the entries", func(t *testing.T) {
		client := newClient(t)

		first := collect(t, client, "acme.compete.accounts", "account-mailer")
		second := collect(t, client, "acme.compete.accounts", "account-mailer")

		pub := newTypedPublisher(t, client, "acme.compete.accounts")
		const total = 6
		for i := range total {
			require.NoError(t, pub.Publish(t.Context(), event{Name: fmt.Sprintf("m%d", i)}))
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
		assert.Equal(t, total, got, "the group should deliver each entry to one instance")
	})

	t.Run("the key round-trips through its field", func(t *testing.T) {
		client := newClient(t)
		coll := collect(t, client, "acme.keys.accounts", "account-mailer")

		pub := newPublisher(t, client)
		msg, err := messaging.NewJSONMessage("acme.keys.accounts", event{Name: "keyed"})
		require.NoError(t, err)
		msg.Key = "account-42"
		require.NoError(t, pub.Publish(t.Context(), msg))

		coll.wait(t, 1)
		assert.Equal(t, "account-42", coll.messages(t)[0].Key)
	})
}

func TestSubscriberFailures(t *testing.T) {
	t.Run("retries transient errors with backoff until the handler succeeds", func(t *testing.T) {
		client := newClient(t)

		var mu sync.Mutex
		attempts := 0
		done := make(chan struct{})
		subscribe(t, client, messagingtest.FailOnError(t), "acme.retry.accounts", "account-mailer",
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
			redis.SubscriberWithBackoff(time.Millisecond, time.Millisecond),
		)

		pub := newTypedPublisher(t, client, "acme.retry.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "checking"}))

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
		client := newClient(t)

		var calls sync.Map
		subscribe(t, client, messagingtest.FailOnError(t), "acme.dead.accounts", "account-mailer",
			func(_ context.Context, msg messaging.Message) error {
				count, _ := calls.LoadOrStore(msg.ID, new(int))
				*(count.(*int))++
				return messaging.Permanent(assert.AnError)
			},
			redis.SubscriberWithDeadLetterTopic("acme.dead.accounts.dead"),
		)
		dead := collect(t, client, "acme.dead.accounts.dead", "dead-letter-auditor")

		pub := newTypedPublisher(t, client, "acme.dead.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "poison"}))

		assert.Equal(t, []event{{Name: "poison"}}, dead.wait(t, 1))

		msg := dead.messages(t)[0]
		assert.Equal(t, "acme.dead.accounts", msg.Headers[messaging.HeaderDeadLetterOrigin])
		assert.Contains(t, msg.Headers[messaging.HeaderDeadLetterReason], assert.AnError.Error())
		assert.Equal(t, messaging.ContentTypeJSON, msg.Headers[messaging.HeaderContentType])

		calls.Range(func(_, count any) bool {
			assert.Equal(t, 1, *(count.(*int)), "a permanent failure should not be retried")
			return true
		})
	})

	t.Run("drops and reports exhausted messages without a dead-letter topic", func(t *testing.T) {
		client := newClient(t)

		var rec errorRecorder
		var mu sync.Mutex
		attempts := 0
		processed := make(chan event, 1)
		subscribe(t, client, rec.onError, "acme.exhaust.accounts", "account-mailer",
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
			redis.SubscriberWithMaxAttempts(2),
			redis.SubscriberWithBackoff(time.Millisecond, time.Millisecond),
		)

		pub := newTypedPublisher(t, client, "acme.exhaust.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "cursed"}, event{Name: "fine"}))

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
		client := newClient(t)

		var rec errorRecorder
		received := make(chan event, 2)
		subscribe(t, client, rec.onError, "acme.panic.accounts", "account-mailer",
			messaging.Handle(func(_ context.Context, payload event, _ messaging.Message) error {
				if payload.Name == "boom" {
					panic("kaboom")
				}
				received <- payload
				return nil
			}),
		)

		pub := newTypedPublisher(t, client, "acme.panic.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "boom"}, event{Name: "after"}))

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

	t.Run("a stalled instance's pending entry is reclaimed by another", func(t *testing.T) {
		client := newClient(t)

		release := make(chan struct{})
		defer close(release)
		stalled := make(chan struct{}, 1)
		subscribe(t, client, messagingtest.FailOnError(t), "acme.claim.accounts", "account-mailer",
			func(context.Context, messaging.Message) error {
				stalled <- struct{}{}
				<-release
				return nil
			},
			redis.SubscriberWithClaimIdle(300*time.Millisecond),
		)

		pub := newTypedPublisher(t, client, "acme.claim.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "stuck"}))
		select {
		case <-stalled:
		case <-time.After(waitTimeout):
			t.Fatal("the stalled handler never received the entry")
		}

		rescuer := collect(t, client, "acme.claim.accounts", "account-mailer", redis.SubscriberWithClaimIdle(300*time.Millisecond))
		assert.Equal(t, []event{{Name: "stuck"}}, rescuer.wait(t, 1),
			"the live instance should reclaim the entry pending on the stalled one")
	})
}

func TestSubscriberLifecycle(t *testing.T) {
	t.Run("the group resumes after resubscribing", func(t *testing.T) {
		client := newClient(t)
		received := make(chan event, 4)
		handler := messaging.Handle(func(_ context.Context, payload event, _ messaging.Message) error {
			received <- payload
			return nil
		})

		sub, err := redis.NewSubscriber(client, "acme.resub.accounts", "account-mailer", handler)
		require.NoError(t, err)
		require.NoError(t, sub.Subscribe(t.Context(), messagingtest.FailOnError(t)))

		pub := newTypedPublisher(t, client, "acme.resub.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "first"}))
		select {
		case got := <-received:
			assert.Equal(t, event{Name: "first"}, got)
		case <-time.After(waitTimeout):
			t.Fatal("the first message never arrived")
		}

		require.NoError(t, sub.Unsubscribe(t.Context()))
		require.NoError(t, pub.Publish(t.Context(), event{Name: "second"}),
			"publishing while unsubscribed should retain the entry in the stream")

		require.NoError(t, sub.Subscribe(t.Context(), messagingtest.FailOnError(t)))
		defer func() { assert.NoError(t, sub.Unsubscribe(context.Background())) }()

		select {
		case got := <-received:
			assert.Equal(t, event{Name: "second"}, got, "the acknowledged entry should not be redelivered")
		case <-time.After(waitTimeout):
			t.Fatal("the second message never arrived")
		}
	})

	t.Run("subscribe twice and unsubscribe twice are errors", func(t *testing.T) {
		client := newClient(t)
		handler := func(context.Context, messaging.Message) error { return nil }

		sub := subscribe(t, client, nil, "acme.lifecycle.accounts", "account-mailer", handler)
		require.ErrorIs(t, sub.Subscribe(t.Context(), nil), messaging.ErrAlreadySubscribed)

		require.NoError(t, sub.Unsubscribe(t.Context()))
		require.ErrorIs(t, sub.Unsubscribe(t.Context()), messaging.ErrNotSubscribed)
	})

	t.Run("subscribe fails fast on an unreachable server", func(t *testing.T) {
		client, err := redis.NewClient(redis.Config{URL: "redis://127.0.0.1:1/0"}, messagingtest.NewLogger(t))
		require.NoError(t, err)

		sub, err := redis.NewSubscriber(client, "acme.unreachable", "account-mailer",
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

		_, err := redis.NewSubscriber(nil, "acme.accounts", "account-mailer", handler)
		require.Error(t, err)

		_, err = redis.NewSubscriber(client, "", "account-mailer", handler)
		require.Error(t, err)

		_, err = redis.NewSubscriber(client, "acme.accounts", "", handler)
		require.Error(t, err)

		_, err = redis.NewSubscriber(client, "acme.accounts", "account-mailer", nil)
		require.Error(t, err)
	})

	t.Run("rejects a config without a valid URL", func(t *testing.T) {
		_, err := redis.NewClient(redis.Config{}, messagingtest.NewLogger(t))
		require.Error(t, err)

		_, err = redis.NewClient(redis.Config{URL: "http://not-redis"}, messagingtest.NewLogger(t))
		require.Error(t, err)
	})
}

func TestPublisher(t *testing.T) {
	t.Run("is a no-op without messages", func(t *testing.T) {
		require.NoError(t, newPublisher(t, newClient(t)).Publish(t.Context()))
	})

	t.Run("validates every message before appending", func(t *testing.T) {
		client := newClient(t)
		coll := collect(t, client, "acme.validate.accounts", "account-mailer")

		err := newPublisher(t, client).Publish(t.Context(),
			messaging.NewMessage("acme.validate.accounts", nil),
			messaging.NewMessage("", nil),
		)
		require.Error(t, err)

		time.Sleep(100 * time.Millisecond)
		assert.Empty(t, coll.events(t), "an invalid batch should publish nothing")
	})

	t.Run("reports partial failures with a BatchError", func(t *testing.T) {
		// A key holding a string cannot be appended to as a stream.
		opts, err := goredis.ParseURL(testURL)
		require.NoError(t, err)
		raw := goredis.NewClient(opts)
		t.Cleanup(func() { assert.NoError(t, raw.Close()) })
		require.NoError(t, raw.Set(t.Context(), "acme.wrongtype.accounts", "not-a-stream", 0).Err())

		pub := newPublisher(t, newClient(t))
		err = pub.Publish(t.Context(),
			messaging.NewMessage("acme.partial.accounts", []byte(`{}`)),
			messaging.NewMessage("acme.wrongtype.accounts", []byte(`{}`)),
		)

		var batchErr *messaging.BatchError
		require.ErrorAs(t, err, &batchErr)
		require.Len(t, batchErr.Failed, 1)
		assert.Contains(t, batchErr.Failed, 1, "only the wrong-type key message should fail")
	})

	t.Run("trims streams to the configured length", func(t *testing.T) {
		client, err := redis.NewClient(redis.Config{URL: testURL, StreamMaxLen: 1}, messagingtest.NewLogger(t))
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, client.Close(context.Background())) })

		pub := newTypedPublisher(t, client, "acme.trim.accounts")
		for i := range 50 {
			require.NoError(t, pub.Publish(t.Context(), event{Name: fmt.Sprintf("m%d", i)}))
		}

		opts, err := goredis.ParseURL(testURL)
		require.NoError(t, err)
		raw := goredis.NewClient(opts)
		t.Cleanup(func() { assert.NoError(t, raw.Close()) })
		length, err := raw.XLen(t.Context(), "acme.trim.accounts").Result()
		require.NoError(t, err)
		assert.Less(t, length, int64(50), "approximate trimming should have dropped old entries")
	})
}
