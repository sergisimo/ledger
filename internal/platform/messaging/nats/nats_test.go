package nats_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/messaging/nats"
)

// waitTimeout bounds every wait against the embedded server.
const waitTimeout = 10 * time.Second

// testURL points at the JetStream-enabled server embedded by TestMain and
// shared by every test; tests isolate through unique topic names.
var testURL string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "nats-test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create the jetstream store dir: %v\n", err)
		os.Exit(1)
	}

	srv, err := server.NewServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  dir,
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build the nats server: %v\n", err)
		os.Exit(1)
	}
	go srv.Start()
	if !srv.ReadyForConnections(waitTimeout) {
		fmt.Fprintln(os.Stderr, "the nats server never became ready")
		os.Exit(1)
	}
	testURL = srv.ClientURL()

	code := m.Run()

	srv.Shutdown()
	srv.WaitForShutdown()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type event struct {
	Name string `json:"name"`
}

func newClient(t *testing.T) *nats.Client {
	t.Helper()

	client, err := nats.NewClient(nats.Config{URL: testURL, Name: "nats-test"}, messagingtest.NewLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		assert.NoError(t, client.Close(ctx))
	})

	return client
}

func newPublisher(t *testing.T, client *nats.Client) *nats.Publisher {
	t.Helper()

	pub := nats.NewPublisher(client)
	require.NoError(t, pub.Open(t.Context()))

	return pub
}

func newTypedPublisher(t *testing.T, client *nats.Client, topic string) messaging.Publisher[event] {
	t.Helper()

	pub, err := messaging.NewPublisher[event](newPublisher(t, client), topic, nats.SystemName)
	require.NoError(t, err)

	return pub
}

func subscribe(
	t *testing.T, client *nats.Client, onError messaging.OnErrorFunc,
	topic, name string, h messaging.HandlerFunc, opts ...nats.SubscriberOpt,
) *nats.Subscriber {
	t.Helper()

	sub, err := nats.NewSubscriber(client, topic, name, h, opts...)
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

func collect(t *testing.T, client *nats.Client, topic, name string, opts ...nats.SubscriberOpt) *collector {
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
		assert.Equal(t, want, mailer.wait(t, 2), "the stream should retain messages published before the consumer existed")
		assert.Equal(t, want, auditor.wait(t, 2))

		msgs := mailer.messages(t)
		require.Len(t, msgs, 2)
		assert.NotEmpty(t, msgs[0].ID)
		assert.NotEqual(t, msgs[0].ID, msgs[1].ID)
		for _, msg := range msgs {
			assert.Equal(t, "acme.pubsub.accounts", msg.Topic)
			assert.Equal(t, messaging.ContentTypeJSON, msg.Headers[messaging.HeaderContentType])
			for key := range msg.Headers {
				assert.NotContains(t, key, "Nats-", "server headers should not leak into the message")
			}
		}

		time.Sleep(100 * time.Millisecond)
		assert.Empty(t, other.events(t), "other topics should not receive the events")
	})

	t.Run("instances sharing a consumer name compete for the messages", func(t *testing.T) {
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
		assert.Equal(t, total, got, "the durable should deliver each message to one instance")
	})

	t.Run("the key round-trips through its header", func(t *testing.T) {
		client := newClient(t)
		coll := collect(t, client, "acme.keys.accounts", "account-mailer")

		pub := newPublisher(t, client)
		msg, err := messaging.NewJSONMessage("acme.keys.accounts", event{Name: "keyed"})
		require.NoError(t, err)
		msg.Key = "account-42"
		require.NoError(t, pub.Publish(t.Context(), msg))

		coll.wait(t, 1)
		got := coll.messages(t)[0]
		assert.Equal(t, "account-42", got.Key)
		assert.NotContains(t, got.Headers, messaging.HeaderMessageKey, "the key header should be lifted into Message.Key")
	})
}

func TestSubscriberFailures(t *testing.T) {
	t.Run("redelivers transient errors with backoff until the handler succeeds", func(t *testing.T) {
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
			nats.SubscriberWithBackoff(time.Millisecond, time.Millisecond),
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
			nats.SubscriberWithDeadLetterTopic("acme.dead.accounts.dead"),
		)
		dead := collect(t, client, "acme.dead.accounts.dead", "dead-letter-auditor")

		pub := newTypedPublisher(t, client, "acme.dead.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "poison"}))

		assert.Equal(t, []event{{Name: "poison"}}, dead.wait(t, 1))

		msg := dead.messages(t)[0]
		assert.Equal(t, "acme.dead.accounts", msg.Headers[messaging.HeaderDeadLetterOrigin])
		assert.Contains(t, msg.Headers[messaging.HeaderDeadLetterReason], assert.AnError.Error())
		assert.Equal(t, messaging.ContentTypeJSON, msg.Headers[messaging.HeaderContentType])

		time.Sleep(100 * time.Millisecond)
		calls.Range(func(_, count any) bool {
			assert.Equal(t, 1, *(count.(*int)), "a permanent failure should not be redelivered")
			return true
		})
	})

	t.Run("terminates and reports exhausted messages without a dead-letter topic", func(t *testing.T) {
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
			nats.SubscriberWithMaxAttempts(2),
			nats.SubscriberWithBackoff(time.Millisecond, time.Millisecond),
		)

		pub := newTypedPublisher(t, client, "acme.exhaust.accounts")
		require.NoError(t, pub.Publish(t.Context(), event{Name: "cursed"}, event{Name: "fine"}))

		select {
		case got := <-processed:
			assert.Equal(t, event{Name: "fine"}, got, "the worker should survive the termination")
		case <-time.After(waitTimeout):
			t.Fatal("the message after the terminated one never arrived")
		}

		require.Eventually(t, func() bool { return len(rec.errors()) == 1 }, waitTimeout, 5*time.Millisecond)
		errs := rec.errors()
		assert.ErrorIs(t, errs[0], assert.AnError)
		assert.ErrorContains(t, errs[0], "terminated")

		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 2, attempts, "MaxAttempts should cap the deliveries")
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

		require.Eventually(t, func() bool { return len(rec.errors()) == 1 }, waitTimeout, 5*time.Millisecond)
		assert.ErrorIs(t, rec.errors()[0], messaging.ErrHandlerPanic)
	})
}

func TestSubscriberLifecycle(t *testing.T) {
	t.Run("the durable resumes after resubscribing", func(t *testing.T) {
		client := newClient(t)
		received := make(chan event, 4)
		handler := messaging.Handle(func(_ context.Context, payload event, _ messaging.Message) error {
			received <- payload
			return nil
		})

		sub, err := nats.NewSubscriber(client, "acme.resub.accounts", "account-mailer", handler)
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
			"publishing while unsubscribed should retain the message in the stream")

		require.NoError(t, sub.Subscribe(t.Context(), messagingtest.FailOnError(t)))
		defer func() { assert.NoError(t, sub.Unsubscribe(context.Background())) }()

		select {
		case got := <-received:
			assert.Equal(t, event{Name: "second"}, got, "the acknowledged message should not be redelivered")
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
		client, err := nats.NewClient(nats.Config{URL: "nats://127.0.0.1:1"}, messagingtest.NewLogger(t))
		require.NoError(t, err)

		sub, err := nats.NewSubscriber(client, "acme.unreachable", "account-mailer",
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

		_, err := nats.NewSubscriber(nil, "acme.accounts", "account-mailer", handler)
		require.Error(t, err)

		_, err = nats.NewSubscriber(client, "", "account-mailer", handler)
		require.Error(t, err)

		_, err = nats.NewSubscriber(client, "acme.accounts", "", handler)
		require.Error(t, err)

		_, err = nats.NewSubscriber(client, "acme.accounts", "account-mailer", nil)
		require.Error(t, err)
	})

	t.Run("rejects a config without a URL", func(t *testing.T) {
		_, err := nats.NewClient(nats.Config{}, messagingtest.NewLogger(t))
		require.Error(t, err)
	})
}

func TestPublisher(t *testing.T) {
	t.Run("is a no-op without messages", func(t *testing.T) {
		require.NoError(t, newPublisher(t, newClient(t)).Publish(t.Context()))
	})

	t.Run("validates every message before publishing", func(t *testing.T) {
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
		pub := newPublisher(t, newClient(t))

		// A subject with a space is valid for Message.Validate but rejected
		// by the server when its stream is declared.
		err := pub.Publish(t.Context(),
			messaging.NewMessage("acme.partial.accounts", []byte(`{}`)),
			messaging.NewMessage("acme.invalid subject", []byte(`{}`)),
		)

		var batchErr *messaging.BatchError
		require.ErrorAs(t, err, &batchErr)
		require.Len(t, batchErr.Failed, 1)
		assert.Contains(t, batchErr.Failed, 1, "only the invalid-subject message should fail")
	})
}
