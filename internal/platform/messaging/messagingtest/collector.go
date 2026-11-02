package messagingtest

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/inmem"
)

// Collector records the typed payloads a topic subscription receives. It is
// safe for concurrent use.
type Collector[T any] struct {
	mu       sync.Mutex
	payloads []T
	msgs     []messaging.Message
}

// Collect subscribes a new Collector to topic on broker. The subscription is
// removed at cleanup.
func Collect[T any](t *testing.T, broker *inmem.Broker, topic string, opts ...inmem.SubscriberOpt) *Collector[T] {
	t.Helper()

	c := &Collector[T]{}
	handler := messaging.Handle(func(_ context.Context, payload T, msg messaging.Message) error {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.payloads = append(c.payloads, payload)
		c.msgs = append(c.msgs, msg)

		return nil
	})
	Subscribe(t, broker, topic, handler, opts...)

	return c
}

// Payloads returns a copy of the received payloads in delivery order.
func (c *Collector[T]) Payloads() []T {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.payloads)
}

// Messages returns a copy of the received envelopes in delivery order.
func (c *Collector[T]) Messages() []messaging.Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.msgs)
}

// Wait blocks until at least n payloads arrived and returns them.
func (c *Collector[T]) Wait(t *testing.T, n int) []T {
	t.Helper()

	require.Eventuallyf(t, func() bool {
		return len(c.Payloads()) >= n
	}, Timeout, time.Millisecond, "expected at least %d payloads, got %d", n, len(c.Payloads()))

	return c.Payloads()
}

// ExpectNone asserts that nothing arrives within a short settle window.
func (c *Collector[T]) ExpectNone(t *testing.T) {
	t.Helper()

	time.Sleep(20 * time.Millisecond)
	assert.Empty(t, c.Payloads())
}
