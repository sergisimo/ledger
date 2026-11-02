package outboxtest

import (
	"sync"
	"time"
)

// Clock is a settable clock for deterministic relay backoff tests; wire its
// Now method with outbox.RelayWithClock and move time with Advance.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock returns a Clock frozen at now.
func NewClock(now time.Time) *Clock {
	return &Clock{now: now}
}

// Now returns the current clock time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}
