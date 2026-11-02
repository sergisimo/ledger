package outbox_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/messagingtest"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox/outboxtest"
)

type event struct {
	Name string `json:"name"`
}

// failingStore injects store failures the real database cannot be asked for.
type failingStore struct {
	outbox.Store

	mu       sync.Mutex
	saveErr  error
	fetchErr error
}

func (s *failingStore) failSaveWith(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.saveErr = err
}

func (s *failingStore) failFetchWith(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.fetchErr = err
}

func (s *failingStore) Save(ctx context.Context, msgs ...messaging.Message) error {
	s.mu.Lock()
	err := s.saveErr
	s.mu.Unlock()

	if err != nil {
		return err
	}

	return s.Store.Save(ctx, msgs...)
}

func (s *failingStore) FetchPending(ctx context.Context, now time.Time, limit, maxAttempts int) ([]outbox.Pending, error) {
	s.mu.Lock()
	err := s.fetchErr
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return s.Store.FetchPending(ctx, now, limit, maxAttempts)
}

// flakyPublisher fails the first failures publishes; a negative value always fails.
type flakyPublisher struct {
	inner messaging.MessagePublisher

	mu       sync.Mutex
	failures int
	calls    int
}

func (f *flakyPublisher) Publish(ctx context.Context, msgs ...messaging.Message) error {
	f.mu.Lock()
	f.calls++
	fail := f.failures != 0
	if f.failures > 0 {
		f.failures--
	}
	f.mu.Unlock()

	if fail {
		return assert.AnError
	}
	if f.inner != nil {
		return f.inner.Publish(ctx, msgs...)
	}

	return nil
}

func (f *flakyPublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

func newTypedPublisher(t *testing.T, store outbox.Store, topic string) messaging.Publisher[event] {
	t.Helper()

	pub, err := messaging.NewPublisher[event](outbox.NewPublisher(store), topic)
	require.NoError(t, err)

	return pub
}

func startRelay(t *testing.T, relay *outbox.Relay) {
	t.Helper()

	require.NoError(t, relay.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), messagingtest.Timeout)
		defer cancel()

		assert.NoError(t, relay.Stop(ctx))
	})
}

func fastCfg() outbox.RelayConfig {
	return outbox.RelayConfig{PollInterval: 2 * time.Millisecond, Retention: time.Hour}
}

func firstRowID(t *testing.T, store *outboxtest.Store) string {
	t.Helper()

	rows := store.Rows(t)
	require.NotEmpty(t, rows)

	return rows[0].ID
}
