// Package outbox implements the transactional outbox pattern for the
// messaging contracts: a MessagePublisher that persists messages inside the
// caller's database transaction and a Relay that forwards them to the real
// broker, giving at-least-once delivery that is atomic with business writes.
//
// Publish inside persistence.Transactioner.Exec so the outbox rows commit or
// roll back with the rest of the transaction. Consumers must be idempotent
// and tolerate reordering: redelivery and retries can duplicate and reorder
// messages.
package outbox

import (
	"context"
	"time"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

type (
	// Store persists outbox messages. outboxsql provides the SQL
	// implementation; outboxtest provides a database-backed store for tests.
	Store interface {
		// Save persists messages as pending, joining the transaction carried
		// by ctx when there is one.
		Save(ctx context.Context, msgs ...messaging.Message) error
		// FetchPending locks and returns up to limit messages due at now with
		// fewer than maxAttempts attempts (0 = no cap), in insertion order.
		// It must be called inside a transaction.
		FetchPending(ctx context.Context, now time.Time, limit, maxAttempts int) ([]Pending, error)
		// MarkDispatched stamps rows as delivered at now.
		MarkDispatched(ctx context.Context, now time.Time, ids ...string) error
		// Reschedule records a failed delivery attempt and when to retry.
		Reschedule(ctx context.Context, id string, attempts int, nextAttemptAt time.Time, lastError string) error
		// Delete removes rows by id.
		Delete(ctx context.Context, ids ...string) error
		// DeleteDispatchedBefore prunes rows dispatched before cutoff and
		// returns how many were removed.
		DeleteDispatchedBefore(ctx context.Context, cutoff time.Time) (int64, error)
	}

	// Pending is a stored message awaiting delivery.
	Pending struct {
		messaging.Message

		// Attempts is the number of failed delivery attempts so far.
		Attempts int
	}
)
