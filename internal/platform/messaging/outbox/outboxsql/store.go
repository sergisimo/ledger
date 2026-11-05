// Package outboxsql implements the outbox.Store on a gorm database (Postgres
// and MySQL). Apply the schema with AutoMigrate; writes join the transaction
// carried by the context (persistence.Transactioner), and pending fetches use
// FOR UPDATE SKIP LOCKED so concurrent relays never deliver the same row.
package outboxsql

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
	"github.com/sergisimo/ledger/internal/platform/persistence"
	"github.com/sergisimo/ledger/internal/platform/query"
)

const FieldNameTransaction fields.Name = "transaction"

// Store is the gorm-backed outbox.Store. Most applications wire it with
// StoreFx.
type Store struct {
	db *gorm.DB
}

// NewStore builds a Store on db. Apply AutoMigrate first.
func NewStore(db *gorm.DB) *Store {
	return &Store{db: db}
}

// Save persists msgs as pending rows, joining the ambient transaction when ctx
// carries one.
func (s *Store) Save(ctx context.Context, msgs ...messaging.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	now := time.Now().UTC()
	models := make([]outboxMessage, 0, len(msgs))
	for _, msg := range msgs {
		model, err := newOutboxMessage(msg, now)
		if err != nil {
			return err
		}
		models = append(models, model)
	}

	return s.session(ctx).Create(&models).Error
}

// FetchPending locks (FOR UPDATE SKIP LOCKED) and returns up to limit messages
// due at now with fewer than maxAttempts attempts (0 = no cap), in insertion
// order. It must run inside the transaction carried by ctx.
func (s *Store) FetchPending(ctx context.Context, now time.Time, limit, maxAttempts int) ([]outbox.Pending, error) {
	tx, ok := persistence.TxFromCtx[*gorm.DB](ctx)
	if !ok {
		return nil, fields.NewErrNil(FieldNameTransaction)
	}

	op := tx.WithContext(ctx).
		Clauses(clause.Locking{
			Strength: query.LockStrengthUpdate.String(),
			Options:  query.LockWaitSkip.String(),
		}).
		Where("dispatched_at IS NULL").
		Where("next_attempt_at <= ?", now)
	if maxAttempts > 0 {
		op = op.Where("attempts < ?", maxAttempts)
	}

	var models []outboxMessage
	if err := op.Order("position").Limit(limit).Find(&models).Error; err != nil {
		return nil, err
	}

	pending := make([]outbox.Pending, 0, len(models))
	for _, model := range models {
		p, err := model.pending()
		if err != nil {
			return nil, err
		}
		pending = append(pending, p)
	}

	return pending, nil
}

// MarkDispatched stamps the rows with ids as dispatched at now.
func (s *Store) MarkDispatched(ctx context.Context, now time.Time, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}

	return s.session(ctx).
		Model(&outboxMessage{}).
		Where("id IN ?", ids).
		Update("dispatched_at", now).
		Error
}

// Reschedule records a failed delivery attempt and when to retry.
func (s *Store) Reschedule(ctx context.Context, id string, attempts int, nextAttemptAt time.Time, lastError string) error {
	return s.session(ctx).
		Model(&outboxMessage{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"attempts":        attempts,
			"next_attempt_at": nextAttemptAt,
			"last_error":      lastError,
		}).
		Error
}

// Delete removes the rows with ids.
func (s *Store) Delete(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}

	return s.session(ctx).Where("id IN ?", ids).Delete(&outboxMessage{}).Error
}

// DeleteDispatchedBefore prunes rows dispatched before cutoff and returns how
// many were removed.
func (s *Store) DeleteDispatchedBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	res := s.session(ctx).
		Where("dispatched_at IS NOT NULL AND dispatched_at < ?", cutoff).
		Delete(&outboxMessage{})

	return res.RowsAffected, res.Error
}

func (s *Store) session(ctx context.Context) *gorm.DB {
	if tx, ok := persistence.TxFromCtx[*gorm.DB](ctx); ok {
		return tx.WithContext(ctx)
	}

	return s.db.WithContext(ctx)
}
