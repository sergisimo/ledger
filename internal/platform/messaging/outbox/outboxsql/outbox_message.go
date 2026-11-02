package outboxsql

import (
	"encoding/json"
	"time"

	"gorm.io/gorm"

	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/messaging/outbox"
)

// TableName is the database table holding the outbox messages.
const TableName = "outbox_message"

type outboxMessage struct {
	Position      int64      `gorm:"primaryKey;autoIncrement"`
	ID            string     `gorm:"size:36;uniqueIndex;not null"`
	Topic         string     `gorm:"size:255;not null"`
	Key           string     `gorm:"size:255;not null"`
	Headers       string     `gorm:"type:text"`
	Payload       []byte     `gorm:"not null"`
	CreatedAt     time.Time  `gorm:"not null"`
	Attempts      int        `gorm:"not null;default:0"`
	NextAttemptAt time.Time  `gorm:"not null;index:idx_outbox_pending"`
	LastError     string     `gorm:"type:text"`
	DispatchedAt  *time.Time `gorm:"index:idx_outbox_dispatched"`
}

func (outboxMessage) TableName() string {
	return TableName
}

// AutoMigrate creates or updates the TableName schema on db.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&outboxMessage{})
}

func newOutboxMessage(msg messaging.Message, now time.Time) (outboxMessage, error) {
	headers, err := json.Marshal(msg.Headers)
	if err != nil {
		return outboxMessage{}, err
	}

	payload := msg.Payload
	if payload == nil {
		payload = []byte{}
	}

	return outboxMessage{
		ID:            msg.ID,
		Topic:         msg.Topic,
		Key:           msg.Key,
		Headers:       string(headers),
		Payload:       payload,
		CreatedAt:     now,
		NextAttemptAt: now,
	}, nil
}

func (m outboxMessage) pending() (outbox.Pending, error) {
	headers := map[string]string{}
	if m.Headers != "" {
		if err := json.Unmarshal([]byte(m.Headers), &headers); err != nil {
			return outbox.Pending{}, err
		}
	}

	return outbox.Pending{
		Message: messaging.Message{
			ID:      m.ID,
			Topic:   m.Topic,
			Key:     m.Key,
			Headers: headers,
			Payload: m.Payload,
		},
		Attempts: m.Attempts,
	}, nil
}
