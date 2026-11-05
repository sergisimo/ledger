package redis

import (
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// Entry field names. Headers are stored one field each under headerPrefix.
const (
	fieldID      = "id"
	fieldKey     = "key"
	fieldPayload = "payload"
	headerPrefix = "h:"
)

// toValues maps a message to the fields of a stream entry.
func toValues(msg messaging.Message) map[string]any {
	values := make(map[string]any, len(msg.Headers)+3)
	values[fieldPayload] = string(msg.Payload)
	if msg.ID != "" {
		values[fieldID] = msg.ID
	}
	if msg.Key != "" {
		values[fieldKey] = msg.Key
	}
	for key, value := range msg.Headers {
		values[headerPrefix+key] = value
	}

	return values
}

// toMessage maps a stream entry of topic back to a message. An entry added
// outside this package without an id field gets its stream coordinate as ID
// so consumers can still deduplicate redeliveries.
func toMessage(topic string, entry redis.XMessage) messaging.Message {
	msg := messaging.Message{Topic: topic, Headers: map[string]string{}}
	for field, raw := range entry.Values {
		value, _ := raw.(string)
		switch {
		case field == fieldID:
			msg.ID = value
		case field == fieldKey:
			msg.Key = value
		case field == fieldPayload:
			msg.Payload = []byte(value)
		case strings.HasPrefix(field, headerPrefix):
			msg.Headers[strings.TrimPrefix(field, headerPrefix)] = value
		}
	}
	if msg.ID == "" {
		msg.ID = topic + "/" + entry.ID
	}

	return msg
}
