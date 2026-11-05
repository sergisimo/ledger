package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// toRecord maps a message to a Kafka record. Kafka records have no native
// message ID, so Message.ID travels in the messaging.HeaderMessageID header.
func toRecord(msg messaging.Message) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(msg.Headers)+1)
	for key, value := range msg.Headers {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	if msg.ID != "" {
		headers = append(headers, kgo.RecordHeader{Key: messaging.HeaderMessageID, Value: []byte(msg.ID)})
	}

	var key []byte
	if msg.Key != "" {
		key = []byte(msg.Key)
	}

	return &kgo.Record{
		Topic:   msg.Topic,
		Key:     key,
		Value:   msg.Payload,
		Headers: headers,
	}
}

// toMessage maps a fetched record back to a message. A record published
// outside this package, without the message ID header, gets the stable
// topic/partition/offset coordinate as its ID so consumers can still
// deduplicate redeliveries.
func toMessage(rec *kgo.Record) messaging.Message {
	headers := make(map[string]string, len(rec.Headers))
	for _, header := range rec.Headers {
		headers[header.Key] = string(header.Value)
	}

	id := headers[messaging.HeaderMessageID]
	delete(headers, messaging.HeaderMessageID)
	if id == "" {
		id = fmt.Sprintf("%s/%d/%d", rec.Topic, rec.Partition, rec.Offset)
	}

	return messaging.Message{
		ID:      id,
		Topic:   rec.Topic,
		Key:     string(rec.Key),
		Headers: headers,
		Payload: rec.Value,
	}
}
