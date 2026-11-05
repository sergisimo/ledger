package messaging

import (
	"encoding/json"

	"github.com/google/uuid"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

const (
	FieldNameTopic    fields.Name = "topic"
	FieldNameHandler  fields.Name = "handler"
	FieldNameConsumer fields.Name = "consumer"
)

type (
	// Message is the envelope brokers and the outbox exchange. Use the typed
	// Publisher[T] and Handle instead of building messages by hand unless you
	// need full control over the payload encoding.
	Message struct {
		// ID uniquely identifies the message; use it to deduplicate
		// redeliveries.
		ID string
		// Topic is the destination topic.
		Topic string
		// Key is a partitioning/ordering hint; brokers may ignore it.
		Key string
		// Headers carries metadata such as the content type and the trace
		// context. See the Header constants.
		Headers map[string]string
		// Payload is the encoded message body.
		Payload []byte
	}
)

// Header keys used by the package.
const (
	HeaderContentType = "content-type"
	HeaderTraceParent = "traceparent"
	HeaderTraceState  = "tracestate"

	// HeaderMessageID and HeaderMessageKey carry Message.ID and Message.Key
	// on brokers without a native field for them, so consumers can still
	// deduplicate redeliveries and read the key.
	HeaderMessageID  = "message-id"
	HeaderMessageKey = "message-key"

	// HeaderDeadLetterOrigin and HeaderDeadLetterReason are set on messages a
	// broker package moves to a dead-letter topic: the topic the message
	// failed on and the error that exhausted it.
	HeaderDeadLetterOrigin = "dead-letter-origin"
	HeaderDeadLetterReason = "dead-letter-reason"

	// ContentTypeJSON is the HeaderContentType value set by NewJSONMessage.
	ContentTypeJSON = "application/json"
)

// NewMessage builds a message for topic with a unique ID and a raw payload.
func NewMessage(topic string, payload []byte) Message {
	return Message{
		ID:      uuid.NewString(),
		Topic:   topic,
		Headers: map[string]string{},
		Payload: payload,
	}
}

// NewJSONMessage builds a message whose payload is v encoded as JSON.
func NewJSONMessage(topic string, v any) (Message, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return Message{}, err
	}

	msg := NewMessage(topic, payload)
	msg.Headers[HeaderContentType] = ContentTypeJSON

	return msg, nil
}

// Validate checks that the message is publishable: a non-empty topic. Broker
// packages call it on every message before accepting a batch.
func (m Message) Validate() error {
	if m.Topic == "" {
		return fields.NewErrInvalidEmptyString(FieldNameTopic)
	}

	return nil
}

// Bind decodes the JSON payload into v. An empty payload is a no-op.
func (m Message) Bind(v any) error {
	if len(m.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(m.Payload, v)
}
