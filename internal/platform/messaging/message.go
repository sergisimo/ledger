package messaging

import (
	"encoding/json"

	"github.com/google/uuid"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

// FieldNameTopic names the topic field in validation errors, e.g.
// fields.NewErrInvalidEmptyString(FieldNameTopic) for an empty topic.
const FieldNameTopic fields.Name = "topic"

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

// Bind decodes the JSON payload into v. An empty payload is a no-op.
func (m Message) Bind(v any) error {
	if len(m.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(m.Payload, v)
}
