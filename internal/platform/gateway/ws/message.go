package ws

import (
	"encoding/json"
	"time"
)

// --------------------------------------------------------------- Contract

type (
	// Message is the JSON envelope of every frame.
	Message struct {
		ID      string          `json:"id,omitempty"`
		Type    string          `json:"type"`
		Topic   string          `json:"topic,omitempty"`
		Payload json.RawMessage `json:"payload,omitempty"`
	}

	// AuthPayload is the payload of TypeAuth.
	AuthPayload struct {
		Token string `json:"token"`
	}

	// AuthOKPayload is the payload of TypeAuthOK.
	AuthOKPayload struct {
		Subject   string     `json:"subject,omitempty"`
		ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	}

	// SubscribePayload is the payload of the subscription types.
	SubscribePayload struct {
		Topic string `json:"topic"`
	}

	// ErrorPayload is the payload of TypeError.
	ErrorPayload struct {
		Message string `json:"message"`
	}
)

const (
	// Message types defined by the package.
	TypeAuth          = "auth"
	TypeAuthOK        = "auth.ok"
	TypeAuthRefresh   = "auth.refresh"
	TypeSubscribe     = "subscribe"
	TypeSubscribeOK   = "subscribe.ok"
	TypeUnsubscribe   = "unsubscribe"
	TypeUnsubscribeOK = "unsubscribe.ok"
	TypeEvent         = "event"
	TypeError         = "error"
)

// --------------------------------------------------------------- Implementation

// NewMessage encodes v as the payload of a message.
func NewMessage(id, msgType string, v any) (Message, error) {
	msg := Message{ID: id, Type: msgType}
	if v == nil {
		return msg, nil
	}

	payload, err := json.Marshal(v)
	if err != nil {
		return Message{}, err
	}
	msg.Payload = payload

	return msg, nil
}

// NewTopicMessage builds the TypeEvent message delivered to the subscribers of topic.
func NewTopicMessage(topic string, v any) (Message, error) {
	msg, err := NewMessage("", TypeEvent, v)
	if err != nil {
		return Message{}, err
	}
	msg.Topic = topic

	return msg, nil
}

// Bind decodes the payload into v. An empty payload is a no-op.
func (m Message) Bind(v any) error {
	if len(m.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(m.Payload, v)
}
