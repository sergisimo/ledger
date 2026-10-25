package ws

import (
	"encoding/json"
	"time"
)

// --------------------------------------------------------------- Contract

type (
	Message struct {
		ID      string          `json:"id,omitempty"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload,omitempty"`
	}

	AuthPayload struct {
		Token string `json:"token"`
	}

	AuthOKPayload struct {
		Subject   string     `json:"subject,omitempty"`
		ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	}

	ErrorPayload struct {
		Message string `json:"message"`
	}
)

const (
	TypeAuth        = "auth"
	TypeAuthOK      = "auth.ok"
	TypeAuthRefresh = "auth.refresh"
	TypeError       = "error"
)

// --------------------------------------------------------------- Implementation

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

func (m Message) Bind(v any) error {
	if len(m.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(m.Payload, v)
}
