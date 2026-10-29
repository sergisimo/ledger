package wstest

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/gateway/ws"
)

type (
	// SubscriptionHandler records hook calls. Allow, when set, decides OnSubscribe.
	SubscriptionHandler struct {
		Allow func(conn *ws.Conn, topic string) error

		mu           sync.Mutex
		subscribed   []Subscription
		unsubscribed []Unsubscription
	}

	// Subscription is a recorded OnSubscribe call.
	Subscription struct {
		ConnID ws.ConnID
		Topic  string
	}

	// Unsubscription is a recorded OnUnsubscribe call.
	Unsubscription struct {
		ConnID ws.ConnID
		Topic  string
		Reason ws.UnsubscribeReason
	}
)

// NewSubscriptionHandler returns an empty recording handler.
func NewSubscriptionHandler() *SubscriptionHandler {
	return &SubscriptionHandler{}
}

// OnSubscribe records the call after Allow accepts it.
func (h *SubscriptionHandler) OnSubscribe(_ context.Context, conn *ws.Conn, topic string) error {
	if h.Allow != nil {
		if err := h.Allow(conn, topic); err != nil {
			return err
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.subscribed = append(h.subscribed, Subscription{ConnID: conn.ID(), Topic: topic})

	return nil
}

// OnUnsubscribe records the call.
func (h *SubscriptionHandler) OnUnsubscribe(_ context.Context, conn *ws.Conn, topic string, reason ws.UnsubscribeReason) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.unsubscribed = append(h.unsubscribed, Unsubscription{ConnID: conn.ID(), Topic: topic, Reason: reason})
}

// Subscribed returns the recorded subscriptions in order.
func (h *SubscriptionHandler) Subscribed() []Subscription {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.subscribed)
}

// Unsubscribed returns the recorded unsubscriptions in order.
func (h *SubscriptionHandler) Unsubscribed() []Unsubscription {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.unsubscribed)
}

// ConnID returns the connection of the first recorded subscription.
func (h *SubscriptionHandler) ConnID(t *testing.T) ws.ConnID {
	t.Helper()

	subscribed := h.Subscribed()
	require.NotEmpty(t, subscribed, "no subscription has been accepted yet")

	return subscribed[0].ConnID
}

// Subscribe requests topic and requires a subscribe.ok reply.
func Subscribe(t *testing.T, conn *websocket.Conn, topic string) ws.Message {
	t.Helper()

	res := RequestSubscribe(t, conn, topic)
	require.Equal(t, ws.TypeSubscribeOK, res.Type, "expected the subscription to be accepted: %s", res.Payload)

	return res
}

// RequestSubscribe requests topic and returns the raw reply.
func RequestSubscribe(t *testing.T, conn *websocket.Conn, topic string) ws.Message {
	t.Helper()

	msg, err := ws.NewMessage("sub-"+topic, ws.TypeSubscribe, ws.SubscribePayload{Topic: topic})
	require.NoError(t, err)
	Send(t, conn, msg)

	return Receive(t, conn)
}

// Unsubscribe leaves topic and requires an unsubscribe.ok reply.
func Unsubscribe(t *testing.T, conn *websocket.Conn, topic string) ws.Message {
	t.Helper()

	msg, err := ws.NewMessage("unsub-"+topic, ws.TypeUnsubscribe, ws.SubscribePayload{Topic: topic})
	require.NoError(t, err)
	Send(t, conn, msg)

	res := Receive(t, conn)
	require.Equal(t, ws.TypeUnsubscribeOK, res.Type, "expected the unsubscription to be confirmed: %s", res.Payload)

	return res
}

// ReceiveEvent reads one message and requires it to be an event for topic.
func ReceiveEvent(t *testing.T, conn *websocket.Conn, topic string) ws.Message {
	t.Helper()

	msg := Receive(t, conn)
	require.Equal(t, ws.TypeEvent, msg.Type)
	require.Equal(t, topic, msg.Topic)

	return msg
}

// ErrorMessage requires msg to be an error and returns its text.
func ErrorMessage(t *testing.T, msg ws.Message) string {
	t.Helper()

	require.Equal(t, ws.TypeError, msg.Type, "expected an error message, got %q", msg.Type)

	var payload ws.ErrorPayload
	require.NoError(t, msg.Bind(&payload))

	return payload.Message
}
