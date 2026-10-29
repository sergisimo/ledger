package ws_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/authx"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws/wstest"
)

type balanceUpdated struct {
	Balance string `json:"balance"`
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	t.Run("refused when subscriptions are not enabled", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.DialAndAuthenticate(t, server)

		res := wstest.RequestSubscribe(t, conn, "accounts.1")
		assert.Equal(t, "subscriptions are not enabled", wstest.ErrorMessage(t, res))
	})

	t.Run("refused until authenticated", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()))
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)

		res := wstest.RequestSubscribe(t, conn, "accounts.1")
		assert.Equal(t, "not authenticated", wstest.ErrorMessage(t, res))
	})

	t.Run("accepted subscription echoes the request id and topic", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		res := wstest.Subscribe(t, conn, "accounts.1")
		assert.Equal(t, "sub-accounts.1", res.ID)

		var payload ws.SubscribePayload
		require.NoError(t, res.Bind(&payload))
		assert.Equal(t, "accounts.1", payload.Topic)

		assert.Len(t, handler.Subscribed(), 1)
		assert.Equal(t, 1, server.SubscriberCount("accounts.1"))
	})

	t.Run("idempotent: a repeated subscription calls the handler once", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "accounts.1")
		wstest.Subscribe(t, conn, "accounts.1")

		assert.Len(t, handler.Subscribed(), 1)
		assert.Equal(t, 1, server.SubscriberCount("accounts.1"))
	})

	t.Run("handler errors are mapped to client messages", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name string
			err  error
			want string
		}{
			{
				name: "forbidden",
				err:  ws.ErrSubscriptionForbidden,
				want: "forbidden",
			},
			{
				name: "wrapped forbidden",
				err:  errors.Join(ws.ErrSubscriptionForbidden, assert.AnError),
				want: "forbidden",
			},
			{
				name: "unknown topic",
				err:  ws.ErrUnknownTopic,
				want: "unknown topic",
			},
			{
				name: "internal error is not leaked",
				err:  assert.AnError,
				want: "subscription failed",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				handler := wstest.NewSubscriptionHandler()
				handler.Allow = func(*ws.Conn, string) error { return tc.err }
				server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
				conn := wstest.DialAndAuthenticate(t, server)

				res := wstest.RequestSubscribe(t, conn, "accounts.1")
				assert.Equal(t, tc.want, wstest.ErrorMessage(t, res))
				assert.Empty(t, handler.Subscribed())
				assert.Equal(t, 0, server.SubscriberCount("accounts.1"))
			})
		}
	})

	t.Run("handler can authorize against the identity", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		handler.Allow = func(conn *ws.Conn, topic string) error {
			if topic != "accounts."+conn.Identity().Subject {
				return ws.ErrSubscriptionForbidden
			}
			return nil
		}
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "accounts.test-subject")
		res := wstest.RequestSubscribe(t, conn, "accounts.other")
		assert.Equal(t, "forbidden", wstest.ErrorMessage(t, res))
	})

	t.Run("per connection limit is enforced before the handler runs", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t,
			ws.ServerWithSubscriptionHandler(handler),
			ws.ServerWithMaxSubscriptions(2),
		)
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "accounts.1")
		wstest.Subscribe(t, conn, "accounts.2")
		res := wstest.RequestSubscribe(t, conn, "accounts.3")
		assert.Equal(t, "too many subscriptions", wstest.ErrorMessage(t, res))

		assert.Len(t, handler.Subscribed(), 2)
		assert.Empty(t, handler.Unsubscribed())

		wstest.Subscribe(t, conn, "accounts.1")
		wstest.Unsubscribe(t, conn, "accounts.1")
		wstest.Subscribe(t, conn, "accounts.3")
	})

	t.Run("invalid topics are rejected", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t,
			ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()),
			ws.ServerWithMaxTopicLength(16),
		)
		conn := wstest.DialAndAuthenticate(t, server)

		for _, topic := range []string{"", "has space", "tab\tseparated", "new\nline", strings.Repeat("x", 17)} {
			res := wstest.RequestSubscribe(t, conn, topic)
			assert.Equal(t, "invalid topic", wstest.ErrorMessage(t, res), "topic %q", topic)
		}

		wstest.Subscribe(t, conn, strings.Repeat("x", 16))
	})

	t.Run("malformed payload is rejected", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Send(t, conn, ws.Message{ID: "bad", Type: ws.TypeSubscribe, Payload: []byte(`"not an object"`)})
		res := wstest.Receive(t, conn)
		assert.Equal(t, "bad", res.ID)
		assert.Equal(t, "malformed subscription payload", wstest.ErrorMessage(t, res))
	})

	t.Run("lists the connection subscriptions sorted", func(t *testing.T) {
		t.Parallel()

		var (
			mu     sync.Mutex
			topics []string
		)
		handler := ws.SubscriptionHandlerFuncs{
			OnSubscribeFunc: func(_ context.Context, conn *ws.Conn, _ string) error {
				mu.Lock()
				defer mu.Unlock()
				topics = conn.Subscriptions()
				return nil
			},
		}
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "b")
		wstest.Subscribe(t, conn, "c")
		wstest.Subscribe(t, conn, "a")

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, []string{"b", "c"}, topics, "the hook runs before the topic is registered")
	})
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()

	t.Run("removes the subscription and notifies the handler once", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "accounts.1")
		res := wstest.Unsubscribe(t, conn, "accounts.1")
		assert.Equal(t, "unsub-accounts.1", res.ID)
		wstest.Unsubscribe(t, conn, "accounts.1")

		assert.Equal(t, []wstest.Unsubscription{
			{ConnID: handler.ConnID(t), Topic: "accounts.1", Reason: ws.UnsubscribeByClient},
		}, handler.Unsubscribed())
		assert.Equal(t, 0, server.SubscriberCount("accounts.1"))
	})

	t.Run("unknown topic is confirmed without notifying the handler", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Unsubscribe(t, conn, "never.subscribed")
		assert.Empty(t, handler.Unsubscribed())
	})

	t.Run("works even when subscriptions are disabled", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Unsubscribe(t, conn, "accounts.1")
	})

	t.Run("disconnect notifies the handler for every topic", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "accounts.2")
		wstest.Subscribe(t, conn, "accounts.1")
		connID := handler.ConnID(t)

		require.NoError(t, conn.Close(websocket.StatusNormalClosure, ""))

		require.Eventually(t, func() bool { return len(handler.Unsubscribed()) == 2 }, 2*time.Second, 10*time.Millisecond)
		assert.Equal(t, []wstest.Unsubscription{
			{ConnID: connID, Topic: "accounts.1", Reason: ws.UnsubscribeOnDisconnect},
			{ConnID: connID, Topic: "accounts.2", Reason: ws.UnsubscribeOnDisconnect},
		}, handler.Unsubscribed())
		assert.Equal(t, 0, server.SubscriberCount("accounts.1"))
		assert.Equal(t, 0, server.SubscriberCount("accounts.2"))
	})

	t.Run("server side revocation", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "accounts.1")
		connID := handler.ConnID(t)

		assert.True(t, server.Unsubscribe(connID, "accounts.1"))
		assert.False(t, server.Unsubscribe(connID, "accounts.1"))
		assert.False(t, server.Unsubscribe("unknown-conn", "accounts.1"))

		assert.Equal(t, []wstest.Unsubscription{
			{ConnID: connID, Topic: "accounts.1", Reason: ws.UnsubscribeByServer},
		}, handler.Unsubscribed())

		delivered, err := server.Publish(t.Context(), "accounts.1", nil)
		require.NoError(t, err)
		assert.Equal(t, 0, delivered)
	})
}

func TestPublish(t *testing.T) {
	t.Parallel()

	t.Run("delivers to every subscriber of the topic only", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()))
		alice := wstest.DialAndAuthenticate(t, server)
		bob := wstest.DialAndAuthenticate(t, server)
		carol := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, alice, "accounts.1")
		wstest.Subscribe(t, bob, "accounts.1")
		wstest.Subscribe(t, carol, "accounts.2")

		publisher := ws.NewPublisher[balanceUpdated](server.Server)
		delivered, err := publisher.Publish(t.Context(), "accounts.1", balanceUpdated{Balance: "10.00"})
		require.NoError(t, err)
		assert.Equal(t, 2, delivered)

		for _, conn := range []*websocket.Conn{alice, bob} {
			msg := wstest.ReceiveEvent(t, conn, "accounts.1")
			assert.Empty(t, msg.ID)
			assert.JSONEq(t, `{"balance":"10.00"}`, string(msg.Payload))
		}

		_, err = server.Publish(t.Context(), "accounts.2", nil)
		require.NoError(t, err)
		msg := wstest.ReceiveEvent(t, carol, "accounts.2")
		assert.Empty(t, msg.Payload)
	})

	t.Run("stops delivering after unsubscribe", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "accounts.1")
		wstest.Subscribe(t, conn, "accounts.2")
		wstest.Unsubscribe(t, conn, "accounts.1")

		delivered, err := server.Publish(t.Context(), "accounts.1", "stale")
		require.NoError(t, err)
		assert.Equal(t, 0, delivered)

		delivered, err = server.Publish(t.Context(), "accounts.2", "fresh")
		require.NoError(t, err)
		assert.Equal(t, 1, delivered)
		wstest.ReceiveEvent(t, conn, "accounts.2")
	})

	t.Run("topic without subscribers", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()))

		delivered, err := server.Publish(t.Context(), "nobody.home", "hello")
		require.NoError(t, err)
		assert.Equal(t, 0, delivered)
	})

	t.Run("unencodable payload", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()))

		_, err := server.Publish(t.Context(), "accounts.1", make(chan int))
		var unsupported *json.UnsupportedTypeError
		require.ErrorAs(t, err, &unsupported)
	})

	t.Run("closes slow consumers by default", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t,
			ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()),
			ws.ServerWithSendBuffer(1),
		)
		conn := wstest.DialAndAuthenticate(t, server)
		wstest.Subscribe(t, conn, "accounts.1")

		var undelivered bool
		for range 1000 {
			delivered, err := server.Publish(t.Context(), "accounts.1", "flood")
			require.NoError(t, err)
			if delivered == 0 {
				undelivered = true
				break
			}
		}
		assert.True(t, undelivered, "the slow consumer should eventually stop being counted")
		assert.Equal(t, websocket.StatusPolicyViolation, closeStatus(t, conn))
	})

	t.Run("drops for slow consumers when configured", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t,
			ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()),
			ws.ServerWithSendBuffer(1),
			ws.ServerWithOverflowPolicy(ws.OverflowDrop),
		)
		conn := wstest.DialAndAuthenticate(t, server)
		wstest.Subscribe(t, conn, "accounts.1")

		for i := range 1000 {
			_, err := server.Publish(t.Context(), "accounts.1", i)
			require.NoError(t, err)
		}

		queued := false
		for range 10 {
			delivered, err := server.Publish(t.Context(), "accounts.1", "after")
			require.NoError(t, err)
			if delivered == 1 {
				queued = true
				break
			}
			wstest.Receive(t, conn)
		}
		require.True(t, queued, "the connection should accept messages again after draining")

		for {
			msg := wstest.ReceiveEvent(t, conn, "accounts.1")
			if string(msg.Payload) == `"after"` {
				break
			}
		}
		assert.Equal(t, 1, server.SubscriberCount("accounts.1"))
	})

	t.Run("fails after shutdown and notifies the handler of disconnects", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)
		wstest.Subscribe(t, conn, "accounts.1")

		shutdown := make(chan error, 1)
		go func() { shutdown <- server.Shutdown(t.Context()) }()
		assert.Equal(t, websocket.StatusGoingAway, closeStatus(t, conn))
		require.NoError(t, <-shutdown)

		_, err := server.Publish(t.Context(), "accounts.1", nil)
		require.ErrorIs(t, err, ws.ErrServerClosed)

		assert.Equal(t, []wstest.Unsubscription{
			{ConnID: handler.ConnID(t), Topic: "accounts.1", Reason: ws.UnsubscribeOnDisconnect},
		}, handler.Unsubscribed())
	})
}

func TestSubscriptionsOnReauthentication(t *testing.T) {
	t.Parallel()

	subjectAuthenticator := func(ttl time.Duration) authx.Authenticator {
		return authx.AuthenticatorFunc(func(_ context.Context, token string) (authx.Identity, error) {
			return authx.Identity{Subject: token, ExpiresAt: time.Now().Add(ttl)}, nil
		})
	}

	t.Run("same subject keeps its subscriptions", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t,
			ws.ServerWithSubscriptionHandler(handler),
			ws.ServerWithAuthenticator(subjectAuthenticator(150*time.Millisecond)),
			ws.ServerWithRefreshTimeout(time.Second),
		)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)
		wstest.Authenticate(t, conn, "alice")
		wstest.Subscribe(t, conn, "accounts.alice")

		assert.Equal(t, ws.TypeAuthRefresh, wstest.Receive(t, conn).Type)
		wstest.Authenticate(t, conn, "alice")

		assert.Empty(t, handler.Unsubscribed())
		assert.Equal(t, 1, server.SubscriberCount("accounts.alice"))
	})

	t.Run("different subject loses its subscriptions", func(t *testing.T) {
		t.Parallel()

		handler := wstest.NewSubscriptionHandler()
		server := wstest.NewServer(t,
			ws.ServerWithSubscriptionHandler(handler),
			ws.ServerWithAuthenticator(subjectAuthenticator(150*time.Millisecond)),
			ws.ServerWithRefreshTimeout(time.Second),
		)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)
		wstest.Authenticate(t, conn, "alice")
		wstest.Subscribe(t, conn, "accounts.alice")

		assert.Equal(t, ws.TypeAuthRefresh, wstest.Receive(t, conn).Type)
		wstest.Authenticate(t, conn, "mallory")

		assert.Equal(t, []wstest.Unsubscription{
			{ConnID: handler.ConnID(t), Topic: "accounts.alice", Reason: ws.UnsubscribeByServer},
		}, handler.Unsubscribed())
		assert.Equal(t, 0, server.SubscriberCount("accounts.alice"))
	})
}

func TestSubscriptionOptions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  ws.ServerOpt
	}{
		{name: "zero max subscriptions", opt: ws.ServerWithMaxSubscriptions(0)},
		{name: "negative max subscriptions", opt: ws.ServerWithMaxSubscriptions(-1)},
		{name: "zero max topic length", opt: ws.ServerWithMaxTopicLength(0)},
		{name: "unknown overflow policy", opt: ws.ServerWithOverflowPolicy(ws.OverflowPolicy(99))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server, err := ws.NewServer(append(serverDefaults(t), tc.opt)...)
			require.ErrorIs(t, err, ws.ErrInvalidServerOption)
			assert.Nil(t, server)
		})
	}

}

func TestSubscriptionOptionsFromEnv(t *testing.T) {
	t.Setenv("WS_MAX_SUBSCRIPTIONS", "1")
	t.Setenv("WS_MAX_TOPIC_LENGTH", "4")

	server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()))
	conn := wstest.DialAndAuthenticate(t, server)

	res := wstest.RequestSubscribe(t, conn, "toolong")
	assert.Equal(t, "invalid topic", wstest.ErrorMessage(t, res))

	wstest.Subscribe(t, conn, "ok")
	res = wstest.RequestSubscribe(t, conn, "two")
	assert.Equal(t, "too many subscriptions", wstest.ErrorMessage(t, res))
}

func TestSubscriptionsConcurrency(t *testing.T) {
	t.Parallel()

	const (
		clients = 8
		rounds  = 20
		topics  = 3
	)

	server := wstest.NewServer(t,
		ws.ServerWithSubscriptionHandler(ws.AllowAllSubscriptions()),
		ws.ServerWithOverflowPolicy(ws.OverflowDrop),
	)

	stopPublishing := make(chan struct{})
	published := make(chan struct{})
	go func() {
		defer close(published)

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for i := 0; ; i++ {
			select {
			case <-stopPublishing:
				return
			case <-ticker.C:
			}

			topic := "topic." + strconv.Itoa(i%topics)
			if _, err := server.Publish(context.Background(), topic, i); err != nil {
				return
			}
			server.Unsubscribe("unknown", topic)
		}
	}()

	errs := make(chan error, clients)
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn := wstest.DialAndAuthenticate(t, server)
			for round := range rounds {
				topic := "topic." + strconv.Itoa((c+round)%topics)
				if err := requestAndAwait(t.Context(), conn, ws.TypeSubscribe, ws.TypeSubscribeOK, topic); err != nil {
					errs <- err
					return
				}
				if err := requestAndAwait(t.Context(), conn, ws.TypeUnsubscribe, ws.TypeUnsubscribeOK, topic); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		assert.NoError(t, err)
	}

	close(stopPublishing)
	<-published

	for i := range topics {
		assert.Equal(t, 0, server.SubscriberCount("topic."+strconv.Itoa(i)))
	}
}

func requestAndAwait(ctx context.Context, conn *websocket.Conn, reqType, resType, topic string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg, err := ws.NewMessage(topic, reqType, ws.SubscribePayload{Topic: topic})
	if err != nil {
		return err
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		return err
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}

		var res ws.Message
		if err := json.Unmarshal(data, &res); err != nil {
			return err
		}
		if res.Type == ws.TypeEvent {
			continue
		}
		if res.Type != resType || res.ID != topic {
			return fmt.Errorf("expected %s for %s, got %s (%s): %s", resType, topic, res.Type, res.ID, res.Payload)
		}
		return nil
	}
}

func TestSubscriptionsDefaultHandler(t *testing.T) {
	t.Parallel()

	t.Run("funcs adapter with nil functions allows everything", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(ws.SubscriptionHandlerFuncs{}))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Subscribe(t, conn, "anything")
		wstest.Unsubscribe(t, conn, "anything")
	})

	t.Run("funcs adapter forwards both hooks", func(t *testing.T) {
		t.Parallel()

		var (
			mu      sync.Mutex
			reasons []ws.UnsubscribeReason
		)
		handler := ws.SubscriptionHandlerFuncs{
			OnSubscribeFunc: func(_ context.Context, _ *ws.Conn, topic string) error {
				if topic == "denied" {
					return ws.ErrSubscriptionForbidden
				}
				return nil
			},
			OnUnsubscribeFunc: func(_ context.Context, _ *ws.Conn, _ string, reason ws.UnsubscribeReason) {
				mu.Lock()
				defer mu.Unlock()
				reasons = append(reasons, reason)
			},
		}
		server := wstest.NewServer(t, ws.ServerWithSubscriptionHandler(handler))
		conn := wstest.DialAndAuthenticate(t, server)

		assert.Equal(t, "forbidden", wstest.ErrorMessage(t, wstest.RequestSubscribe(t, conn, "denied")))
		wstest.Subscribe(t, conn, "allowed")
		wstest.Unsubscribe(t, conn, "allowed")

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, []ws.UnsubscribeReason{ws.UnsubscribeByClient}, reasons)
	})
}
