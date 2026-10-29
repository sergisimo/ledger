package ws_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/authx/authxtest"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws/wstest"
)

func TestConnAuthentication(t *testing.T) {
	t.Parallel()

	t.Run("accepts a valid token and reports the identity", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)

		res := wstest.Authenticate(t, conn, authxtest.AnyToken)
		assert.Equal(t, "auth-1", res.ID, "the reply should echo the request id")

		var payload ws.AuthOKPayload
		require.NoError(t, res.Bind(&payload))
		assert.Equal(t, "test-subject", payload.Subject)
		assert.Nil(t, payload.ExpiresAt, "a token without ttl should never expire")
	})

	t.Run("rejects an invalid token and closes", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)

		msg, err := ws.NewMessage("auth-1", ws.TypeAuth, ws.AuthPayload{Token: "nope"})
		require.NoError(t, err)
		wstest.Send(t, conn, msg)

		res := wstest.Receive(t, conn)
		assert.Equal(t, ws.TypeError, res.Type)

		assert.Equal(t, websocket.StatusPolicyViolation, closeStatus(t, conn))
	})

	t.Run("closes when no token arrives in time", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithAuthTimeout(100*time.Millisecond))
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)

		assert.Equal(t, websocket.StatusPolicyViolation, closeStatus(t, conn))
	})

	t.Run("refuses messages until authenticated", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)

		wstest.Send(t, conn, ws.Message{Type: "custom", ID: "1"})

		res := wstest.Receive(t, conn)
		assert.Equal(t, ws.TypeError, res.Type)
		assert.Equal(t, "1", res.ID)

		var payload ws.ErrorPayload
		require.NoError(t, res.Bind(&payload))
		assert.Equal(t, "not authenticated", payload.Message)
	})

	t.Run("rejects a malformed message", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)

		require.NoError(t, conn.Write(t.Context(), websocket.MessageText, []byte("{not json")))

		res := wstest.Receive(t, conn)
		assert.Equal(t, ws.TypeError, res.Type)
	})

	t.Run("rejects a binary message", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)

		require.NoError(t, conn.Write(t.Context(), websocket.MessageBinary, []byte("payload")))

		res := wstest.Receive(t, conn)
		assert.Equal(t, ws.TypeError, res.Type)
	})
}

func TestConnTokenRefresh(t *testing.T) {
	t.Parallel()

	t.Run("asks for a new token when the current one expires", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t,
			ws.ServerWithAuthenticator(authxtest.NewAuthenticator(150*time.Millisecond)),
			ws.ServerWithRefreshTimeout(time.Second),
		)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)
		wstest.Authenticate(t, conn, authxtest.AnyToken)

		assert.Equal(t, ws.TypeAuthRefresh, wstest.Receive(t, conn).Type)

		wstest.Authenticate(t, conn, authxtest.AnyToken)
		wstest.Send(t, conn, ws.Message{Type: "custom", ID: "after-refresh"})
		assert.Equal(t, ws.TypeAuthRefresh, wstest.Receive(t, conn).Type)
	})

	t.Run("closes when the new token does not arrive in time", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t,
			ws.ServerWithAuthenticator(authxtest.NewAuthenticator(100*time.Millisecond)),
			ws.ServerWithRefreshTimeout(100*time.Millisecond),
		)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)
		wstest.Authenticate(t, conn, authxtest.AnyToken)

		assert.Equal(t, ws.TypeAuthRefresh, wstest.Receive(t, conn).Type)
		assert.Equal(t, websocket.StatusPolicyViolation, closeStatus(t, conn))
	})

	t.Run("refuses messages while the token is expired", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t,
			ws.ServerWithAuthenticator(authxtest.NewAuthenticator(100*time.Millisecond)),
			ws.ServerWithRefreshTimeout(time.Second),
		)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)
		wstest.Authenticate(t, conn, authxtest.AnyToken)

		assert.Equal(t, ws.TypeAuthRefresh, wstest.Receive(t, conn).Type)

		wstest.Send(t, conn, ws.Message{Type: "custom", ID: "while-expired"})
		res := wstest.Receive(t, conn)
		assert.Equal(t, ws.TypeError, res.Type)
		assert.Equal(t, "while-expired", res.ID)
	})
}

func TestConnHandler(t *testing.T) {
	t.Parallel()

	t.Run("delivers authenticated messages to the handler", func(t *testing.T) {
		t.Parallel()

		received := make(chan ws.Message, 1)
		server := wstest.NewServer(t, ws.ServerWithHandler(
			ws.HandlerFunc(func(_ context.Context, conn *ws.Conn, msg ws.Message) error {
				received <- msg
				return conn.SendPayload(msg.ID, "echo", map[string]string{"subject": conn.Identity().Subject})
			}),
		))
		conn := wstest.DialAndAuthenticate(t, server)

		sent, err := ws.NewMessage("req-1", "custom", map[string]string{"hello": "world"})
		require.NoError(t, err)
		wstest.Send(t, conn, sent)

		select {
		case got := <-received:
			assert.Equal(t, "custom", got.Type)
			assert.JSONEq(t, `{"hello":"world"}`, string(got.Payload))
		case <-time.After(time.Second):
			t.Fatal("the handler never received the message")
		}

		res := wstest.Receive(t, conn)
		assert.Equal(t, "echo", res.Type)
		assert.Equal(t, "req-1", res.ID)
		assert.JSONEq(t, `{"subject":"test-subject"}`, string(res.Payload))
	})

	t.Run("reports a handler error back to the client", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithHandler(
			ws.HandlerFunc(func(context.Context, *ws.Conn, ws.Message) error {
				return assert.AnError
			}),
		))
		conn := wstest.DialAndAuthenticate(t, server)

		wstest.Send(t, conn, ws.Message{Type: "custom", ID: "boom"})

		res := wstest.Receive(t, conn)
		assert.Equal(t, ws.TypeError, res.Type)
		assert.Equal(t, "boom", res.ID)
	})
}

func TestConnPeriodicPush(t *testing.T) {
	t.Parallel()

	const (
		interval = 100 * time.Millisecond
		want     = 5
	)

	var (
		pushers sync.WaitGroup
		failed  = make(chan error, 1)
	)

	server := wstest.NewServer(t, ws.ServerWithHandler(
		ws.HandlerFunc(func(_ context.Context, conn *ws.Conn, _ ws.Message) error {
			pushers.Add(1)
			go func() {
				defer pushers.Done()

				ticker := time.NewTicker(interval)
				defer ticker.Stop()

				for tick := 1; ; tick++ {
					select {
					case <-conn.Context().Done():
						return
					case <-ticker.C:
						if err := conn.SendPayload("", "tick", tick); err != nil {
							select {
							case failed <- err:
							default:
							}
							return
						}
					}
				}
			}()

			return nil
		}),
	))
	t.Cleanup(pushers.Wait)

	conn := wstest.DialAndAuthenticate(t, server)
	wstest.Send(t, conn, ws.Message{Type: "start"})

	start := time.Now()
	for tick := 1; tick <= want; tick++ {
		msg := wstest.Receive(t, conn)
		require.Equal(t, "tick", msg.Type)

		var got int
		require.NoError(t, json.Unmarshal(msg.Payload, &got))
		assert.Equal(t, tick, got, "ticks should arrive in order")
	}

	assert.GreaterOrEqual(t, time.Since(start), time.Duration(want-1)*interval,
		"pushes should be paced by the ticker, not delivered in a burst")

	select {
	case err := <-failed:
		t.Fatalf("pushing to the client failed: %v", err)
	default:
	}
}

func TestConnSend(t *testing.T) {
	t.Parallel()

	t.Run("reports a full send buffer instead of blocking", func(t *testing.T) {
		t.Parallel()

		var (
			sendErr error
			once    sync.Once
			done    = make(chan struct{})
		)

		server := wstest.NewServer(t,
			ws.ServerWithSendBuffer(1),
			ws.ServerWithHandler(ws.HandlerFunc(func(_ context.Context, conn *ws.Conn, _ ws.Message) error {
				once.Do(func() {
					defer close(done)
					for range 1000 {
						if err := conn.SendPayload("", "flood", strconv.Itoa(1)); err != nil {
							sendErr = err
							return
						}
					}
				})
				return nil
			})),
		)

		conn := wstest.DialAndAuthenticate(t, server)
		wstest.Send(t, conn, ws.Message{Type: "flood-me"})

		select {
		case <-done:
			require.ErrorIs(t, sendErr, ws.ErrSendBufferFull)
		case <-time.After(5 * time.Second):
			t.Fatal("Send blocked instead of reporting a full buffer")
		}
	})
}

func closeStatus(t *testing.T, conn *websocket.Conn) websocket.StatusCode {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return websocket.CloseStatus(err)
		}
	}
}
