package ws_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/authx/authxtest"
	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/gateway/gatewaytest"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws/wstest"
)

func TestNewServer(t *testing.T) {
	t.Parallel()

	t.Run("logger is mandatory", func(t *testing.T) {
		t.Parallel()

		server, err := ws.NewServer()
		require.ErrorIs(t, err, fields.NewErrNil(fields.NameLogger))
		assert.Nil(t, server)
	})

	t.Run("authenticator is mandatory", func(t *testing.T) {
		t.Parallel()

		server, err := ws.NewServer(ws.ServerWithLogger(wstest.NewLogger(t)))
		require.ErrorIs(t, err, fields.NewErrNil(fields.NameAuthenticator))
		assert.Nil(t, server)
	})

	t.Run("invalid origin pattern", func(t *testing.T) {
		t.Parallel()

		server, err := ws.NewServer(append(serverDefaults(t), ws.ServerWithOrigins("valid.example", "[invalid"))...)
		require.ErrorIs(t, err, ws.ErrInvalidOriginPattern)
		assert.Nil(t, server)
	})

	t.Run("upgrades on the default path", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)
		wstest.Ping(t, conn)
	})

	t.Run("upgrades on a custom path only", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithPath("/custom"))

		_, resp, err := websocket.Dial(t.Context(), server.URL+ws.DefaultPath, nil)
		require.Error(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)

		conn := wstest.Dial(t, server.URL+"/custom", nil)
		wstest.Ping(t, conn)
	})

	t.Run("rejects cross origin requests by default", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		_, resp, err := websocket.Dial(t.Context(), server.URL+ws.DefaultPath, &websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{"http://other.example"}},
		})
		require.Error(t, err)
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})

	t.Run("allows configured origins", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t, ws.ServerWithOrigins("*.example"))
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, &websocket.DialOptions{
			HTTPHeader: http.Header{"Origin": []string{"http://other.example"}},
		})
		wstest.Ping(t, conn)
	})

	t.Run("applies middlewares in order before upgrading", func(t *testing.T) {
		t.Parallel()

		tag := func(value string) ws.Middleware {
			return func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Add("X-Order", value)
					next.ServeHTTP(w, r)
				})
			}
		}
		server := wstest.NewServer(t, ws.ServerWithMiddlewares(tag("first"), tag("second")))

		_, resp, err := websocket.Dial(t.Context(), server.URL+ws.DefaultPath, nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"first", "second"}, resp.Header.Values("X-Order"))
	})

	t.Run("middlewares can refuse the upgrade", func(t *testing.T) {
		t.Parallel()

		deny := func(http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "nope", http.StatusUnauthorized)
			})
		}
		server := wstest.NewServer(t, ws.ServerWithMiddlewares(deny))

		_, resp, err := websocket.Dial(t.Context(), server.URL+ws.DefaultPath, nil)
		require.Error(t, err)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("shutdown closes clients with going away and refuses new ones", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		conn := wstest.Dial(t, server.URL+ws.DefaultPath, nil)

		shutdown := make(chan error, 1)
		go func() { shutdown <- server.Shutdown(t.Context()) }()

		_, _, err := conn.Read(t.Context())
		assert.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err))

		require.NoError(t, <-shutdown)

		_, _, err = websocket.Dial(t.Context(), server.URL+ws.DefaultPath, nil)
		require.Error(t, err)
	})

	t.Run("shutdown gives up on unresponsive clients when the context expires", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		// Authenticating proves the server registered the connection before we shut down. The
		// client stops reading afterwards, so it never answers the close handshake.
		wstest.DialAndAuthenticate(t, server)

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		err := server.Shutdown(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("shutdown is idempotent", func(t *testing.T) {
		t.Parallel()

		server := wstest.NewServer(t)
		require.NoError(t, server.Shutdown(t.Context()))
		require.NoError(t, server.Shutdown(t.Context()))
		require.NoError(t, server.Close())
	})

	t.Run("refuses upgrades while shutting down", func(t *testing.T) {
		t.Parallel()

		server, err := ws.NewServer(serverDefaults(t)...)
		require.NoError(t, err)
		require.NoError(t, server.Shutdown(t.Context()))

		httpServer := httptest.NewServer(server.Handler())
		t.Cleanup(httpServer.Close)

		url := "ws" + strings.TrimPrefix(httpServer.URL, "http") + ws.DefaultPath
		_, resp, err := websocket.Dial(t.Context(), url, nil)
		require.Error(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	})

	t.Run("tls", func(t *testing.T) {
		t.Parallel()

		cert, certPool := gatewaytest.NewSelfSignedCert(t)
		server, err := ws.NewServer(append(serverDefaults(t), ws.ServerWithTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}))...)
		require.NoError(t, err)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(func() { _ = server.Close() })

		conn := wstest.Dial(t, "wss://"+listener.Addr().String()+ws.DefaultPath, &websocket.DialOptions{
			HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: certPool}}},
		})
		wstest.Ping(t, conn)
	})

	t.Run("listen and serve on the configured address", func(t *testing.T) {
		t.Parallel()

		server, err := ws.NewServer(append(serverDefaults(t), ws.ServerWithAddress("127.0.0.1:0"))...)
		require.NoError(t, err)

		served := make(chan error, 1)
		go func() { served <- server.ListenAndServe() }()

		time.Sleep(100 * time.Millisecond)
		require.NoError(t, server.Shutdown(t.Context()))
		require.ErrorIs(t, <-served, http.ErrServerClosed)
	})

	t.Run("listen and serve on an invalid address", func(t *testing.T) {
		t.Parallel()

		server, err := ws.NewServer(append(serverDefaults(t), ws.ServerWithAddress("invalid:address:99999"))...)
		require.NoError(t, err)
		assert.Error(t, server.ListenAndServe())
	})

	t.Run("serve returns server closed after close", func(t *testing.T) {
		t.Parallel()

		server, err := ws.NewServer(serverDefaults(t)...)
		require.NoError(t, err)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		served := make(chan error, 1)
		go func() { served <- server.Serve(listener) }()

		conn := wstest.Dial(t, "ws://"+listener.Addr().String()+ws.DefaultPath, nil)
		require.NoError(t, server.Close())

		_, _, err = conn.Read(t.Context())
		require.Error(t, err)
		assert.False(t, errors.Is(err, context.DeadlineExceeded))
		require.ErrorIs(t, <-served, http.ErrServerClosed)
	})
}

// serverDefaults supplies the two mandatory options, so each test only states what it exercises.
func serverDefaults(t *testing.T) []ws.ServerOpt {
	t.Helper()

	return []ws.ServerOpt{
		ws.ServerWithLogger(wstest.NewLogger(t)),
		ws.ServerWithAuthenticator(authxtest.NewAuthenticator(0)),
	}
}
