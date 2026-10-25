package wstest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/authx/authxtest"
	"github.com/sergisimo/ledger/internal/platform/gateway/ws"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

type Server struct {
	*ws.Server

	URL string
}

func NewServer(t *testing.T, opts ...ws.ServerOpt) *Server {
	t.Helper()

	defaults := []ws.ServerOpt{
		ws.ServerWithLogger(NewLogger(t)),
		ws.ServerWithAuthenticator(authxtest.NewAuthenticator(0)),
	}

	server, err := ws.NewServer(append(defaults, opts...)...)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	return &Server{Server: server, URL: "ws://" + listener.Addr().String()}
}

func NewLogger(t *testing.T) logging.Logger {
	t.Helper()

	logger, err := logging.NewLogger(
		logging.WithServiceName("wstest"),
		logging.WithHandler(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NoError(t, err)

	return logger
}

func Dial(t *testing.T, url string, opts *websocket.DialOptions) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.Dial(t.Context(), url, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })

	return conn
}

func DialAndAuthenticate(t *testing.T, server *Server) *websocket.Conn {
	t.Helper()

	conn := Dial(t, server.URL+ws.DefaultPath, nil)
	Authenticate(t, conn, authxtest.AnyToken)

	return conn
}

func Send(t *testing.T, conn *websocket.Conn, msg ws.Message) {
	t.Helper()

	data, err := json.Marshal(msg)
	require.NoError(t, err)
	require.NoError(t, conn.Write(t.Context(), websocket.MessageText, data))
}

func Receive(t *testing.T, conn *websocket.Conn) ws.Message {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	msgType, data, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, msgType)

	var msg ws.Message
	require.NoError(t, json.Unmarshal(data, &msg))

	return msg
}

func Authenticate(t *testing.T, conn *websocket.Conn, token string) ws.Message {
	t.Helper()

	msg, err := ws.NewMessage("auth-1", ws.TypeAuth, ws.AuthPayload{Token: token})
	require.NoError(t, err)
	Send(t, conn, msg)

	res := Receive(t, conn)
	require.Equal(t, ws.TypeAuthOK, res.Type, "expected the token to be accepted")

	return res
}

func Ping(t *testing.T, conn *websocket.Conn) {
	t.Helper()

	conn.CloseRead(t.Context())
	require.NoError(t, conn.Ping(t.Context()))
}
