package rpctest

import (
	"net"
	"testing"

	"github.com/sergisimo/ledger/internal/platform/gateway/rpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewServerConn(t *testing.T, opts ...rpc.ServerOpt) *grpc.ClientConn {
	t.Helper()

	server, err := rpc.NewServer(opts...)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}
