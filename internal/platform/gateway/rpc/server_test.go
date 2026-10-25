package rpc_test

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/sergisimo/ledger/internal/platform/gateway/gatewaytest"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc/rpcpb"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc/rpctest"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc/rpctest/pb"
	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// --------------------------------------------------------------- Stub

type rawErrCtrl struct {
	pb.UnimplementedTestServiceServer
}

func (c *rawErrCtrl) Get(context.Context, *rpcpb.IDQuery) (*pb.Test, error) {
	return nil, resource.NewErrorNotFound("tests", "raw")
}

func (c *rawErrCtrl) Register(registrar grpc.ServiceRegistrar) {
	pb.RegisterTestServiceServer(registrar, c)
}

// --------------------------------------------------------------- Test

func TestNewServer(t *testing.T) {
	t.Parallel()

	t.Run("health enabled by default", func(t *testing.T) {
		t.Parallel()

		conn := rpctest.NewServerConn(t)
		res, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, res.GetStatus())
	})

	t.Run("health disabled", func(t *testing.T) {
		t.Parallel()

		conn := rpctest.NewServerConn(t, rpc.ServerWithHealth(false))
		_, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
		assert.Equal(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("reflection enabled", func(t *testing.T) {
		t.Parallel()

		conn := rpctest.NewServerConn(t, rpc.ServerWithReflection(true))
		res, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, res.GetStatus())
	})

	t.Run("interceptor maps raw domain errors", func(t *testing.T) {
		t.Parallel()

		conn := rpctest.NewServerConn(t, rpc.ServerWithControllers(&rawErrCtrl{}))
		_, err := pb.NewTestServiceClient(conn).Get(t.Context(), &rpcpb.IDQuery{Id: "raw"})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("raw grpc server options", func(t *testing.T) {
		t.Parallel()

		conn := rpctest.NewServerConn(t,
			rpc.ServerWithControllers(&rawErrCtrl{}),
			rpc.ServerWithOptions(grpc.MaxRecvMsgSize(1)),
		)
		_, err := pb.NewTestServiceClient(conn).Get(t.Context(), &rpcpb.IDQuery{Id: "raw"})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("tls", func(t *testing.T) {
		t.Parallel()

		cert, certPool := gatewaytest.NewSelfSignedCert(t)
		server, err := rpc.NewServer(rpc.ServerWithTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}))
		require.NoError(t, err)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)

		conn, err := grpc.NewClient(
			listener.Addr().String(),
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(certPool, "localhost")),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })

		res, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, res.GetStatus())
	})

	t.Run("listen and serve on the configured address", func(t *testing.T) {
		t.Parallel()

		server, err := rpc.NewServer(rpc.ServerWithAddress("127.0.0.1:0"))
		require.NoError(t, err)

		served := make(chan error, 1)
		go func() { served <- server.ListenAndServe() }()

		time.Sleep(100 * time.Millisecond)
		server.Stop()
		require.NoError(t, <-served)
	})

	t.Run("listen and serve on an invalid address", func(t *testing.T) {
		t.Parallel()

		server, err := rpc.NewServer(rpc.ServerWithAddress("invalid:address:99999"))
		require.NoError(t, err)
		assert.Error(t, server.ListenAndServe())
	})
}
