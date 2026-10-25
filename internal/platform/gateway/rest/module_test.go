package rest_test

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/sergisimo/ledger/internal/platform/gateway/gatewaytest"
	"github.com/sergisimo/ledger/internal/platform/gateway/rest"
)

func TestModule(t *testing.T) {
	t.Run("starts and stops", func(t *testing.T) {
		t.Setenv("REST_ADDRESS", "127.0.0.1:0")

		app := fxtest.New(t, rest.Module(time.Second), fx.NopLogger)

		app.RequireStart()
		app.RequireStop()
	})

	t.Run("shuts the app down when it cannot listen", func(t *testing.T) {
		t.Setenv("REST_ADDRESS", "invalid:address:99999")

		app := fx.New(rest.Module(time.Second), fx.NopLogger)
		require.NoError(t, app.Start(t.Context()))
		t.Cleanup(func() { _ = app.Stop(t.Context()) })

		assert.Equal(t, 1, (<-app.Wait()).ExitCode)
	})
}

func TestListenAndServe(t *testing.T) {
	t.Parallel()

	t.Run("serves TLS when configured", func(t *testing.T) {
		t.Parallel()

		cert, certPool := gatewaytest.NewSelfSignedCert(t)
		url := serveOnLoopback(t, rest.ServerWithTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}))

		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: certPool}}}
		assertNotFound(t, client, url+"/missing")
	})

	t.Run("serves plaintext without TLS", func(t *testing.T) {
		t.Parallel()

		assertNotFound(t, http.DefaultClient, serveOnLoopback(t)+"/missing")
	})
}

func serveOnLoopback(t *testing.T, opts ...rest.ServerOpt) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	server, err := rest.NewServer(append([]rest.ServerOpt{rest.ServerWithAddress(addr)}, opts...)...)
	require.NoError(t, err)

	scheme := "http://"
	if server.TLSConfig != nil {
		scheme = "https://"
	}

	go func() { _ = rest.ListenAndServe(server) }()
	t.Cleanup(func() { _ = server.Close() })

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		return conn.Close() == nil
	}, time.Second, 10*time.Millisecond)

	return scheme + addr
}

func assertNotFound(t *testing.T, client *http.Client, url string) {
	t.Helper()

	res, err := client.Get(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = res.Body.Close() })

	assert.Equal(t, http.StatusNotFound, res.StatusCode)
}
