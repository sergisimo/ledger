package metrics_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxtest"

	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/metrics"
)

func TestModule(t *testing.T) {
	t.Setenv("SERVICE_NAME", "test-service")
	debugHost := "localhost:6060"

	app := fxtest.New(
		t,
		logging.Module(),
		metrics.Module(debugHost),
	)

	err := app.Start(t.Context())
	require.NoError(t, err, "failed to start module")

	time.Sleep(100 * time.Millisecond)

	endpoints := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
		"/debug/pprof/trace",
		"/debug/vars/",
		"/debug/statsviz/",
	}

	for _, endpoint := range endpoints {
		url := fmt.Sprintf("http://%s%s", debugHost, endpoint)
		resp, err := http.Get(url)
		require.NoError(t, err, fmt.Sprintf("failed to GET %s", endpoint))
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, fmt.Sprintf("endpoint %s should return 200", endpoint))
	}

	err = app.Stop(t.Context())
	require.NoError(t, err, "failed to stop module")
}
