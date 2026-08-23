package restest

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/sergisimo/ledger/internal/platform/gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	ResponseAssertion func(t *testing.T, res *http.Response)

	HandlerTest struct {
		name       string
		handler    http.Handler
		req        *http.Request
		assertions []ResponseAssertion
	}
)

func NewHandlerTest(
	name string,
	handler http.Handler,
	req *http.Request,
	assertions ...ResponseAssertion,
) *HandlerTest {
	return &HandlerTest{
		name:       name,
		handler:    handler,
		req:        req,
		assertions: assertions,
	}
}

type handlerSuite struct {
	tests []*HandlerTest
}

func NewHandlerSuite(tests ...*HandlerTest) *handlerSuite {
	return &handlerSuite{tests}
}

func (hts *handlerSuite) Exec(t *testing.T) {
	t.Helper()

	for _, test := range hts.tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				t.Helper()
				executeTest(t, test)
				synctest.Wait()
			})
		})
	}
}

func executeTest(t *testing.T, test *HandlerTest) {
	t.Helper()

	rr := httptest.NewRecorder()
	test.handler.ServeHTTP(rr, test.req)
	res := rr.Result()
	defer res.Body.Close()

	for _, assertion := range test.assertions {
		assertion(t, res)
	}
}

func AssertResponseStatus(status int) ResponseAssertion {
	return func(t *testing.T, res *http.Response) {
		t.Helper()

		assert.Equal(t, status, res.StatusCode)
	}
}

func AssertCreateResponseOK() ResponseAssertion {
	return AssertResponseStatus(http.StatusCreated)
}

func AssertListResponseOK() ResponseAssertion {
	return AssertResponseStatus(http.StatusOK)
}

func AssertGetResponseOK() ResponseAssertion {
	return AssertResponseStatus(http.StatusOK)
}

func AssertPatchResponseOK() ResponseAssertion {
	return AssertResponseStatus(http.StatusOK)
}

func AssertDeleteResponseNoContent() ResponseAssertion {
	return AssertResponseStatus(http.StatusNoContent)
}

func AssertResMatchingFile(fileDir, fileName string, updateGoldenFile bool) ResponseAssertion {
	return func(t *testing.T, res *http.Response) {
		t.Helper()

		defer res.Body.Close()
		resDump, err := httputil.DumpResponse(res, true)
		require.NoError(t, err)

		filePath := filepath.Join(fileDir, fileName)
		f := getGoldenFile(t, filePath, os.O_RDWR, 0o644)
		if updateGoldenFile {
			t.Logf("updating golden file: %s", filePath)
			err := f.Truncate(0)
			require.NoError(t, err)
			_, err = f.Seek(0, io.SeekStart)
			require.NoError(t, err)
			_, err = f.Write(resDump)
			require.NoError(t, err)
			t.Logf("golden file: %s updated", filePath)
			return
		}

		goldenBs, err := io.ReadAll(f)
		require.NoError(t, err)
		assert.Equal(t, goldenBs, resDump)
	}
}

func getGoldenFile(t *testing.T, filePath string, flg int, mode fs.FileMode) *os.File {
	t.Helper()

	goldenPath := filePath
	if filepath.Ext(filePath) != ".golden" {
		goldenPath = filePath + ".golden"
	}

	f, err := os.OpenFile(goldenPath, flg, mode)
	require.NoError(t, err)
	require.NotNil(t, f)

	t.Cleanup(
		func() {
			assert.NoError(t, f.Close())
		},
	)

	return f
}

func NewEndpointsHandler(basePath string, endpoints ...*rest.Endpoint) http.Handler {
	mux := http.NewServeMux()
	for _, end := range endpoints {
		mux.Handle(
			fmt.Sprintf("%s %s", end.Method, path.Join(basePath, end.Path)),
			end.Handler,
		)
	}

	return mux
}
