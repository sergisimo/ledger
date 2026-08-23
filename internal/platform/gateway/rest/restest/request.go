package restest

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	RequestOption func(req *http.Request)
)

func NewGetRequest(t *testing.T, resType resource.Type, resID string, opts ...RequestOption) *http.Request {
	t.Helper()

	target := fmt.Sprintf("/%s/%s", resType, resID)
	return newRequest(t, target, http.MethodGet, opts...)
}

func NewListRequest(t *testing.T, resType resource.Type, opts ...RequestOption) *http.Request {
	t.Helper()

	target := fmt.Sprintf("/%s", resType)
	return newRequest(t, target, http.MethodGet, opts...)
}

func NewCreateRequest(t *testing.T, resType resource.Type, opts ...RequestOption) *http.Request {
	t.Helper()

	target := fmt.Sprintf("/%s", resType)
	return newRequest(t, target, http.MethodPost, opts...)
}

func NewPatchRequest(t *testing.T, resType resource.Type, resID string, opts ...RequestOption) *http.Request {
	t.Helper()

	target := fmt.Sprintf("/%s/%s", resType, resID)
	return newRequest(t, target, http.MethodPatch, opts...)
}

func NewDeleteRequest(t *testing.T, resType resource.Type, resID string, opts ...RequestOption) *http.Request {
	t.Helper()

	target := fmt.Sprintf("/%s/%s", resType, resID)
	return newRequest(t, target, http.MethodDelete, opts...)
}

func GetHandlerResponseFileDir(handlerName string) string {
	return filepath.Join(getHandlerFileDir(handlerName), "response")
}

func GetHandlerRequestFileDir(handlerName string) string {
	return filepath.Join(getHandlerFileDir(handlerName), "request")
}

func getHandlerFileDir(handlerName string) string {
	return filepath.Join("testdata", "handler", handlerName)
}

func RequestWithBodyFromFile(t *testing.T, fileFolder, fileName string) RequestOption {
	t.Helper()

	return func(req *http.Request) {

		f, err := os.Open(filepath.Join(fileFolder, fileName))
		assert.NoError(t, err)

		req.Body = f
	}
}

func RequestWithHeader(key, value string) RequestOption {
	return func(req *http.Request) {
		req.Header.Set(key, value)
	}
}

func RequestWithQueryParam(key, value string) RequestOption {
	return func(req *http.Request) {
		q := req.URL.Query()
		q.Add(key, value)
		req.URL.RawQuery = q.Encode()
	}
}

func newRequest(t *testing.T, path, method string, opts ...RequestOption) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, path, http.NoBody)
	require.NoError(t, err)
	require.NotNil(t, req)

	for _, opt := range opts {
		opt(req)
	}

	return req
}
