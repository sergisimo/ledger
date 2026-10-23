package rpctest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	ResponseAssertion func(t *testing.T, res proto.Message, err error)

	HandlerTest struct {
		name       string
		invoke     func(ctx context.Context) (proto.Message, error)
		assertions []ResponseAssertion
	}
)

func NewHandlerTest(
	name string,
	invoke func(ctx context.Context) (proto.Message, error),
	assertions ...ResponseAssertion,
) *HandlerTest {
	return &HandlerTest{
		name:       name,
		invoke:     invoke,
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
			res, err := test.invoke(t.Context())
			for _, assertion := range test.assertions {
				assertion(t, res, err)
			}
		})
	}
}

func AssertResponseCode(code codes.Code) ResponseAssertion {
	return func(t *testing.T, _ proto.Message, err error) {
		t.Helper()

		assert.Equal(t, code, status.Code(err))
	}
}

func AssertResponseOK() ResponseAssertion {
	return AssertResponseCode(codes.OK)
}

func AssertResMatchingFile(fileDir, fileName string, updateGoldenFile bool) ResponseAssertion {
	return func(t *testing.T, res proto.Message, err error) {
		t.Helper()

		resDump := dumpResponse(t, res, err)

		filePath := filepath.Join(fileDir, fileName+".golden")
		if updateGoldenFile {
			t.Logf("updating golden file: %s", filePath)
			require.NoError(t, os.MkdirAll(fileDir, 0o755))
			require.NoError(t, os.WriteFile(filePath, resDump, 0o644))
			t.Logf("golden file: %s updated", filePath)
			return
		}

		goldenBs, err := os.ReadFile(filePath)
		require.NoError(t, err)
		assert.Equal(t, string(goldenBs), string(resDump))
	}
}

func dumpResponse(t *testing.T, res proto.Message, err error) []byte {
	t.Helper()

	st, ok := status.FromError(err)
	require.True(t, ok, "error is not a gRPC status: %v", err)

	msg := res
	if err != nil {
		msg = st.Proto()
	}

	jsonBs, mErr := protojson.Marshal(msg)
	require.NoError(t, mErr)

	var dump bytes.Buffer
	fmt.Fprintf(&dump, "gRPC %d %s\n\n", st.Code(), st.Code())
	require.NoError(t, json.Indent(&dump, jsonBs, "", "    "))
	dump.WriteByte('\n')

	return dump.Bytes()
}
