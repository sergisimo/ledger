package rpctest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func GetHandlerResponseFileDir(handlerName string) string {
	return filepath.Join(getHandlerFileDir(handlerName), "response")
}

func GetHandlerRequestFileDir(handlerName string) string {
	return filepath.Join(getHandlerFileDir(handlerName), "request")
}

func getHandlerFileDir(handlerName string) string {
	return filepath.Join("testdata", "rpchandler", handlerName)
}

func RequestFromFile[Req proto.Message](t *testing.T, fileDir, fileName string, req Req) Req {
	t.Helper()

	bs, err := os.ReadFile(filepath.Join(fileDir, fileName+".json"))
	require.NoError(t, err)
	require.NoError(t, protojson.Unmarshal(bs, req))

	return req
}
