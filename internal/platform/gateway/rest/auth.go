package rest

import (
	"context"
	"net/http"
)

// --------------------------------------------------------------- Contract

type HTTPAuthenticator interface {
	Authenticate(req *http.Request) (context.Context, error)
}
