package rest

import (
	"net/http"
)

// --------------------------------------------------------------- Contract

type (
	Endpoint struct {
		http.Handler
		Method string
		Path   string
	}
)

const (
	IDPath = "/{id}"
)

// --------------------------------------------------------------- Constructors

func NewGetEndpoint(handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodGet, IDPath, handler)
}

func NewListEndpoint(handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodGet, "", handler)
}

func NewCreateEndpoint(handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodPost, "", handler)
}

func NewPatchEndpoint(handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodPatch, IDPath, handler)
}

func NewDeleteEndpoint(handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodDelete, IDPath, handler)
}

func newEndpoint(method, path string, handler http.Handler) *Endpoint {
	return &Endpoint{
		Handler: handler,
		Method:  method,
		Path:    path,
	}
}
