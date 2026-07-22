package rest

import (
	"fmt"
	"net/http"
)

// --------------------------------------------------------------- Contract

type (
	Endpoint struct {
		http.Handler
		method string
		path   string
	}
)

// --------------------------------------------------------------- Constructors

func NewGetEndpoint(resPath string, handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodGet, fmt.Sprintf("%s/{id}", resPath), handler)
}

func NewListEndpoint(resPath string, handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodGet, resPath, handler)
}

func NewCreateEndpoint(resPath string, handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodPost, resPath, handler)
}

func NewPatchEndpoint(resPath string, handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodPatch, fmt.Sprintf("%s/{id}", resPath), handler)
}

func NewDeleteEndpoint(resPath string, handler http.Handler) *Endpoint {
	return newEndpoint(http.MethodDelete, fmt.Sprintf("%s/{id}", resPath), handler)
}

func newEndpoint(method, path string, handler http.Handler) *Endpoint {
	return &Endpoint{
		Handler: handler,
		method:  method,
		path:    path,
	}
}
