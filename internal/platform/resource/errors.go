package resource

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	ErrNotFound = errors.New("not found")
)

type errorNotFound struct {
	kind  Type
	given string
	code  string
}

func NewErrorNotFound(kind Type, given string) *errorNotFound {
	return &errorNotFound{
		kind:  kind,
		given: given,
	}
}

func (e *errorNotFound) Unwrap() error {
	return ErrNotFound
}

func (e *errorNotFound) Error() string {
	return fmt.Sprintf("resource of type '%s' not found given: '%s'", e.kind, e.given)
}

func (e *errorNotFound) Code() string {
	return e.code
}

func (e *errorNotFound) HTTPStatus() int {
	return http.StatusNotFound
}
