package rest

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/sergisimo/ledger/internal/platform/utils/sliceutils"
)

// --------------------------------------------------------------- Contract

type (
	ErrorEncoder func(w http.ResponseWriter, statusCode int, err error)

	HttpStatusCoder interface {
		error
		HTTPStatus() int
	}

	CodeError interface {
		error
		Code() string
	}
)

// --------------------------------------------------------------- Implementation

type (
	errorsResponse struct {
		Errors []errorResponse `json:"errors"`
	}

	errorResponse struct {
		Status string `json:"status"`
		Code   string `json:"code,omitempty"`
		Detail string `json:"detail"`
	}
)

func encodeErrorResponse(w http.ResponseWriter, statusCode int, err error) {
	var errs []error
	if chainedErr, ok := err.(interface{ Unwrap() []error }); ok {
		errs = append(errs, chainedErr.Unwrap()...)
	} else {
		errs = append(errs, err)
	}

	if errWithCode, ok := errors.AsType[HttpStatusCoder](err); ok {
		statusCode = errWithCode.HTTPStatus()
	}

	errResponses := sliceutils.Map(errs, func(e error) errorResponse {
		status := statusCode
		code := ""

		if errWithStatus, ok := errors.AsType[HttpStatusCoder](e); ok {
			status = errWithStatus.HTTPStatus()
		}

		if errWithCode, ok := errors.AsType[CodeError](e); ok {
			code = errWithCode.Code()
		}

		return errorResponse{
			Status: strconv.Itoa(status),
			Code:   code,
			Detail: e.Error(),
		}
	})

	encodeSingleJSONResponse(w, statusCode, errorsResponse{Errors: errResponses})
}
