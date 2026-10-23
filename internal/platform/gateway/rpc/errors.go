package rpc

import (
	"errors"
	"net/http"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

// --------------------------------------------------------------- Contract

type (
	ErrorMapper func(err error) error

	GRPCCoder interface {
		error
		GRPCCode() codes.Code
	}

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

func ToStatusError(err error) error {
	return toStatusError(err, codes.Internal)
}

func invalidArgument(err error) error {
	return toStatusError(err, codes.InvalidArgument)
}

func toStatusError(err error, fallback codes.Code) error {
	if err == nil {
		return nil
	}
	if statusErr, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		return statusErr.GRPCStatus().Err()
	}

	var errs []error
	if chainedErr, ok := err.(interface{ Unwrap() []error }); ok {
		errs = append(errs, chainedErr.Unwrap()...)
	} else {
		errs = append(errs, err)
	}

	st := status.New(codeFromError(err, fallback), err.Error())

	var details []*errdetails.BadRequest_FieldViolation
	for _, e := range errs {
		if errWithCode, ok := errors.AsType[CodeError](e); ok && errWithCode.Code() != "" {
			if withInfo, detErr := st.WithDetails(&errdetails.ErrorInfo{Reason: errWithCode.Code()}); detErr == nil {
				st = withInfo
			}
		}

		if fieldErr, ok := errors.AsType[fields.WithFieldNameError](e); ok {
			details = append(details, &errdetails.BadRequest_FieldViolation{
				Field:       string(fieldErr.FieldName()),
				Description: fieldErr.Error(),
			})
		}
	}
	if len(details) > 0 {
		if withBadReq, detErr := st.WithDetails(&errdetails.BadRequest{FieldViolations: details}); detErr == nil {
			st = withBadReq
		}
	}

	return st.Err()
}

func codeFromError(err error, fallback codes.Code) codes.Code {
	if coder, ok := errors.AsType[GRPCCoder](err); ok {
		return coder.GRPCCode()
	}
	if coder, ok := errors.AsType[HttpStatusCoder](err); ok {
		return codeFromHTTPStatus(coder.HTTPStatus())
	}
	return fallback
}

func codeFromHTTPStatus(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	case http.StatusPreconditionFailed:
		return codes.FailedPrecondition
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusNotImplemented:
		return codes.Unimplemented
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	default:
		if httpStatus >= http.StatusBadRequest && httpStatus < http.StatusInternalServerError {
			return codes.InvalidArgument
		}
		return codes.Internal
	}
}
