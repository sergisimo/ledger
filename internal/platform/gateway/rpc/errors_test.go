package rpc_test

import (
	"net/http"
	"testing"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --------------------------------------------------------------- Stub

type grpcCodedErr struct {
	code codes.Code
}

func (e *grpcCodedErr) Error() string {
	return "grpc coded error"
}

func (e *grpcCodedErr) GRPCCode() codes.Code {
	return e.code
}

type httpStatusErr struct {
	status int
}

func (e *httpStatusErr) Error() string {
	return "http status error"
}

func (e *httpStatusErr) HTTPStatus() int {
	return e.status
}

type codedErr struct {
	code string
}

func (e *codedErr) Error() string {
	return "coded error"
}

func (e *codedErr) Code() string {
	return e.code
}

type multiErr struct {
	errs []error
}

func (e *multiErr) Error() string {
	return "multi error"
}

func (e *multiErr) Unwrap() []error {
	return e.errs
}

// --------------------------------------------------------------- Test

func TestToStatusError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		wantCode    codes.Code
		wantReasons []string
		wantFields  []string
	}{
		{
			name:     "nil error",
			err:      nil,
			wantCode: codes.OK,
		},
		{
			name:     "status error passthrough",
			err:      status.Error(codes.AlreadyExists, "already exists"),
			wantCode: codes.AlreadyExists,
		},
		{
			name:     "grpc coder takes precedence over http status",
			err:      &multiErr{errs: []error{&httpStatusErr{status: http.StatusNotFound}, &grpcCodedErr{code: codes.FailedPrecondition}}},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "grpc coder",
			err:      &grpcCodedErr{code: codes.FailedPrecondition},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:        "code error attaches error info",
			err:         &codedErr{code: "TEST_CODE"},
			wantCode:    codes.Internal,
			wantReasons: []string{"TEST_CODE"},
		},
		{
			name:     "empty code attaches nothing",
			err:      &codedErr{code: ""},
			wantCode: codes.Internal,
		},
		{
			name: "multi error aggregates details",
			err: &multiErr{errs: []error{
				fields.NewErrInvalidEmptyString("name"),
				&codedErr{code: "TEST_CODE"},
				fields.NewErrNil("country"),
			}},
			wantCode:    codes.Internal,
			wantReasons: []string{"TEST_CODE"},
			wantFields:  []string{"name", "country"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st, ok := status.FromError(rpc.ToStatusError(tt.err))
			require.True(t, ok)
			assert.Equal(t, tt.wantCode, st.Code())

			var reasons, violated []string
			for _, detail := range st.Details() {
				switch d := detail.(type) {
				case *errdetails.ErrorInfo:
					reasons = append(reasons, d.GetReason())
				case *errdetails.BadRequest:
					for _, violation := range d.GetFieldViolations() {
						violated = append(violated, violation.GetField())
					}
				}
			}
			assert.Equal(t, tt.wantReasons, reasons)
			assert.Equal(t, tt.wantFields, violated)
		})
	}
}

func TestToStatusErrorHTTPStatusMapping(t *testing.T) {
	t.Parallel()

	tests := map[int]codes.Code{
		http.StatusBadRequest:          codes.InvalidArgument,
		http.StatusUnauthorized:        codes.Unauthenticated,
		http.StatusForbidden:           codes.PermissionDenied,
		http.StatusNotFound:            codes.NotFound,
		http.StatusConflict:            codes.AlreadyExists,
		http.StatusRequestTimeout:      codes.DeadlineExceeded,
		http.StatusGatewayTimeout:      codes.DeadlineExceeded,
		http.StatusPreconditionFailed:  codes.FailedPrecondition,
		http.StatusTooManyRequests:     codes.ResourceExhausted,
		http.StatusNotImplemented:      codes.Unimplemented,
		http.StatusServiceUnavailable:  codes.Unavailable,
		http.StatusTeapot:              codes.InvalidArgument,
		http.StatusInternalServerError: codes.Internal,
	}

	for httpStatus, wantCode := range tests {
		t.Run(http.StatusText(httpStatus), func(t *testing.T) {
			t.Parallel()

			err := rpc.ToStatusError(&httpStatusErr{status: httpStatus})
			assert.Equal(t, wantCode, status.Code(err))
		})
	}
}
