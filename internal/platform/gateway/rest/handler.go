package rest

import (
	"encoding/json"
	"net/http"

	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/sergisimo/ledger/internal/platform/usecase"
	"github.com/sergisimo/ledger/internal/platform/utils/sliceutils"
	"github.com/sergisimo/ledger/internal/platform/utils/varutils"
)

// --------------------------------------------------------------- Contract

type (
	HandlerOpt func(c *handlerConfig)

	handlerConfig struct{}

	response struct {
		Data any `json:"data"`
	}

	errorResponse struct {
		Errors []string `json:"errors"`
	}
)

// --------------------------------------------------------------- Constructors

func NewGetHandler[R, DTO resource.Resource](
	getter usecase.Getter[R],
	toDTO func(R) DTO,
	opts ...HandlerOpt,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srchOpts, err := decodeGetReq(r.Context(), r)
		if err != nil {
			encodeErrorResponse(w, http.StatusBadRequest, err)
			return
		}

		res, err := getter.Get(r.Context(), srchOpts...)
		if err != nil {
			encodeErrorResponse(w, http.StatusInternalServerError, err)
			return
		}

		encodeJSONResponse(w, http.StatusOK, response{Data: toDTO(res)})
	})
}

func NewListHandler[R, DTO resource.Resource](
	lister usecase.Lister[R],
	toDTO func(R) DTO,
	opts ...HandlerOpt,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srchOpts, err := parseURLSrchOpts(r.URL)
		if err != nil {
			encodeErrorResponse(w, http.StatusBadRequest, err)
			return
		}

		resList, err := lister.List(r.Context(), srchOpts...)
		if err != nil {
			encodeErrorResponse(w, http.StatusInternalServerError, err)
			return
		}

		dtoList := sliceutils.Map(resList.Result(), toDTO)
		encodeJSONResponse(w, http.StatusOK, response{Data: dtoList})
	})
}

func NewCreateHandler[R, DTO resource.Resource](
	creator usecase.Creator[R],
	toDTO func(R) DTO,
	opts ...HandlerOpt,
) http.Handler {
	varutils.MustImplement[R, DTO]()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var dto DTO
		if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
			encodeErrorResponse(w, http.StatusBadRequest, err)
			return
		}

		res, err := creator.Create(r.Context(), any(dto).(R))
		if err != nil {
			encodeErrorResponse(w, http.StatusInternalServerError, err)
			return
		}

		encodeJSONResponse(w, http.StatusCreated, response{Data: toDTO(res)})
	})
}

// --------------------------------------------------------------- Helpers

func encodeJSONResponse(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	err := json.NewEncoder(w).Encode(data)
	if err != nil {
		encodeErrorResponse(w, http.StatusInternalServerError, err)
	}
}

func encodeErrorResponse(w http.ResponseWriter, statusCode int, err error) {
	errWithCode, ok := err.(interface{ HTTPCode() int })
	if ok {
		statusCode = errWithCode.HTTPCode()
	}

	var errs []error
	if chainedErr, ok := err.(interface{ Unwrap() []error }); ok {
		errs = append(errs, chainedErr.Unwrap()...)
	} else {
		errs = append(errs, err)
	}

	errResponses := sliceutils.Map(errs, func(e error) string { return e.Error() })
	encodeJSONResponse(w, statusCode, errorResponse{Errors: errResponses})
}
