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
	Encoder     func(w http.ResponseWriter, statusCode int, data any)
	ListEncoder func(w http.ResponseWriter, statusCode int, data []any, totalCollSize uint)

	HandlerOpt func(c *handlerConfig)

	handlerConfig struct {
		status       int
		encoder      Encoder
		listEncoder  ListEncoder
		errorEncoder ErrorEncoder
	}
)

func WithStatus(status int) HandlerOpt {
	return func(c *handlerConfig) {
		c.status = status
	}
}

func WithEncoder(encoder Encoder) HandlerOpt {
	return func(c *handlerConfig) {
		c.encoder = encoder
	}
}

func WithListEncoder(encoder ListEncoder) HandlerOpt {
	return func(c *handlerConfig) {
		c.listEncoder = encoder
	}
}

func WithErrorEncoder(errorEncoder ErrorEncoder) HandlerOpt {
	return func(c *handlerConfig) {
		c.errorEncoder = errorEncoder
	}
}

// --------------------------------------------------------------- Constructors

func getHandlerDefaultOpts() []HandlerOpt {
	return []HandlerOpt{
		WithStatus(http.StatusOK),
		WithEncoder(encodeSingleJSONResponse),
		WithErrorEncoder(encodeErrorResponse),
	}
}

func NewGetHandler[R, DTO resource.Resource](
	getter usecase.Getter[R],
	toDTO func(R) DTO,
	opts ...HandlerOpt,
) http.Handler {
	cfg := &handlerConfig{}
	for _, opt := range append(getHandlerDefaultOpts(), opts...) {
		opt(cfg)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srchOpts, err := decodeGetReq(r.Context(), r)
		if err != nil {
			cfg.errorEncoder(w, http.StatusBadRequest, err)
			return
		}

		res, err := getter.Get(r.Context(), srchOpts...)
		if err != nil {
			cfg.errorEncoder(w, http.StatusInternalServerError, err)
			return
		}

		cfg.encoder(w, cfg.status, toDTO(res))
	})
}

func listHandlerDefaultOpts() []HandlerOpt {
	return []HandlerOpt{
		WithStatus(http.StatusOK),
		WithListEncoder(encodeListJSONResponse),
		WithErrorEncoder(encodeErrorResponse),
	}
}

func NewListHandler[R, DTO resource.Resource](
	lister usecase.Lister[R],
	toDTO func(R) DTO,
	opts ...HandlerOpt,
) http.Handler {
	cfg := &handlerConfig{}
	for _, opt := range append(listHandlerDefaultOpts(), opts...) {
		opt(cfg)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srchOpts, err := parseURLSrchOpts(r.URL)
		if err != nil {
			cfg.errorEncoder(w, http.StatusBadRequest, err)
			return
		}

		resList, err := lister.List(r.Context(), srchOpts...)
		if err != nil {
			cfg.errorEncoder(w, http.StatusInternalServerError, err)
			return
		}

		dtoList := sliceutils.Map(resList.Items(), func(res R) any { return toDTO(res) })
		cfg.listEncoder(w, cfg.status, dtoList, resList.TotalCollSize())
	})
}

func createHandlerDefaultOpts() []HandlerOpt {
	return []HandlerOpt{
		WithStatus(http.StatusCreated),
		WithEncoder(encodeSingleJSONResponse),
		WithErrorEncoder(encodeErrorResponse),
	}
}

func NewCreateHandler[R, DTO resource.Resource](
	creator usecase.Creator[R],
	toDTO func(R) DTO,
	opts ...HandlerOpt,
) http.Handler {
	varutils.MustImplement[R, DTO]()

	cfg := &handlerConfig{}
	for _, opt := range append(createHandlerDefaultOpts(), opts...) {
		opt(cfg)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var dto struct {
			Data DTO `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
			cfg.errorEncoder(w, http.StatusBadRequest, err)
			return
		}

		res, err := creator.Create(r.Context(), any(dto.Data).(R))
		if err != nil {
			cfg.errorEncoder(w, http.StatusInternalServerError, err)
			return
		}

		cfg.encoder(w, cfg.status, toDTO(res))
	})
}
