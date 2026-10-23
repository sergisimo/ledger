package rpc

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/sergisimo/ledger/internal/platform/usecase"
)

// --------------------------------------------------------------- Contract

type (
	UnaryHandler[Req, Res proto.Message] func(ctx context.Context, req Req) (Res, error)

	HandlerOpt func(c *handlerConfig)

	handlerConfig struct {
		errorMapper ErrorMapper
	}
)

func WithErrorMapper(mapper ErrorMapper) HandlerOpt {
	return func(c *handlerConfig) {
		c.errorMapper = mapper
	}
}

// --------------------------------------------------------------- Constructors

func newHandlerConfig(opts ...HandlerOpt) *handlerConfig {
	cfg := &handlerConfig{errorMapper: ToStatusError}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func NewGetHandler[R resource.Resource, Req Identifiable, Res proto.Message](
	getter usecase.Getter[R],
	encode func(R) Res,
	opts ...HandlerOpt,
) UnaryHandler[Req, Res] {
	cfg := newHandlerConfig(opts...)

	return func(ctx context.Context, req Req) (Res, error) {
		var zero Res

		srchOpts, err := idSrchOpts(req)
		if err != nil {
			return zero, invalidArgument(err)
		}

		res, err := getter.Get(ctx, srchOpts...)
		if err != nil {
			return zero, cfg.errorMapper(err)
		}

		return encode(res), nil
	}
}

func NewListHandler[R resource.Resource, Req, Res proto.Message](
	lister usecase.Lister[R],
	decode func(Req) ([]query.SrchOption, error),
	encode func(items []R, totalCollSize uint) Res,
	opts ...HandlerOpt,
) UnaryHandler[Req, Res] {
	cfg := newHandlerConfig(opts...)

	return func(ctx context.Context, req Req) (Res, error) {
		var zero Res

		srchOpts, err := decode(req)
		if err != nil {
			return zero, invalidArgument(err)
		}

		resList, err := lister.List(ctx, srchOpts...)
		if err != nil {
			return zero, cfg.errorMapper(err)
		}

		return encode(resList.Items(), resList.TotalCollSize()), nil
	}
}

func NewCreateHandler[R resource.Resource, Req, Res proto.Message](
	creator usecase.Creator[R],
	decode func(Req) (R, error),
	encode func(R) Res,
	opts ...HandlerOpt,
) UnaryHandler[Req, Res] {
	cfg := newHandlerConfig(opts...)

	return func(ctx context.Context, req Req) (Res, error) {
		var zero Res

		res, err := decode(req)
		if err != nil {
			return zero, invalidArgument(err)
		}

		created, err := creator.Create(ctx, res)
		if err != nil {
			return zero, cfg.errorMapper(err)
		}

		return encode(created), nil
	}
}

func NewPatchHandler[R resource.Resource, Req Identifiable, Res proto.Message](
	patcher usecase.Patcher[R],
	decode func(Req) ([]query.PatchOption, error),
	encode func(R) Res,
	opts ...HandlerOpt,
) UnaryHandler[Req, Res] {
	cfg := newHandlerConfig(opts...)

	return func(ctx context.Context, req Req) (Res, error) {
		var zero Res

		srchOpts, err := idSrchOpts(req)
		if err != nil {
			return zero, invalidArgument(err)
		}
		patchOpts := []query.PatchOption{query.PatchSearchOpts(srchOpts...)}

		reqOpts, err := decode(req)
		if err != nil {
			return zero, invalidArgument(err)
		}
		patchOpts = append(patchOpts, reqOpts...)

		res, err := patcher.Patch(ctx, patchOpts...)
		if err != nil {
			return zero, cfg.errorMapper(err)
		}

		return encode(res), nil
	}
}

func NewDeleteHandler[Req Identifiable](
	deleter usecase.Deleter,
	delType query.DeleteType,
	opts ...HandlerOpt,
) UnaryHandler[Req, *emptypb.Empty] {
	cfg := newHandlerConfig(opts...)

	return func(ctx context.Context, req Req) (*emptypb.Empty, error) {
		srchOpts, err := idSrchOpts(req)
		if err != nil {
			return nil, invalidArgument(err)
		}

		if err := deleter.Delete(ctx, delType, srchOpts...); err != nil {
			return nil, cfg.errorMapper(err)
		}

		return &emptypb.Empty{}, nil
	}
}
