package rpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc/rpcpb"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc/rpctest"
	"github.com/sergisimo/ledger/internal/platform/gateway/rpc/rpctest/pb"
	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/query/querytest"
	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/sergisimo/ledger/internal/platform/resource/resourcetest"
	"github.com/sergisimo/ledger/internal/platform/usecase/usecasetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// --------------------------------------------------------------- Stub

const resourceTypeTest resource.Type = "tests"

type (
	testCtrl struct {
		pb.UnimplementedTestServiceServer
		get    rpc.UnaryHandler[*rpcpb.IDQuery, *pb.Test]
		list   rpc.UnaryHandler[*pb.ListRequest, *pb.ListResponse]
		create rpc.UnaryHandler[*pb.Test, *pb.Test]
		patch  rpc.UnaryHandler[*pb.PatchRequest, *pb.Test]
		delete rpc.UnaryHandler[*rpcpb.IDQuery, *emptypb.Empty]
	}

	testEntity interface {
		resource.Resource
		Boolean() bool
		String() string
		Number() int64
		Double() float64
	}

	testProto struct {
		resource.Resource
		*pb.Test
	}
)

func (m *testProto) Boolean() bool {
	return m.GetBoolean()
}

func (m *testProto) String() string {
	return m.GetStr()
}

func (m *testProto) Number() int64 {
	return m.GetNumber()
}

func (m *testProto) Double() float64 {
	return m.GetDouble()
}

func matchTestEntity(want testEntity) func(testEntity) bool {
	return func(got testEntity) bool {
		if got == nil && want == nil {
			return true
		}

		return resourcetest.Match(want)(got) &&
			got.Boolean() == want.Boolean() &&
			got.String() == want.String() &&
			got.Number() == want.Number() &&
			got.Double() == want.Double()
	}
}

func newTestCtrl(ucase *usecasetest.Usecase[testEntity]) *testCtrl {
	return &testCtrl{
		get:    rpc.NewGetHandler[testEntity, *rpcpb.IDQuery](ucase, encodeTest),
		list:   rpc.NewListHandler(ucase, decodeListReq, encodeListRes),
		create: rpc.NewCreateHandler(ucase, decodeCreateReq, encodeTest),
		patch:  rpc.NewPatchHandler(ucase, decodePatchReq, encodeTest),
		delete: rpc.NewDeleteHandler[*rpcpb.IDQuery](ucase, query.DeleteTypeSoft),
	}
}

func (c *testCtrl) Register(registrar grpc.ServiceRegistrar) {
	pb.RegisterTestServiceServer(registrar, c)
}

func (c *testCtrl) Get(ctx context.Context, req *rpcpb.IDQuery) (*pb.Test, error) {
	return c.get(ctx, req)
}

func encodeTest(entity testEntity) *pb.Test {
	return &pb.Test{
		Resource: resource.ToProto(entity),
		Boolean:  entity.Boolean(),
		Str:      entity.String(),
		Number:   entity.Number(),
		Double:   entity.Double(),
	}
}

func (c *testCtrl) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	return c.list(ctx, req)
}

func decodeListReq(req *pb.ListRequest) ([]query.SrchOption, error) {
	opts := []query.SrchOption{rpc.PaginationFromProto(req.GetPage())}
	if req.Str != nil {
		opts = append(opts, query.Filter(query.Where(fields.Name("str"), filter.OpEq, req.GetStr())))
	}

	sortOpts, err := rpc.SortingFromProto(req.GetSort())
	if err != nil {
		return nil, err
	}

	return append(opts, sortOpts...), nil
}

func encodeListRes(items []testEntity, totalCollSize uint) *pb.ListResponse {
	res := &pb.ListResponse{Meta: &rpcpb.ListMeta{TotalCount: uint32(totalCollSize)}}
	for _, item := range items {
		res.Items = append(res.Items, encodeTest(item))
	}
	return res
}

func (c *testCtrl) Create(ctx context.Context, req *pb.Test) (*pb.Test, error) {
	return c.create(ctx, req)
}

func decodeCreateReq(data *pb.Test) (testEntity, error) {
	return &testProto{
		Resource: resource.FromProto(data.GetResource()),
		Test:     data,
	}, nil
}

func (c *testCtrl) Patch(ctx context.Context, req *pb.PatchRequest) (*pb.Test, error) {
	return c.patch(ctx, req)
}

func decodePatchReq(req *pb.PatchRequest) ([]query.PatchOption, error) {
	var opts []query.PatchOption
	if req.Boolean != nil {
		opts = append(opts, query.Patch("boolean", req.GetBoolean()))
	}
	if req.Str != nil {
		opts = append(opts, query.Patch("str", req.GetStr()))
	}
	if req.Number != nil {
		opts = append(opts, query.Patch("number", req.GetNumber()))
	}
	if req.Double != nil {
		opts = append(opts, query.Patch("double", req.GetDouble()))
	}

	return opts, nil
}

func (c *testCtrl) Delete(ctx context.Context, req *rpcpb.IDQuery) (*emptypb.Empty, error) {
	return c.delete(ctx, req)
}

// --------------------------------------------------------------- Test

type handlerDeps struct {
	ucase *usecasetest.Usecase[testEntity]
}

func newTestClient(t *testing.T) (pb.TestServiceClient, *handlerDeps) {
	t.Helper()

	deps := &handlerDeps{ucase: usecasetest.New[testEntity](t)}
	conn := rpctest.NewServerConn(t, rpc.ServerWithControllers(newTestCtrl(deps.ucase)))

	return pb.NewTestServiceClient(conn), deps
}

func (d *handlerDeps) expectGet(id string, entity testEntity, err error) {
	matcher := mock.MatchedBy(querytest.SrchOptMatcherFunc(query.Filter(query.Where(fields.NameID, filter.OpEq, id))))
	d.ucase.Getter.EXPECT().Get(mock.Anything, matcher).Return(entity, err)
}

func (d *handlerDeps) expectList(opts []query.SrchOption, list resource.List[testEntity], err error) {
	matcher := mock.MatchedBy(querytest.SrchOptMatcherFunc(opts...))
	d.ucase.Lister.EXPECT().List(mock.Anything, matcher).Return(list, err)
}

func (d *handlerDeps) expectCreate(toCreate, created testEntity, err error) {
	matcher := mock.MatchedBy(matchTestEntity(toCreate))
	d.ucase.Creator.EXPECT().Create(mock.Anything, matcher).Return(created, err)
}

func (d *handlerDeps) expectPatch(id string, opts []query.PatchOption, entity testEntity, err error) {
	pOpts := []query.PatchOption{query.PatchSearchOpts(query.Filter(query.Where(fields.NameID, filter.OpEq, id)))}
	opts = append(pOpts, opts...)
	matcher := mock.MatchedBy(querytest.PatchOptMatcherFunc(opts...))
	d.ucase.Patcher.EXPECT().Patch(mock.Anything, matcher).Return(entity, err)
}

func (d *handlerDeps) expectDelete(id string, delType query.DeleteType, err error) {
	matcher := mock.MatchedBy(querytest.SrchOptMatcherFunc(query.Filter(query.Where(fields.NameID, filter.OpEq, id))))
	d.ucase.Deleter.EXPECT().Delete(mock.Anything, delType, matcher).Return(err)
}

func newTestEntity(createdAt time.Time) *testProto {
	return &testProto{
		Resource: resourcetest.New(
			resourcetest.WithID("12345"),
			resourcetest.WithType(resourceTypeTest),
			resourcetest.WithCreatedAt(createdAt),
			resourcetest.WithUpdatedAt(createdAt),
		),
		Test: &pb.Test{
			Boolean: true,
			Str:     "test string",
			Number:  42,
			Double:  3.14,
		},
	}
}

func TestNewGetHandler(t *testing.T) {
	var (
		createdAt  = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		resFileDir = rpctest.GetHandlerResponseFileDir("get")

		entity = newTestEntity(createdAt)
	)

	tests := []struct {
		name       string
		id         string
		mock       func(*handlerDeps)
		assertions []rpctest.ResponseAssertion
	}{
		{
			name: "ok",
			id:   entity.ID(),
			mock: func(deps *handlerDeps) {
				deps.expectGet(entity.ID(), entity, nil)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseOK(),
				rpctest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "empty id",
			id:   "",
			mock: func(deps *handlerDeps) {},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.InvalidArgument),
				rpctest.AssertResMatchingFile(resFileDir, "empty_id", *updateGoldenFiles),
			},
		},
		{
			name: "not found",
			id:   entity.ID(),
			mock: func(deps *handlerDeps) {
				deps.expectGet(entity.ID(), nil, resource.NewErrorNotFound(resourceTypeTest, entity.ID()))
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.NotFound),
				rpctest.AssertResMatchingFile(resFileDir, "not_found", *updateGoldenFiles),
			},
		},
		{
			name: "internal error",
			id:   entity.ID(),
			mock: func(deps *handlerDeps) {
				deps.expectGet(entity.ID(), nil, assert.AnError)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.Internal),
				rpctest.AssertResMatchingFile(resFileDir, "internal", *updateGoldenFiles),
			},
		},
	}

	handlerTests := []*rpctest.HandlerTest{}
	for _, tt := range tests {
		client, deps := newTestClient(t)
		tt.mock(deps)

		handlerTests = append(handlerTests, rpctest.NewHandlerTest(
			tt.name,
			func(ctx context.Context) (proto.Message, error) {
				return client.Get(ctx, &rpcpb.IDQuery{Id: tt.id})
			},
			tt.assertions...,
		))
	}

	rpctest.NewHandlerSuite(handlerTests...).Exec(t)
}

func TestNewListHandler(t *testing.T) {
	var (
		createdAt  = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		reqFileDir = rpctest.GetHandlerRequestFileDir("list")
		resFileDir = rpctest.GetHandlerResponseFileDir("list")

		entity = newTestEntity(createdAt)
	)

	tests := []struct {
		name       string
		reqFile    string
		mock       func(*handlerDeps)
		assertions []rpctest.ResponseAssertion
	}{
		{
			name:    "ok",
			reqFile: "valid",
			mock: func(deps *handlerDeps) {
				deps.expectList(
					[]query.SrchOption{
						query.Pagination(10, 5),
						query.Filter(query.Where(fields.Name("str"), filter.OpEq, "test string")),
						query.SortBy(fields.Name("str"), query.SortDesc),
					},
					resource.NewList([]testEntity{entity}, 1),
					nil,
				)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseOK(),
				rpctest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name:    "empty",
			reqFile: "empty",
			mock: func(deps *handlerDeps) {
				deps.expectList(
					[]query.SrchOption{query.Pagination(0, 0)},
					resource.NewList([]testEntity{}, 0),
					nil,
				)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseOK(),
				rpctest.AssertResMatchingFile(resFileDir, "empty", *updateGoldenFiles),
			},
		},
		{
			name:    "invalid sort",
			reqFile: "invalid_sort",
			mock:    func(deps *handlerDeps) {},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.InvalidArgument),
				rpctest.AssertResMatchingFile(resFileDir, "invalid_sort", *updateGoldenFiles),
			},
		},
		{
			name:    "internal error",
			reqFile: "empty",
			mock: func(deps *handlerDeps) {
				deps.expectList([]query.SrchOption{query.Pagination(0, 0)}, nil, assert.AnError)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.Internal),
				rpctest.AssertResMatchingFile(resFileDir, "internal", *updateGoldenFiles),
			},
		},
	}

	handlerTests := []*rpctest.HandlerTest{}
	for _, tt := range tests {
		client, deps := newTestClient(t)
		tt.mock(deps)
		req := rpctest.RequestFromFile(t, reqFileDir, tt.reqFile, &pb.ListRequest{})

		handlerTests = append(handlerTests, rpctest.NewHandlerTest(
			tt.name,
			func(ctx context.Context) (proto.Message, error) {
				return client.List(ctx, req)
			},
			tt.assertions...,
		))
	}

	rpctest.NewHandlerSuite(handlerTests...).Exec(t)
}

func TestNewCreateHandler(t *testing.T) {
	var (
		createdAt  = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		reqFileDir = rpctest.GetHandlerRequestFileDir("create")
		resFileDir = rpctest.GetHandlerResponseFileDir("create")

		created  = newTestEntity(createdAt)
		toCreate = &testProto{
			Resource: resourcetest.New(resourcetest.ToCreateResource(resourceTypeTest)),
			Test: &pb.Test{
				Boolean: true,
				Str:     "test string",
				Number:  42,
				Double:  3.14,
			},
		}
	)

	tests := []struct {
		name       string
		reqFile    string
		mock       func(*handlerDeps)
		assertions []rpctest.ResponseAssertion
	}{
		{
			name:    "created",
			reqFile: "valid",
			mock: func(deps *handlerDeps) {
				deps.expectCreate(toCreate, created, nil)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseOK(),
				rpctest.AssertResMatchingFile(resFileDir, "created", *updateGoldenFiles),
			},
		},
		{
			name:    "internal error",
			reqFile: "valid",
			mock: func(deps *handlerDeps) {
				deps.expectCreate(toCreate, nil, assert.AnError)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.Internal),
				rpctest.AssertResMatchingFile(resFileDir, "internal", *updateGoldenFiles),
			},
		},
	}

	handlerTests := []*rpctest.HandlerTest{}
	for _, tt := range tests {
		client, deps := newTestClient(t)
		tt.mock(deps)
		req := rpctest.RequestFromFile(t, reqFileDir, tt.reqFile, &pb.Test{})

		handlerTests = append(handlerTests, rpctest.NewHandlerTest(
			tt.name,
			func(ctx context.Context) (proto.Message, error) {
				return client.Create(ctx, req)
			},
			tt.assertions...,
		))
	}

	rpctest.NewHandlerSuite(handlerTests...).Exec(t)
}

func TestNewPatchHandler(t *testing.T) {
	var (
		createdAt  = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		reqFileDir = rpctest.GetHandlerRequestFileDir("patch")
		resFileDir = rpctest.GetHandlerResponseFileDir("patch")

		entity    = newTestEntity(createdAt)
		patchOpts = []query.PatchOption{
			query.Patch("boolean", false),
			query.Patch("str", "patched string"),
			query.Patch("number", int64(99)),
			query.Patch("double", 7.5),
		}
	)

	tests := []struct {
		name       string
		reqFile    string
		mock       func(*handlerDeps)
		assertions []rpctest.ResponseAssertion
	}{
		{
			name:    "ok",
			reqFile: "valid",
			mock: func(deps *handlerDeps) {
				deps.expectPatch(entity.ID(), patchOpts, entity, nil)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseOK(),
				rpctest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name:    "partial",
			reqFile: "partial",
			mock: func(deps *handlerDeps) {
				deps.expectPatch(entity.ID(), []query.PatchOption{query.Patch("str", "patched string")}, entity, nil)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseOK(),
				rpctest.AssertResMatchingFile(resFileDir, "partial", *updateGoldenFiles),
			},
		},
		{
			name:    "empty id",
			reqFile: "empty_id",
			mock:    func(deps *handlerDeps) {},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.InvalidArgument),
				rpctest.AssertResMatchingFile(resFileDir, "empty_id", *updateGoldenFiles),
			},
		},
		{
			name:    "internal error",
			reqFile: "valid",
			mock: func(deps *handlerDeps) {
				deps.expectPatch(entity.ID(), patchOpts, nil, assert.AnError)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.Internal),
				rpctest.AssertResMatchingFile(resFileDir, "internal", *updateGoldenFiles),
			},
		},
	}

	handlerTests := []*rpctest.HandlerTest{}
	for _, tt := range tests {
		client, deps := newTestClient(t)
		tt.mock(deps)
		req := rpctest.RequestFromFile(t, reqFileDir, tt.reqFile, &pb.PatchRequest{})

		handlerTests = append(handlerTests, rpctest.NewHandlerTest(
			tt.name,
			func(ctx context.Context) (proto.Message, error) {
				return client.Patch(ctx, req)
			},
			tt.assertions...,
		))
	}

	rpctest.NewHandlerSuite(handlerTests...).Exec(t)
}

func TestNewDeleteHandler(t *testing.T) {
	resFileDir := rpctest.GetHandlerResponseFileDir("delete")

	tests := []struct {
		name       string
		id         string
		mock       func(*handlerDeps)
		assertions []rpctest.ResponseAssertion
	}{
		{
			name: "ok",
			id:   "12345",
			mock: func(deps *handlerDeps) {
				deps.expectDelete("12345", query.DeleteTypeSoft, nil)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseOK(),
				rpctest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "empty id",
			id:   "",
			mock: func(deps *handlerDeps) {},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.InvalidArgument),
				rpctest.AssertResMatchingFile(resFileDir, "empty_id", *updateGoldenFiles),
			},
		},
		{
			name: "internal error",
			id:   "12345",
			mock: func(deps *handlerDeps) {
				deps.expectDelete("12345", query.DeleteTypeSoft, assert.AnError)
			},
			assertions: []rpctest.ResponseAssertion{
				rpctest.AssertResponseCode(codes.Internal),
				rpctest.AssertResMatchingFile(resFileDir, "internal", *updateGoldenFiles),
			},
		},
	}

	handlerTests := []*rpctest.HandlerTest{}
	for _, tt := range tests {
		client, deps := newTestClient(t)
		tt.mock(deps)

		handlerTests = append(handlerTests, rpctest.NewHandlerTest(
			tt.name,
			func(ctx context.Context) (proto.Message, error) {
				return client.Delete(ctx, &rpcpb.IDQuery{Id: tt.id})
			},
			tt.assertions...,
		))
	}

	rpctest.NewHandlerSuite(handlerTests...).Exec(t)
}

func TestWithErrorMapper(t *testing.T) {
	t.Parallel()

	ucase := usecasetest.New[testEntity](t)
	ucase.Deleter.EXPECT().Delete(mock.Anything, query.DeleteTypeSoft, mock.Anything).Return(assert.AnError)

	handler := rpc.NewDeleteHandler[*rpcpb.IDQuery](
		ucase,
		query.DeleteTypeSoft,
		rpc.WithErrorMapper(func(error) error { return status.Error(codes.Aborted, "mapped") }),
	)

	_, err := handler(t.Context(), &rpcpb.IDQuery{Id: "12345"})
	assert.Equal(t, codes.Aborted, status.Code(err))
}
