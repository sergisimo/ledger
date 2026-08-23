package rest_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/gateway/rest"
	"github.com/sergisimo/ledger/internal/platform/gateway/rest/restest"
	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/query/querytest"
	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/sergisimo/ledger/internal/platform/resource/resourcetest"
	"github.com/sergisimo/ledger/internal/platform/usecase/usecasetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --------------------------------------------------------------- Stub

const resourceTypeTest resource.Type = "tests"

type (
	testCtrl struct {
		get    http.Handler
		list   http.Handler
		create http.Handler
	}

	testEntity interface {
		resource.Resource
		Boolean() bool
		String() string
		Number() int
		Double() float64
	}

	testDTO struct {
		resource.RestDTO
		TBoolean bool    `json:"boolean"`
		TStr     string  `json:"str"`
		TNumber  int     `json:"number"`
		TDouble  float64 `json:"double"`
	}
)

func (d *testDTO) Boolean() bool {
	return d.TBoolean
}

func (d *testDTO) String() string {
	return d.TStr
}

func (d *testDTO) Number() int {
	return d.TNumber
}

func (d *testDTO) Double() float64 {
	return d.TDouble
}

func testEntityToDTO(entity testEntity) *testDTO {
	return &testDTO{
		RestDTO:  resource.ToRestDTO(entity),
		TBoolean: entity.Boolean(),
		TStr:     entity.String(),
		TNumber:  entity.Number(),
		TDouble:  entity.Double(),
	}
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
		get:    rest.NewGetHandler(ucase, testEntityToDTO),
		list:   rest.NewListHandler(ucase, testEntityToDTO),
		create: rest.NewCreateHandler(ucase, testEntityToDTO),
	}
}

func (c *testCtrl) BasePath() string {
	return "/" + resourceTypeTest.String()
}

func (c *testCtrl) Endpoints() []*rest.Endpoint {
	return []*rest.Endpoint{
		rest.NewGetEndpoint(c.get),
		rest.NewListEndpoint(c.list),
		rest.NewCreateEndpoint(c.create),
	}
}

// --------------------------------------------------------------- Test

type handlerDeps struct {
	ucase *usecasetest.Usecase[testEntity]
}

func (d *handlerDeps) expectGet(id string, entity testEntity, err error) {
	matcher := mock.MatchedBy(querytest.SrchOptMatcherFunc(query.FilterBy(fields.NameID, filter.OpEq, id)))
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

func TestNewGetHandler(t *testing.T) {
	var (
		createdAt  = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		resFileDir = restest.GetHandlerResponseFileDir("get")

		entity = &testDTO{
			RestDTO: resource.ToRestDTO(resourcetest.New(
				resourcetest.WithID("12345"),
				resourcetest.WithType(resourceTypeTest),
				resourcetest.WithCreatedAt(createdAt),
				resourcetest.WithUpdatedAt(createdAt),
			)),
			TBoolean: true,
			TStr:     "test string",
			TNumber:  42,
			TDouble:  3.14,
		}
	)

	tests := []struct {
		name       string
		id         string
		mock       func(*handlerDeps)
		reqOpts    []restest.RequestOption
		assertions []restest.ResponseAssertion
	}{
		{
			name: "ok",
			id:   entity.ID(),
			mock: func(deps *handlerDeps) {
				deps.expectGet(entity.ID(), entity, nil)
			},
			reqOpts: []restest.RequestOption{},
			assertions: []restest.ResponseAssertion{
				restest.AssertGetResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "not found",
			id:   "4321",
			mock: func(deps *handlerDeps) {
				qry := query.NewSearch(query.FilterBy(fields.NameID, filter.OpEq, "4321"))
				err := resource.NewErrorNotFound(resourceTypeTest, qry.Filters().String())
				deps.expectGet("4321", nil, err)
			},
			reqOpts: []restest.RequestOption{},
			assertions: []restest.ResponseAssertion{
				restest.AssertResponseStatus(http.StatusNotFound),
				restest.AssertResMatchingFile(resFileDir, "not_found", *updateGoldenFiles),
			},
		},
	}

	handlerTests := []*restest.HandlerTest{}
	for _, tt := range tests {
		deps := &handlerDeps{ucase: usecasetest.New[testEntity](t)}
		ctrl := newTestCtrl(deps.ucase)
		tt.mock(deps)

		handlerTests = append(handlerTests, restest.NewHandlerTest(
			tt.name,
			restest.NewEndpointsHandler(ctrl.BasePath(), ctrl.Endpoints()...),
			restest.NewGetRequest(t, resourceTypeTest, tt.id, tt.reqOpts...),
			tt.assertions...,
		))
	}

	restest.NewHandlerSuite(handlerTests...).Exec(t)
}

func TestNewListHandler(t *testing.T) {
	var (
		createdAt  = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		resFileDir = restest.GetHandlerResponseFileDir("list")

		entity1 = &testDTO{
			RestDTO: resource.ToRestDTO(resourcetest.New(
				resourcetest.WithID("12345"),
				resourcetest.WithType(resourceTypeTest),
				resourcetest.WithCreatedAt(createdAt),
				resourcetest.WithUpdatedAt(createdAt),
			)),
			TBoolean: true,
			TStr:     "test string 1",
			TNumber:  42,
			TDouble:  3.14,
		}

		entity2 = &testDTO{
			RestDTO: resource.ToRestDTO(resourcetest.New(
				resourcetest.WithID("67890"),
				resourcetest.WithType(resourceTypeTest),
				resourcetest.WithCreatedAt(createdAt),
				resourcetest.WithUpdatedAt(createdAt),
			)),
			TBoolean: false,
			TStr:     "test string 2",
			TNumber:  100,
			TDouble:  2.71,
		}

		listEmpty     = resource.NewList([]testEntity{}, 0)
		listWithItems = resource.NewList([]testEntity{entity1, entity2}, 2)

		qryOpts = []query.SrchOption{query.FilterBy("str", filter.OpEq, "hello")}
	)

	tests := []struct {
		name       string
		mock       func(*handlerDeps)
		reqOpts    []restest.RequestOption
		assertions []restest.ResponseAssertion
	}{
		{
			name: "empty",
			mock: func(deps *handlerDeps) {
				deps.expectList(qryOpts, listEmpty, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][eq]", "hello"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertListResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "empty", *updateGoldenFiles),
			},
		},
		{
			name: "ok",
			mock: func(deps *handlerDeps) {
				deps.expectList(qryOpts, listWithItems, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][eq]", "hello"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertListResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "internal",
			mock: func(deps *handlerDeps) {
				deps.expectList(qryOpts, nil, assert.AnError)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][eq]", "hello"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertResponseStatus(http.StatusInternalServerError),
				restest.AssertResMatchingFile(resFileDir, "internal", *updateGoldenFiles),
			},
		},
		{
			name: "filters with various operators and value types",
			mock: func(deps *handlerDeps) {
				opts := []query.SrchOption{
					query.FilterBy("str", filter.OpLike, "%hello%"),
					query.FilterBy("boolean", filter.OpEq, true),
					query.FilterBy("number", filter.OpGT, "100"),
					query.FilterBy("double", filter.OpLTEq, "3.14"),
					query.FilterBy("id", filter.OpIn, []string{"123", "456", "789"}),
				}
				deps.expectList(opts, listWithItems, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][like]", "%hello%"),
				restest.RequestWithQueryParam("filter[boolean][eq]", "true"),
				restest.RequestWithQueryParam("filter[number][gt]", "100"),
				restest.RequestWithQueryParam("filter[double][lte]", "3.14"),
				restest.RequestWithQueryParam("filter[id][in]", "123,456,789"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertListResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "filters with null and negative values",
			mock: func(deps *handlerDeps) {
				opts := []query.SrchOption{
					query.FilterBy("str", filter.OpIs, nil),
					query.FilterBy("number", filter.OpLT, "-50"),
					query.FilterBy("boolean", filter.OpNEq, false),
				}
				deps.expectList(opts, listWithItems, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][is]", "null"),
				restest.RequestWithQueryParam("filter[number][lt]", "-50"),
				restest.RequestWithQueryParam("filter[boolean][ne]", "false"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertListResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "pagination with limit and offset",
			mock: func(deps *handlerDeps) {
				opts := []query.SrchOption{
					query.FilterBy("str", filter.OpEq, "hello"),
					query.Pagination(25, 50),
				}
				deps.expectList(opts, listWithItems, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][eq]", "hello"),
				restest.RequestWithQueryParam("page[limit]", "25"),
				restest.RequestWithQueryParam("page[offset]", "50"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertListResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "pagination with only limit",
			mock: func(deps *handlerDeps) {
				opts := []query.SrchOption{
					query.FilterBy("str", filter.OpEq, "hello"),
					query.Pagination(10, 0),
				}
				deps.expectList(opts, listWithItems, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][eq]", "hello"),
				restest.RequestWithQueryParam("page[limit]", "10"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertListResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "sorting ascending and descending",
			mock: func(deps *handlerDeps) {
				opts := []query.SrchOption{
					query.FilterBy("str", filter.OpEq, "hello"),
					query.SortBy("str", query.SortAsc),
					query.SortBy("number", query.SortDesc),
					query.SortBy("double", query.SortAsc),
				}
				deps.expectList(opts, listWithItems, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][eq]", "hello"),
				restest.RequestWithQueryParam("sort", "str,-number,double"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertListResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
		{
			name: "combined filters, pagination, and sorting",
			mock: func(deps *handlerDeps) {
				opts := []query.SrchOption{
					query.FilterBy("str", filter.OpEq, "hello"),
					query.FilterBy("number", filter.OpGTEq, "42"),
					query.FilterBy("double", filter.OpBetween, []string{"1.0", "5.0"}),
					query.Pagination(15, 30),
					query.SortBy("createdAt", query.SortDesc),
					query.SortBy("str", query.SortAsc),
				}
				deps.expectList(opts, listWithItems, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithQueryParam("filter[str][eq]", "hello"),
				restest.RequestWithQueryParam("filter[number][gte]", "42"),
				restest.RequestWithQueryParam("filter[double][btw]", "1.0,5.0"),
				restest.RequestWithQueryParam("page[limit]", "15"),
				restest.RequestWithQueryParam("page[offset]", "30"),
				restest.RequestWithQueryParam("sort", "-createdAt,str"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertListResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", *updateGoldenFiles),
			},
		},
	}

	handlerTests := []*restest.HandlerTest{}
	for _, tt := range tests {
		deps := &handlerDeps{ucase: usecasetest.New[testEntity](t)}
		ctrl := newTestCtrl(deps.ucase)
		tt.mock(deps)

		handlerTests = append(handlerTests, restest.NewHandlerTest(
			tt.name,
			restest.NewEndpointsHandler(ctrl.BasePath(), ctrl.Endpoints()...),
			restest.NewListRequest(t, resourceTypeTest, tt.reqOpts...),
			tt.assertions...,
		))
	}

	restest.NewHandlerSuite(handlerTests...).Exec(t)
}

func TestNewCreateHandler(t *testing.T) {
	var (
		createdAt  = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		reqFileDir = restest.GetHandlerRequestFileDir("create")
		resFileDir = restest.GetHandlerResponseFileDir("create")

		toCreate = &testDTO{
			RestDTO:  resource.ToRestDTO(resourcetest.New(resourcetest.ToCreateResource(resourceTypeTest))),
			TBoolean: true,
			TStr:     "test string",
			TNumber:  42,
			TDouble:  3.14,
		}

		created = &testDTO{
			RestDTO: resource.ToRestDTO(resourcetest.New(
				resourcetest.WithID("12345"),
				resourcetest.WithType(resourceTypeTest),
				resourcetest.WithCreatedAt(createdAt),
				resourcetest.WithUpdatedAt(createdAt),
			)),
			TBoolean: true,
			TStr:     "test string",
			TNumber:  42,
			TDouble:  3.14,
		}
	)

	tests := []struct {
		name       string
		mock       func(*handlerDeps)
		reqOpts    []restest.RequestOption
		assertions []restest.ResponseAssertion
	}{
		{
			name: "created",
			mock: func(deps *handlerDeps) {
				deps.expectCreate(toCreate, created, nil)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithBodyFromFile(t, reqFileDir, "valid.json"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertCreateResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "created", *updateGoldenFiles),
			},
		},
		{
			name: "internal error",
			mock: func(deps *handlerDeps) {
				deps.expectCreate(toCreate, nil, assert.AnError)
			},
			reqOpts: []restest.RequestOption{
				restest.RequestWithBodyFromFile(t, reqFileDir, "valid.json"),
			},
			assertions: []restest.ResponseAssertion{
				restest.AssertResponseStatus(http.StatusInternalServerError),
				restest.AssertResMatchingFile(resFileDir, "internal", *updateGoldenFiles),
			},
		},
	}

	handlerTests := []*restest.HandlerTest{}
	for _, tt := range tests {
		deps := &handlerDeps{ucase: usecasetest.New[testEntity](t)}
		ctrl := newTestCtrl(deps.ucase)
		tt.mock(deps)

		handlerTests = append(handlerTests, restest.NewHandlerTest(
			tt.name,
			restest.NewEndpointsHandler(ctrl.BasePath(), ctrl.Endpoints()...),
			restest.NewCreateRequest(t, resourceTypeTest, tt.reqOpts...),
			tt.assertions...,
		))
	}

	restest.NewHandlerSuite(handlerTests...).Exec(t)
}
