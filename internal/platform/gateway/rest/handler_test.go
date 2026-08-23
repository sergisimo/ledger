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
	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/sergisimo/ledger/internal/platform/resource/resourcetest"
	"github.com/sergisimo/ledger/internal/platform/usecase/usecasetest"
	"github.com/stretchr/testify/mock"
)

const resourceTypeTest resource.Type = "tests"

type (
	testCtrl struct {
		get http.Handler
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

func newTestCtrl(ucase *usecasetest.Usecase[testEntity]) *testCtrl {
	return &testCtrl{
		get: rest.NewGetHandler(ucase, testEntityToDTO),
	}
}

func (c *testCtrl) BasePath() string {
	return "/" + resourceTypeTest.String()
}

func (c *testCtrl) Endpoints() []*rest.Endpoint {
	return []*rest.Endpoint{
		rest.NewGetEndpoint(c.get),
	}
}

// TODO: create matchers for the search options for the mocks
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
		mock       func(*usecasetest.Usecase[testEntity])
		reqOpts    []restest.RequestOption
		assertions []restest.ResponseAssertion
	}{
		{
			name: "ok",
			id:   entity.ID(),
			mock: func(ucase *usecasetest.Usecase[testEntity]) {
				ucase.Getter.EXPECT().Get(mock.Anything, mock.Anything).Return(entity, nil)
			},
			reqOpts: []restest.RequestOption{},
			assertions: []restest.ResponseAssertion{
				restest.AssertGetResponseOK(),
				restest.AssertResMatchingFile(resFileDir, "ok", false),
			},
		},
		{
			name: "not found",
			id:   "4321",
			mock: func(ucase *usecasetest.Usecase[testEntity]) {
				qry := query.NewSearch(query.FilterBy(fields.NameID, filter.OpEq, "4321"))
				err := resource.NewErrorNotFound(resourceTypeTest, qry.Filters().String())
				ucase.Getter.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, err)
			},
			reqOpts: []restest.RequestOption{},
			assertions: []restest.ResponseAssertion{
				restest.AssertResponseStatus(http.StatusNotFound),
				restest.AssertResMatchingFile(resFileDir, "not_found", false),
			},
		},
	}

	handlerTests := []*restest.HandlerTest{}
	for _, tt := range tests {
		ucase := usecasetest.New[testEntity](t)
		ctrl := newTestCtrl(ucase)
		tt.mock(ucase)

		handlerTests = append(handlerTests, restest.NewHandlerTest(
			tt.name,
			restest.NewEndpointsHandler(ctrl.BasePath(), ctrl.Endpoints()...),
			restest.NewGETRequest(t, resourceTypeTest, tt.id, tt.reqOpts...),
			tt.assertions...,
		))
	}

	restest.NewHandlerSuite(handlerTests...).Exec(t)
}
