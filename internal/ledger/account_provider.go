package ledger

import (
	"context"
	"net/http"
	"time"

	"github.com/biter777/countries"
	"github.com/sergisimo/ledger/internal/platform/gateway/rest"
	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/sergisimo/ledger/internal/platform/usecase"
)

// --------------------------------------------------------------- Contract

type (
	AccountProvider interface {
		resource.Resource
		Name() string
		Country() countries.CountryCode
		Kind() AccountProviderType
	}

	AccountProviderUsecase interface {
		usecase.Getter[AccountProvider]
	}

	AccountProviderType string
)

const (
	ResourceTypeAccountProvider resource.Type = "account-providers"

	AccountProviderTypeUnknown     AccountProviderType = "UNKNOWN"
	AccountProviderTypeBank        AccountProviderType = "BANK"
	AccountProviderTypeBroker      AccountProviderType = "BROKER"
	AccountProviderTypeCrowdlender AccountProviderType = "CROWDLENDER"
)

// --------------------------------------------------------------- Repository

// --------------------------------------------------------------- Usecase

type accountProviderUsecase struct{}

func (u *accountProviderUsecase) Get(ctx context.Context, opts ...query.SrchOption) (AccountProvider, error) {
	return &accountProviderRestDto{
		RestDTO:   resource.RestDTO{RID: "1", RType: ResourceTypeAccountProvider, RCreatedAt: time.Now(), RUpdatedAt: time.Now()},
		APName:    "Bank of America",
		APCountry: countries.USA.String(),
		APKind:    string(AccountProviderTypeBank),
	}, nil
}

// --------------------------------------------------------------- Rest Ctrl

type accountProviderRestDto struct {
	resource.RestDTO
	APName    string `json:"name"`
	APCountry string `json:"country"`
	APKind    string `json:"kind"`
}

func accountProviderToRestDTO(ap AccountProvider) *accountProviderRestDto {
	return &accountProviderRestDto{
		RestDTO:   resource.ToRestDTO(ap),
		APName:    ap.Name(),
		APCountry: ap.Country().String(),
		APKind:    string(ap.Kind()),
	}
}

func (dto *accountProviderRestDto) Name() string {
	return dto.APName
}

func (dto *accountProviderRestDto) Country() countries.CountryCode {
	return countries.ByName(dto.APCountry)
}

func (dto *accountProviderRestDto) Kind() AccountProviderType {
	return AccountProviderType(dto.APKind)
}

type accountProviderRestCtrl struct {
	get http.Handler
}

func NewAccountProviderRestCtrl() *accountProviderRestCtrl {
	var accountProviderUsecase AccountProviderUsecase = &accountProviderUsecase{}
	return &accountProviderRestCtrl{
		get: rest.NewGetHandler(accountProviderUsecase, accountProviderToRestDTO),
	}
}

func (c *accountProviderRestCtrl) BasePath() string {
	return "/" + ResourceTypeAccountProvider.String()
}

func (c *accountProviderRestCtrl) Endpoints() []*rest.Endpoint {
	return []*rest.Endpoint{
		rest.NewGetEndpoint(c.get),
	}
}
