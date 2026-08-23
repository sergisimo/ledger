package ledger

import (
	"github.com/sergisimo/ledger/internal/platform/gateway/rest"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Module(
		"ledger",
		rest.ControllerFx(NewAccountProviderRestCtrl),
	)
}
