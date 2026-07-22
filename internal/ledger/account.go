package ledger

import "github.com/sergisimo/ledger/internal/platform/resource"

// --------------------------------------------------------------- Contract

type (
	Account interface {
		resource.Resource
	}

	AccountType string
)

const (
	AccountTypeUnknown  AccountType = "UNKNOWN"
	AccountTypeCash     AccountType = "CASH"
	AccountTypeSavings  AccountType = "SAVINGS"
	AccountTypeFixed    AccountType = "FIXED"
	AccountTypeVariable AccountType = "VARIABLE"
)
