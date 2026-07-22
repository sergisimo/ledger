package query

import (
	"github.com/sergisimo/ledger/internal/platform/fields"
)

type (
	PatchField interface {
		Value() any
		Operation() PatchFieldOperation
	}

	PatchFields map[fields.Name]PatchField

	PatchFieldOperation int32

	PatchQuery interface {
		SearchOpts() []SrchOption
		Fields() PatchFields
	}

	patchQuery struct {
		patchFields map[fields.Name]PatchField
		searchOpts  []SrchOption
	}

	PatchOption func(pq *patchQuery)
)

const (
	PatchFieldOperationSet PatchFieldOperation = iota
	PatchFieldOperationAdd
	PatchFieldOperationRemove
)
