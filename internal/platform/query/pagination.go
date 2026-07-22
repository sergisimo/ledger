package query

import (
	"fmt"

	"golang.org/x/exp/constraints"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

// --------------------------------------------------------------- Contract

type (
	PaginationParams interface {
		fmt.Stringer
		Kind() PaginationKind
	}

	LimitPagination interface {
		PaginationParams
		Limit() uint
		Offset() uint
	}

	PaginationKind int
)

const (
	FieldNamePagination fields.Name = "pagination"
)

const (
	PaginationKindUndefined PaginationKind = iota
	PaginationKindLimit
)

// --------------------------------------------------------------- Implementation

type limitPagination struct {
	limit  uint
	offset uint
}

func (lp *limitPagination) Kind() PaginationKind {
	return PaginationKindLimit
}

func (lp *limitPagination) Limit() uint {
	return lp.limit
}

func (lp *limitPagination) Offset() uint {
	return lp.offset
}

func (lp *limitPagination) String() string {
	return fmt.Sprintf("limit=%d, offset=%d", lp.limit, lp.offset)
}

func Pagination[L constraints.Integer](limit, offset L) SrchOption {
	return func(s *search) {
		s.pagination = &limitPagination{
			limit:  uint(limit),
			offset: uint(offset),
		}
	}
}
