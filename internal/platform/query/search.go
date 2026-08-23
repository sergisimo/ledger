package query

import (
	"github.com/sergisimo/ledger/internal/platform/fields"
)

// --------------------------------------------------------------- Contract

type (
	Search interface {
		Filters() Filters[any]
		Sorting() SortingParams
		Pagination() PaginationParams
		IncludedResourceObjects() IncludedResourceObject
	}

	IncludedResourceObject []fields.Name
)

const (
	FieldNameSearch   fields.Name = "search"
	FieldNameIncludes fields.Name = "includes"
)

// --------------------------------------------------------------- Implementation

type (
	search struct {
		filters                 Filters[any]
		sorting                 SortingParams
		pagination              PaginationParams
		includedResourceObjects IncludedResourceObject
	}

	SrchOption func(s *search)
)

func NewSearch(opts ...SrchOption) *search {
	srch := &search{
		filters:                 make(Filters[any]),
		sorting:                 &sortingParams{fields: make(map[fields.Name]SortingDir), keys: []fields.Name{}},
		pagination:              nil,
		includedResourceObjects: IncludedResourceObject{},
	}

	for _, opt := range opts {
		opt(srch)
	}

	return srch
}

func (s *search) Filters() Filters[any] {
	return s.filters
}

func (s *search) Sorting() SortingParams { //nolint:ireturn // struct also contains the interface for flexibility
	return s.sorting
}

func (s *search) Pagination() PaginationParams { //nolint:ireturn // struct also contains the interface for flexibility
	return s.pagination
}

func (s *search) IncludedResourceObjects() IncludedResourceObject {
	return s.includedResourceObjects
}
