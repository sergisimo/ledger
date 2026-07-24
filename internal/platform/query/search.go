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
		Load() []fields.Name
	}
)

const (
	FieldNameSearch fields.Name = "search"
)

// --------------------------------------------------------------- Implementation

type (
	search struct {
		filters    Filters[any]
		sorting    SortingParams
		pagination PaginationParams
		load       []fields.Name
	}

	SrchOption func(s *search)
)

func NewSearch(opts ...SrchOption) *search {
	srch := &search{
		filters:    make(Filters[any]),
		sorting:    &sortingParams{fields: make(map[fields.Name]SortingDir), keys: []fields.Name{}},
		pagination: nil,
		load:       []fields.Name{},
	}

	for _, opt := range opts {
		opt(srch)
	}

	return srch
}

func LoadRelated(fields ...fields.Name) SrchOption {
	return func(s *search) {
		s.load = append(s.load, fields...)
	}
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

func (s *search) Load() []fields.Name {
	return s.load
}
