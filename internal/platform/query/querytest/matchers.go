package querytest

import (
	"reflect"
	"slices"

	"github.com/sergisimo/ledger/internal/platform/query"
)

// --------------------------------------------------------------- Search

func SrchOptMatcherFunc(want ...query.SrchOption) func([]query.SrchOption) bool {
	return func(got []query.SrchOption) bool {
		q1 := query.NewSearch(want...)
		q2 := query.NewSearch(got...)

		return equalFilters(q1.Filters(), q2.Filters()) &&
			equalSorting(q1.Sorting(), q2.Sorting()) &&
			equalPagination(q1.Pagination(), q2.Pagination()) &&
			equalLoad(q1, q2)
	}
}

func equalFilters(f1, f2 query.Filters[any]) bool {
	if len(f1) != len(f2) {
		return false
	}

	for k, v1 := range f1 {
		v2, ok := f2[k]
		if !ok {
			return false
		}

		if v1.Operator() != v2.Operator() || !reflect.DeepEqual(v1.Value(), v2.Value()) {
			return false
		}
	}

	return true
}

func equalSorting(s1, s2 query.SortingParams) bool {
	if s1 == nil && s2 == nil {
		return true
	}

	if len(s1.Keys()) != len(s2.Keys()) {
		return false
	}

	for _, k := range s1.Keys() {
		if s1.Get(k) != s2.Get(k) {
			return false
		}
	}

	return true
}

func equalPagination(p1, p2 query.PaginationParams) bool {
	if p1 == nil && p2 == nil {
		return true
	}

	if p1.Kind() != p2.Kind() {
		return false
	}

	switch p1.Kind() {
	case query.PaginationKindLimit:
		lp1, ok1 := p1.(query.LimitPagination)
		lp2, ok2 := p2.(query.LimitPagination)

		if !ok1 || !ok2 {
			return false
		}

		return lp1.Limit() == lp2.Limit() && lp1.Offset() == lp2.Offset()
	default:
		return false
	}
}

func equalLoad(s1, s2 query.Search) bool {
	if len(s1.Load()) != len(s2.Load()) {
		return false
	}

	for _, f := range s1.Load() {
		if !slices.Contains(s2.Load(), f) {
			return false
		}
	}

	return true
}

// --------------------------------------------------------------- Patch

func PatchOptMatcherFunc(want ...query.PatchOption) func([]query.PatchOption) bool {
	return func(got []query.PatchOption) bool {
		q1 := query.NewPatch(want...)
		q2 := query.NewPatch(got...)

		return SrchOptMatcherFunc(q1.SearchOpts()...)(q2.SearchOpts()) &&
			equalPatchFields(q1.Fields(), q2.Fields())
	}
}

func equalPatchFields(pf1, pf2 query.PatchFields) bool {
	if len(pf1) != len(pf2) {
		return false
	}

	for k, v1 := range pf1 {
		v2, ok := pf2[k]
		if !ok {
			return false
		}

		if v1.Operation() != v2.Operation() || !reflect.DeepEqual(v1.Value(), v2.Value()) {
			return false
		}
	}

	return true
}
