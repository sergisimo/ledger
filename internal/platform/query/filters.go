package query

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
)

// --------------------------------------------------------------- Contract

type (
	Filters[T any] map[fields.Name]filter.FieldFilter[T]
)

const (
	FieldNameFilters fields.Name = "filters"
)

// --------------------------------------------------------------- Implementation

func (qf Filters[T]) Get(key fields.Name) filter.FieldFilter[T] {
	return qf[key]
}

func (qf Filters[T]) Exists(keys ...fields.Name) bool {
	if len(keys) < 1 {
		panic("exists called without any keys")
	}
	for _, k := range keys {
		if qf[k] == nil {
			return false
		}
	}
	return true
}

func (qf Filters[T]) Delete(key fields.Name) {
	if qf.Exists(key) {
		delete(qf, key)
	}
}

func (qf Filters[T]) Rename(oldKey, newKey fields.Name) {
	if !qf.Exists(oldKey) {
		return
	}

	qf[newKey] = filter.NewFieldFilter(qf[oldKey].Operator(), newKey, qf[oldKey].Value())
	qf.Delete(oldKey)
}

func (qf Filters[T]) String() string {
	strBuilder := strings.Builder{}
	filters := slices.Collect(maps.Values(qf))
	slices.SortFunc(filters, func(filter1, filter2 filter.FieldFilter[T]) int {
		return strings.Compare(filter1.Name().String(), filter2.Name().String())
	})

	for i, filter := range filters {
		fmt.Fprintf(&strBuilder, "%s %s %v", filter.Name(), filter.Operator(), filter.Value())
		if i < len(filters)-1 {
			strBuilder.WriteString(", ")
		}
	}

	return strBuilder.String()
}

func FilterBy(fieldName fields.Name, operator filter.Operator, val any) SrchOption {
	return func(srch *search) {
		if !operator.Valid() {
			return
		}
		if val == nil && operator != filter.OpIs && operator != filter.OpIsNot && operator != filter.OpEq {
			return
		}
		srch.filters[fieldName] = filter.NewFieldFilter(operator, fieldName, val)
	}
}
