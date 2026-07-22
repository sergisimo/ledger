package query

import (
	"fmt"
	"strings"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

// --------------------------------------------------------------- Contract

type (
	SortingParams interface {
		fmt.Stringer
		Get(field fields.Name) SortingDir
		Set(field fields.Name, dir SortingDir)
		Keys() []fields.Name
	}

	SortingDir int32
)

const (
	FieldNameSorting fields.Name = "sorting"
)

const (
	SortDirUndefined SortingDir = iota
	SortAsc
	SortDesc
)

// --------------------------------------------------------------- Implementation

func (sd SortingDir) Valid() bool {
	return sd == SortAsc || sd == SortDesc
}

func (sd SortingDir) String() string {
	switch sd {
	case SortAsc:
		return "ASC"
	case SortDesc:
		return "DESC"
	case SortDirUndefined:
		return ""
	default:
		return ""
	}
}

type sortingParams struct {
	fields map[fields.Name]SortingDir
	keys   []fields.Name
}

func (sp *sortingParams) Get(field fields.Name) SortingDir {
	if dir, ok := sp.fields[field]; ok {
		return dir
	}
	return SortDirUndefined
}

func (sp *sortingParams) Set(field fields.Name, dir SortingDir) {
	if _, exists := sp.fields[field]; !exists {
		sp.keys = append(sp.keys, field)
	}
	sp.fields[field] = dir
}

func (sp *sortingParams) Keys() []fields.Name {
	return sp.keys
}

func (sp *sortingParams) String() string {
	strBuilder := strings.Builder{}
	for i, key := range sp.keys {
		strBuilder.WriteString(fmt.Sprintf("%s=%s", key, sp.fields[key]))
		if i < len(sp.keys)-1 {
			strBuilder.WriteString(", ")
		}
	}
	return strBuilder.String()
}

func SortBy(field fields.Name, dir SortingDir) SrchOption {
	return func(srch *search) {
		if srch.sorting == nil {
			srch.sorting = &sortingParams{
				fields: make(map[fields.Name]SortingDir),
				keys:   []fields.Name{},
			}
		}

		srch.sorting.Set(field, dir)
	}
}
