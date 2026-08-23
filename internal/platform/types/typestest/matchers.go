package typestest

import (
	"slices"

	"github.com/sergisimo/ledger/internal/platform/types"
	"github.com/sergisimo/ledger/internal/platform/utils/sliceutils"
)

func ElementsMatch[T any](match func(T) func(T) bool, want ...T) func(...T) bool {
	return func(got ...T) bool {
		if len(want) != len(got) {
			return false
		}

		for _, elem := range want {
			found := false
			for _, another := range got {
				if match(elem)(another) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}

		return true
	}
}

func ElementsEqual[T comparable](want, got []T) bool {
	if len(want) != len(got) {
		return false
	}

	for _, elem := range want {
		if !slices.Contains(got, elem) {
			return false
		}
	}

	return true
}

func EnumElementsEqual[O types.Enum, A types.Enum](want []O, got []A) bool {
	return ElementsEqual(
		sliceutils.Map(want, func(v O) string {
			return v.String()
		}),
		sliceutils.Map(got, func(v A) string {
			return v.String()
		}),
	)
}
