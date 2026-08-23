// Package setstest contains test helpers to work with set types in types/sets package.
package setstest

import (
	"cmp"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sergisimo/ledger/internal/platform/types/sets"
	"github.com/sergisimo/ledger/internal/platform/types/typestest"
)

type (
	sortable interface {
		cmp.Ordered
		comparable
	}
)

func Equal[T sortable](want, got sets.Set[T]) bool {
	if want == nil {
		return got == nil
	}
	if want.Len() != got.Len() {
		return false
	}
	var (
		wantVals = want.Values()
		gotVals  = got.Values()
	)
	_, isOrdered := want.(*sets.Ordered[T])
	if isOrdered {
		return slices.Equal(wantVals, gotVals)
	}

	return typestest.ElementsEqual(wantVals, gotVals)
}

func AssertEqual[T comparable](t *testing.T, expected, got sets.Set[T]) {
	t.Helper()

	if expected == nil {
		assert.Nil(t, got)
		return
	}
	assert.Equal(t, expected.Len(), got.Len())
	var (
		wantVals = expected.Values()
		gotVals  = got.Values()
	)
	_, isOrdered := expected.(*sets.Ordered[T])
	if isOrdered {
		assert.Equal(t, wantVals, gotVals)
		return
	}

	assert.ElementsMatch(t, wantVals, gotVals)
}
