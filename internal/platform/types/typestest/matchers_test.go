package typestest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/types/typestest"
)

func eqMatcher[T comparable](elem T) func(T) bool {
	return func(another T) bool {
		return elem == another
	}
}

func TestElementsEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		one     []string
		another []string
		want    bool
	}{
		{"both elems empty", nil, nil, true},
		{"first empty, second not empty", nil, []string{"a"}, false},
		{"no matches", []string{"b"}, []string{"a"}, false},
		{"matches same order", []string{"b", "c"}, []string{"b", "c"}, true},
		{"matches different order", []string{"b", "c"}, []string{"c", "b"}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(
				t, test.want,
				typestest.ElementsEqual(test.one, test.another),
			)
		})
	}
}

func TestElementsMatch(t *testing.T) {
	t.Parallel()

	type (
		innerStruct struct {
			val any
		}
		customStruct struct {
			str    string
			anInt  int
			aFloat float64
			inner  innerStruct
		}
	)
	tests := []struct {
		name   string
		first  []any
		second []any
		want   bool
	}{
		{"different types returns false", []any{1}, []any{1.0}, false},
		{"different length", []any{1}, []any{1, 3}, false},
		{"both empty length", nil, nil, true},
		{"elements match same order", []any{"a", "b", "c"}, []any{"a", "b", "c"}, true},
		{
			"elements match with structs same values different order",
			[]any{
				customStruct{"a", 1, 1.1, innerStruct{"b"}},
				customStruct{"b", 2, 2.2, innerStruct{"c"}},
				customStruct{"c", 3, 3.1, innerStruct{"d"}},
			},
			[]any{
				customStruct{"b", 2, 2.2, innerStruct{"c"}},
				customStruct{"a", 1, 1.1, innerStruct{"b"}},
				customStruct{"c", 3, 3.1, innerStruct{"d"}},
			},
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(
				t,
				test.want,
				typestest.ElementsMatch(eqMatcher, test.first...)(test.second...),
			)
		})
	}
}
