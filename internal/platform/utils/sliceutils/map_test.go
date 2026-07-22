package sliceutils_test

import (
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	"github.com/sergisimo/ledger/internal/platform/utils/sliceutils"
)

func TestMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		in       []uint
		expected []string
	}{
		{
			name:     "integer to string conversion",
			in:       []uint{1, 2, 3},
			expected: []string{"1", "2", "3"},
		},
		{
			name:     "empty slice",
			in:       []uint{},
			expected: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			out := sliceutils.Map(
				test.in,
				func(val uint) string {
					return strconv.FormatUint(uint64(val), 10)
				},
			)
			assert.True(t, cmp.Equal(test.expected, out))
		})
	}
}
