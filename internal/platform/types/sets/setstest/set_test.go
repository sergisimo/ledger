package setstest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sergisimo/ledger/internal/platform/types/sets"
	"github.com/sergisimo/ledger/internal/platform/types/sets/setstest"
)

func TestEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		want     sets.Set[int]
		got      sets.Set[int]
		expected bool
	}{
		{
			name:     "both sets nil",
			want:     nil,
			got:      nil,
			expected: true,
		},
		{
			name:     "want nil got not nil",
			want:     nil,
			got:      sets.New(sets.With(1, 2, 3)),
			expected: false,
		},
		{
			name:     "unordered sets with same elements",
			want:     sets.New(sets.With(1, 2, 3)),
			got:      sets.New(sets.With(3, 2, 1)),
			expected: true,
		},
		{
			name:     "unordered sets with different elements",
			want:     sets.New(sets.With(1, 2, 3)),
			got:      sets.New(sets.With(1, 2, 4)),
			expected: false,
		},
		{
			name:     "unordered sets with different lengths",
			want:     sets.New(sets.With(1, 2, 3)),
			got:      sets.New(sets.With(1, 2)),
			expected: false,
		},
		{
			name:     "empty unordered sets",
			want:     sets.New[int](),
			got:      sets.New[int](),
			expected: true,
		},
		{
			name:     "ordered sets with same elements in same order",
			want:     sets.New(sets.With(1, 2, 3), sets.KeepOrder),
			got:      sets.New(sets.With(1, 2, 3), sets.KeepOrder),
			expected: true,
		},
		{
			name:     "ordered sets with same elements in different order",
			want:     sets.New(sets.With(1, 2, 3), sets.KeepOrder),
			got:      sets.New(sets.With(3, 2, 1), sets.KeepOrder),
			expected: false,
		},
		{
			name:     "ordered sets with different elements",
			want:     sets.New(sets.With(1, 2, 3), sets.KeepOrder),
			got:      sets.New(sets.With(1, 2, 4), sets.KeepOrder),
			expected: false,
		},
		{
			name:     "empty ordered sets",
			want:     sets.New[int](sets.KeepOrder),
			got:      sets.New[int](sets.KeepOrder),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := setstest.Equal(tt.want, tt.got)
			assert.Equal(t, tt.expected, got)
		})
	}
}
