package filter_test

import (
	"testing"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"

	"github.com/stretchr/testify/assert"
)

func TestNewFieldFilter(t *testing.T) {
	t.Parallel()

	t.Run("invalid operator", func(t *testing.T) {
		t.Parallel()

		assert.Panics(t, func() { filter.NewFieldFilter(filter.OpUndefined, fields.NameID, "Aitor Menta") })
	})

	t.Run("invalid name", func(t *testing.T) {
		t.Parallel()

		assert.Panics(t, func() { filter.NewFieldFilter(filter.OpEq, "", "Aitor Menta") })
	})

	t.Run("valid field filter", func(t *testing.T) {
		t.Parallel()

		ff := filter.NewFieldFilter(filter.OpEq, fields.NameEmail, "aitormenta@myguestbot.com")
		assert.Equal(t, filter.OpEq, ff.Operator())
		assert.Equal(t, fields.NameEmail, ff.Name())
		assert.Equal(t, "aitormenta@myguestbot.com", ff.Value())
		ff.Update("benitocamela@myguestbot.com")
		assert.Equal(t, "benitocamela@myguestbot.com", ff.Value())
	})
}
