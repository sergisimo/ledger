package fields_test

import (
	"testing"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestName_Merge(t *testing.T) {
	t.Parallel()

	merged := fields.Name("random").Merge("inner").Merge("field")
	expected := "random.inner.field"
	assert.Equal(t, expected, merged.String())
	assert.Equal(t, fields.Name(expected), merged)
}

func TestCast(t *testing.T) {
	t.Parallel()

	t.Run("invalid type", func(t *testing.T) {
		t.Parallel()

		v, err := fields.Cast[*string](fieldNameTest, 1)
		require.ErrorIs(t, err, fields.NewErrInvalidType(fieldNameTest, v, int(1)))
		assert.Nil(t, v)
	})

	t.Run("valid type", func(t *testing.T) {
		t.Parallel()

		v, err := fields.Cast[string](fieldNameTest, "test")
		require.NoError(t, err)
		assert.Equal(t, "test", v)
	})
}
