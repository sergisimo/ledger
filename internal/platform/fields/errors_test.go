package fields_test

import (
	"testing"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewErrWithChild(t *testing.T) {
	t.Parallel()

	err := fields.NewErrWithChild("reason", fields.ErrEnumValueNotAllowed)
	assert.Equal(t, "reason; err: enum value not allowed", err.Error())
	assert.ErrorIs(t, err, fields.ErrEnumValueNotAllowed)
}

func TestNewErrWithFieldName(t *testing.T) {
	t.Parallel()

	err := fields.NewErrWithFieldName(fields.NameID, fields.ErrInvalidType)
	var e fields.WithFieldNameError
	require.ErrorAs(t, err, &e)
	assert.Equal(t, fields.NameID, e.FieldName())
	assert.Equal(t, "error in field id: invalid type", err.Error())
	assert.ErrorIs(t, err, fields.ErrInvalidType)
}

func TestNewErrZeroVal(t *testing.T) {
	t.Parallel()

	err := fields.NewErrZeroVal(fields.NameID)
	assert.Equal(t, "error in field id: value is not allowed to be set with a zero val", err.Error())
	assert.ErrorIs(t, err, fields.ErrZeroVal)
}

func TestNewErrNil(t *testing.T) {
	t.Parallel()

	err := fields.NewErrNil(fields.NameID)
	assert.Equal(t, "error in field id: value cannot be nil", err.Error())
	assert.ErrorIs(t, err, fields.ErrNilVal)
}

func TestNewErrInvalidType(t *testing.T) {
	t.Parallel()

	err := fields.NewErrInvalidType(fields.NameID, "expected", 1)
	assert.Equal(t, "error in field id: value is not of the expected type: got int expected string; err: invalid type", err.Error())
	assert.ErrorIs(t, err, fields.ErrInvalidType)
}

func TestNewErrInvalidValue(t *testing.T) {
	t.Parallel()

	err := fields.NewErrInvalidValue(fields.NameID, "val", "reason")
	assert.Equal(t, "error in field id: value val is invalid because: reason; err: invalid value", err.Error())
	assert.ErrorIs(t, err, fields.ErrInvalidVal)
}

func TestNewErrInvalidEmptyString(t *testing.T) {
	t.Parallel()

	err := fields.NewErrInvalidEmptyString(fields.NameID)
	assert.Equal(t, "error in field id: cannot be an empty string; err: value is not allowed to be set with a zero val", err.Error())
	assert.ErrorIs(t, err, fields.ErrZeroVal)
}

func TestNewErrEnumValueNotAllowed(t *testing.T) {
	t.Parallel()

	err := fields.NewErrEnumValueNotAllowed(fields.NameID, testEnum("val"), testEnum("allowed1"), testEnum("allowed2"))
	assert.Equal(t, "error in field id: value val is not allowed, allowed values: [allowed1 allowed2]; err: enum value not allowed", err.Error())
	assert.ErrorIs(t, err, fields.ErrEnumValueNotAllowed)
}
