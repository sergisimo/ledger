package varutils_test

import (
	"reflect"
	"testing"

	"github.com/sergisimo/ledger/internal/platform/utils/varutils"
	"github.com/stretchr/testify/assert"
)

func TestPtr(t *testing.T) {
	t.Parallel()

	got := varutils.Ptr(true)
	assert.IsType(t, reflect.Pointer, reflect.ValueOf(got).Kind())
}

type (
	Iface interface {
		Method() string
	}

	noImplements  struct{}
	implements    struct{}
	implementsPtr struct{}
)

func (i implements) Method() string {
	return "implemented"
}

func (i *implementsPtr) Method() string {
	return "implemented"
}

func TestMustImplementPtr(t *testing.T) {
	t.Parallel()

	assert.PanicsWithValue(t,
		"type: varutils_test.noImplements does not implement *varutils_test.Iface",
		func() { varutils.MustImplement[Iface, noImplements]() },
	)

	assert.NotPanics(t, func() { varutils.MustImplement[Iface, implements]() })
	assert.NotPanics(t, func() { varutils.MustImplement[Iface, *implementsPtr]() })
}
