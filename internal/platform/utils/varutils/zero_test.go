package varutils_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sergisimo/ledger/internal/platform/utils/varutils"
)

func TestZero(t *testing.T) {
	t.Parallel()

	type customStruct struct {
		A string
	}
	type customIface interface {
		A() string
	}

	assert.Empty(t, varutils.Zero[string]())
	assert.Nil(t, varutils.Zero[*string]())
	assert.Equal(t, customStruct{A: ""}, varutils.Zero[customStruct]())
	assert.Nil(t, varutils.Zero[*customStruct]())
	assert.Nil(t, varutils.Zero[any]())
	assert.Equal(t, time.Time{}, varutils.Zero[time.Time]())
	assert.Nil(t, varutils.Zero[customIface]())
}
