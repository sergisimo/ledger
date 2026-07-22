package types_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/types"
)

func TestUppercasedMarshalUnmarshal(t *testing.T) {
	t.Parallel()

	uppercasedPointer := func(val string) *types.Uppercased {
		v := types.Uppercased(val)

		return &v
	}

	type dto struct {
		UpperVal *types.Uppercased `json:"upper,omitempty"`
	}
	tests := []struct {
		name string
		in   *types.Uppercased
		want string
	}{
		{"nil", nil, "{}"},
		{"empty", uppercasedPointer(""), `{"upper":""}`},
		{"lower", uppercasedPointer("hello world"), `{"upper":"HELLO WORLD"}`},
		{"mixed", uppercasedPointer("heLLo worLd"), `{"upper":"HELLO WORLD"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			bs, err := json.Marshal(&dto{UpperVal: test.in})
			require.NoError(t, err)
			assert.Equal(t, test.want, string(bs))

			unmarshalled := new(dto)
			err = json.Unmarshal(bs, unmarshalled)
			require.NoError(t, err)
			if test.in == nil {
				require.Nil(t, unmarshalled.UpperVal)
				return
			}
			assert.Equal(t, strings.ToUpper(test.in.String()), unmarshalled.UpperVal.String())
		})
	}
}
