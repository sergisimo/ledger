package resource_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/sergisimo/ledger/internal/platform/resource/resourcetest"
)

func TestToRestDTO(t *testing.T) {

	var (
		r   = resourcetest.New()
		dto = resource.ToRestDTO(r)
	)

	assert.Implements(t, new(resource.Resource), &dto)
	resourcetest.AssertEqual(t, r, &dto)

	jsonBytes, err := json.Marshal(dto)
	require.NoError(t, err)
	expectedJson := fmt.Sprintf(
		`{"id":"%s","type":"%s","created_at":"%s","updated_at":"%s"}`,
		r.ID(),
		r.Type(),
		r.CreatedAt().Format(time.RFC3339Nano),
		r.UpdatedAt().Format(time.RFC3339Nano),
	)
	assert.JSONEq(t, expectedJson, string(jsonBytes))
}
