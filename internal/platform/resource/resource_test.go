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

func TestToProto(t *testing.T) {
	t.Parallel()

	var (
		deletedAt = time.Now().UTC().Truncate(time.Microsecond)
		r         = resourcetest.New(resourcetest.WithDeletedAt(&deletedAt))
		msg       = resource.ToProto(r)
	)

	assert.Equal(t, r.ID(), msg.GetId())
	assert.Equal(t, r.Type().String(), msg.GetType())
	assert.Equal(t, r.CreatedAt(), msg.GetCreatedAt().AsTime())
	assert.Equal(t, r.UpdatedAt(), msg.GetUpdatedAt().AsTime())
	assert.Equal(t, deletedAt, msg.GetDeletedAt().AsTime())
}

func TestToProtoWithoutDeletedAt(t *testing.T) {
	t.Parallel()

	msg := resource.ToProto(resourcetest.New())
	assert.Nil(t, msg.GetDeletedAt())
}

func TestFromProto(t *testing.T) {
	t.Parallel()

	var (
		deletedAt = time.Now().UTC().Truncate(time.Microsecond)
		r         = resourcetest.New(
			resourcetest.WithCreatedAt(time.Now().UTC().Truncate(time.Microsecond)),
			resourcetest.WithUpdatedAt(time.Now().UTC().Truncate(time.Microsecond)),
			resourcetest.WithDeletedAt(&deletedAt),
		)
		got = resource.FromProto(resource.ToProto(r))
	)

	assert.Implements(t, new(resource.Resource), got)
	resourcetest.AssertEqual(t, r, got)
}

func TestFromProtoWithoutDeletedAt(t *testing.T) {
	t.Parallel()

	got := resource.FromProto(resource.ToProto(resourcetest.New(
		resourcetest.WithCreatedAt(time.Now().UTC().Truncate(time.Microsecond)),
		resourcetest.WithUpdatedAt(time.Now().UTC().Truncate(time.Microsecond)),
	)))
	assert.Nil(t, got.DeletedAt())
}

func TestIndentifierToProto(t *testing.T) {
	t.Parallel()

	var (
		r   = resourcetest.New()
		msg = resource.IndentifierToProto(r)
	)

	assert.Equal(t, r.ID(), msg.GetId())
	assert.Equal(t, r.Type().String(), msg.GetType())
}

func TestIdentifierFromProto(t *testing.T) {
	t.Parallel()

	var (
		r   = resourcetest.New()
		got = resource.IdentifierFromProto(resource.IndentifierToProto(r))
	)

	assert.Implements(t, new(resource.Identifier), got)
	assert.Equal(t, r.ID(), got.ID())
	assert.Equal(t, r.Type(), got.Type())
}
