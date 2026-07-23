package resourcetest

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/sergisimo/ledger/internal/platform/resource"
	"github.com/stretchr/testify/assert"
)

// --------------------------------------------------------------- Assertion

func AssertEqual(t *testing.T, want, got resource.Resource) {
	t.Helper()

	assert.Equal(t, want.ID(), got.ID())
	assert.Equal(t, want.Type(), got.Type())
	assert.Equal(t, want.CreatedAt(), got.CreatedAt())
	assert.Equal(t, want.UpdatedAt(), got.UpdatedAt())
	assert.Equal(t, want.DeletedAt(), got.DeletedAt())
}

// --------------------------------------------------------------- Stub

type (
	resourceStub struct {
		id        string
		kind      resource.Type
		createdAt time.Time
		updatedAt time.Time
		deletedAt *time.Time
	}

	resourceOption func(*resourceStub)
)

func defaultOpts() []resourceOption {
	return []resourceOption{
		WithRandomSerialID(),
		WithType("stub"),
		WithCreatedAt(time.Now().UTC()),
		WithUpdatedAt(time.Now().UTC()),
	}
}

func New(opts ...resourceOption) *resourceStub {
	r := &resourceStub{}

	for _, opt := range append(defaultOpts(), opts...) {
		opt(r)
	}

	return r
}

func WithRandomSerialID() resourceOption {
	return func(r *resourceStub) {
		const Serial4Limit = 2147483647 - 1
		r.id = strconv.Itoa(rand.Intn(Serial4Limit))
	}
}

func WithID(id string) resourceOption {
	return func(r *resourceStub) {
		r.id = id
	}
}

func WithType(kind resource.Type) resourceOption {
	return func(r *resourceStub) {
		r.kind = kind
	}
}

func WithCreatedAt(createdAt time.Time) resourceOption {
	return func(r *resourceStub) {
		r.createdAt = createdAt
	}
}

func WithUpdatedAt(updatedAt time.Time) resourceOption {
	return func(r *resourceStub) {
		r.updatedAt = updatedAt
	}
}

func WithDeletedAt(deletedAt *time.Time) resourceOption {
	return func(r *resourceStub) {
		r.deletedAt = deletedAt
	}
}

func (r *resourceStub) ID() string {
	return r.id
}

func (r *resourceStub) Type() resource.Type {
	return r.kind
}

func (r *resourceStub) CreatedAt() time.Time {
	return r.createdAt
}

func (r *resourceStub) UpdatedAt() time.Time {
	return r.updatedAt
}

func (r *resourceStub) DeletedAt() *time.Time {
	return r.deletedAt
}
