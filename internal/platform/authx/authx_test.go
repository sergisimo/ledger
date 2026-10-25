package authx_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sergisimo/ledger/internal/platform/authx"
	"github.com/sergisimo/ledger/internal/platform/authx/authxtest"
)

func TestIdentityExpired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity authx.Identity
		want     bool
	}{
		{
			name:     "no expiry never expires",
			identity: authx.Identity{},
			want:     false},
		{
			name:     "expiry in the future",
			identity: authx.Identity{ExpiresAt: time.Now().Add(time.Hour)},
			want:     false,
		},
		{
			name:     "expiry in the past",
			identity: authx.Identity{ExpiresAt: time.Now().Add(-time.Hour)},
			want:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, test.identity.Expired())
		})
	}
}

func TestAuthenticatorFunc(t *testing.T) {
	t.Parallel()

	authenticator := authxtest.NewAuthenticator(time.Hour)

	identity, err := authenticator.Authenticate(t.Context(), authxtest.AnyToken)
	require.NoError(t, err)
	assert.Equal(t, "test-subject", identity.Subject)
	assert.False(t, identity.Expired())

	_, err = authenticator.Authenticate(t.Context(), "other")
	require.ErrorIs(t, err, authxtest.ErrInvalidToken)
}
