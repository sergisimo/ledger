package authxtest

import (
	"context"
	"errors"
	"time"

	"github.com/sergisimo/ledger/internal/platform/authx"
)

const AnyToken = "valid-token"

var ErrInvalidToken = errors.New("invalid token")

func NewAuthenticator(ttl time.Duration) authx.Authenticator {
	return authx.AuthenticatorFunc(func(_ context.Context, token string) (authx.Identity, error) {
		if token != AnyToken {
			return authx.Identity{}, ErrInvalidToken
		}

		identity := authx.Identity{Subject: "test-subject"}
		if ttl > 0 {
			identity.ExpiresAt = time.Now().Add(ttl)
		}

		return identity, nil
	})
}
