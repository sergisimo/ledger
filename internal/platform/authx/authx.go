package authx

import (
	"context"
	"time"
)

// --------------------------------------------------------------- Contract

type (
	Identity struct {
		Subject   string
		ExpiresAt time.Time
		Attrs     map[string]any
	}

	Authenticator interface {
		Authenticate(ctx context.Context, token string) (Identity, error)
	}

	AuthenticatorFunc func(ctx context.Context, token string) (Identity, error)
)

// --------------------------------------------------------------- Implementation

func (f AuthenticatorFunc) Authenticate(ctx context.Context, token string) (Identity, error) {
	return f(ctx, token)
}

func (i Identity) Expired() bool {
	return !i.ExpiresAt.IsZero() && !i.ExpiresAt.After(time.Now())
}
