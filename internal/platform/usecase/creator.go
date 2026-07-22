package usecase

import "context"

// --------------------------------------------------------------- Contract

type Creator[R any] interface {
	Create(context.Context, R) (R, error)
}
