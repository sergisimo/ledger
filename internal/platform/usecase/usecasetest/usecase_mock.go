package usecasetest

import (
	"context"
	"testing"

	"github.com/sergisimo/ledger/internal/platform/query"
	"github.com/sergisimo/ledger/internal/platform/resource"
)

type (
	Usecase[R resource.Resource] struct {
		*Creator[R]
		*Getter[R]
		*Lister[R]
		*Patcher[R]
		*Deleter
	}
)

func New[R resource.Resource](t *testing.T) *Usecase[R] {
	return &Usecase[R]{
		Creator: NewCreator[R](t),
		Getter:  NewGetter[R](t),
		Lister:  NewLister[R](t),
		Patcher: NewPatcher[R](t),
		Deleter: NewDeleter(t),
	}
}

func (u *Usecase[R]) Create(ctx context.Context, v R) (R, error) {
	return u.Creator.Create(ctx, v)
}

func (u *Usecase[R]) Get(ctx context.Context, opts ...query.SrchOption) (R, error) {
	return u.Getter.Get(ctx, opts...)
}

func (u *Usecase[R]) List(ctx context.Context, opts ...query.SrchOption) (resource.List[R], error) {
	return u.Lister.List(ctx, opts...)
}

func (u *Usecase[R]) Patch(ctx context.Context, opts ...query.PatchOption) (R, error) {
	return u.Patcher.Patch(ctx, opts...)
}

func (u *Usecase[R]) Delete(ctx context.Context, delType query.DeleteType, opts ...query.SrchOption) error {
	return u.Deleter.Delete(ctx, delType, opts...)
}
