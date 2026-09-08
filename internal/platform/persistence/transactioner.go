package persistence

import "context"

type (
	TxFunc func(txCtx context.Context) error

	txKey struct{}
)

type Transactioner interface {
	Exec(ctx context.Context, f TxFunc) error
}

func InjectTx(ctx context.Context, tx any) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func TxFromCtx[T any](ctx context.Context) (T, bool) {
	if val := ctx.Value(txKey{}); val == nil {
		var zero T
		return zero, false
	}

	return ctx.Value(txKey{}).(T), true
}

func WithoutTx(ctx context.Context) context.Context {
	return context.WithValue(ctx, txKey{}, nil)
}
