package varutils

type IsZeroChecker interface {
	IsZero() bool
}

func Zero[T any]() T {
	var zero T
	return zero
}
