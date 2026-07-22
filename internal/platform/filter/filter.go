package filter

import (
	"fmt"

	"github.com/sergisimo/ledger/internal/platform/fields"
)

type (
	Field[T any] interface {
		Value() T
		Name() fields.Name
		Update(val T)
	}

	FieldFilter[T any] interface {
		Field[T]
		Operator() Operator
	}

	Operator uint
)

const (
	FieldNameFilter  fields.Name = "filter"
	FieldNameFilters fields.Name = "filters"
)

const (
	OpUndefined Operator = iota
	OpEq
	OpNEq
	OpGT
	OpGTEq
	OpLT
	OpLTEq
	OpIn
	OpNotIn
	OpLike
	OpBetween
	OpContains
	OpNotContains
	OpContainsLike
	OpIs
	OpIsNot
)

func (op Operator) Valid() bool {
	return op == OpEq || op == OpNEq ||
		op == OpGT || op == OpGTEq ||
		op == OpLT || op == OpLTEq ||
		op == OpIn || op == OpNotIn || op == OpLike ||
		op == OpBetween || op == OpContains || op == OpNotContains ||
		op == OpContainsLike || op == OpIs ||
		op == OpIsNot
}

func (op Operator) String() string {
	switch op {
	case OpEq:
		return "eq"
	case OpNEq:
		return "neq"
	case OpGT:
		return "gt"
	case OpGTEq:
		return "gteq"
	case OpLT:
		return "lt"
	case OpLTEq:
		return "lteq"
	case OpIn:
		return "in"
	case OpNotIn:
		return "notin"
	case OpLike:
		return "like"
	case OpBetween:
		return "between"
	case OpContains:
		return "contains"
	case OpNotContains:
		return "notcontains"
	case OpContainsLike:
		return "containslike"
	case OpIs:
		return "is"
	case OpIsNot:
		return "isnot"
	default:
		panic(fmt.Sprintf("invalid operator: %d", op))
	}
}

type field[T any] struct {
	val  T
	name fields.Name
}

func (f field[T]) Name() fields.Name {
	return f.name
}

func (f field[T]) Value() T {
	return f.val
}

func (f *field[T]) Update(val T) {
	f.val = val
}

func newField[T any](name fields.Name, val T) Field[T] {
	if len(name) < 1 {
		panic("invalid filter name")
	}

	return &field[T]{
		name: name,
		val:  val,
	}
}

type fieldFilter[T any] struct {
	Field[T]

	operator Operator
}

func (f fieldFilter[T]) Operator() Operator {
	return f.operator
}

func NewFieldFilter[T any](operator Operator, name fields.Name, val T) FieldFilter[T] {
	if !operator.Valid() {
		panic(fmt.Sprintf("invalid operator: %v", operator))
	}
	f := &fieldFilter[T]{
		Field:    newField(name, val),
		operator: operator,
	}

	return f
}
