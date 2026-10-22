package sqldb

import (
	"errors"
	"fmt"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
	"github.com/sergisimo/ledger/internal/platform/query"
)

var (
	ErrUnmappedField         = errors.New("field is not mapped to a database column")
	ErrUnsupportedOperator   = errors.New("operator is not supported by the sql repository")
	ErrUnsupportedPagination = errors.New("pagination kind is not supported by the sql repository")
	ErrInvalidFilterValue    = errors.New("filter value is not valid for the given operator")
	ErrEmptyPatch            = errors.New("patch contains no fields to update")
	ErrInvalidPatchValue     = errors.New("patch value is not valid for the given operation")
	ErrUnsupportedLocking    = errors.New("lock strength is not supported by the sql repository")
	ErrInvalidLocking        = errors.New("locking configuration is not valid")
	ErrLockingWithoutTx      = errors.New("a locking read requires a transaction in the context")
)

func NewErrUnsupportedLocking(strength query.LockStrength) error {
	return fields.NewErrWithFieldName(
		query.FieldNameLocking,
		fields.NewErrWithChild(fmt.Sprintf("strength %s", strength), ErrUnsupportedLocking),
	)
}

func NewErrInvalidLocking(reason string) error {
	return fields.NewErrWithFieldName(
		query.FieldNameLocking,
		fields.NewErrWithChild(reason, ErrInvalidLocking),
	)
}

func NewErrUnmappedField(fName fields.Name) error {
	return fields.NewErrWithFieldName(fName, ErrUnmappedField)
}

func NewErrUnsupportedOperator(fName fields.Name, op filter.Operator) error {
	return fields.NewErrWithFieldName(
		fName,
		fields.NewErrWithChild(fmt.Sprintf("operator %s", op), ErrUnsupportedOperator),
	)
}

func NewErrInvalidFilterValue(fName fields.Name, op filter.Operator, val any, reason string) error {
	return fields.NewErrWithFieldName(
		fName,
		fields.NewErrWithChild(
			fmt.Sprintf("value %v is invalid for operator %s because: %s", val, op, reason),
			ErrInvalidFilterValue,
		),
	)
}

func NewErrInvalidPatchValue(fName fields.Name, op query.PatchFieldOperation, val any, reason string) error {
	return fields.NewErrWithFieldName(
		fName,
		fields.NewErrWithChild(
			fmt.Sprintf("value %v is invalid for operation %s because: %s", val, op, reason),
			ErrInvalidPatchValue,
		),
	)
}

func NewErrUnsupportedPatchOperation(fName fields.Name, op query.PatchFieldOperation) error {
	return fields.NewErrWithFieldName(
		fName,
		fields.NewErrWithChild(fmt.Sprintf("operation %s", op), ErrUnsupportedOperator),
	)
}

func NewErrUnsupportedPagination(kind query.PaginationKind) error {
	return fields.NewErrWithFieldName(
		query.FieldNamePagination,
		fields.NewErrWithChild(fmt.Sprintf("kind %d", kind), ErrUnsupportedPagination),
	)
}
