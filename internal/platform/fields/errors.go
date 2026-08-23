package fields

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrZeroVal             = errors.New("value is not allowed to be set with a zero val")
	ErrNilVal              = errors.New("value cannot be nil")
	ErrInvalidType         = errors.New("invalid type")
	ErrInvalidVal          = errors.New("invalid value")
	ErrEnumValueNotAllowed = errors.New("enum value not allowed")
)

type WithChildError struct {
	reason   string
	childErr error
}

func (err WithChildError) Error() string {
	return fmt.Sprintf("%s; err: %v", err.reason, err.childErr)
}

func (err WithChildError) Unwrap() error {
	return err.childErr
}

func NewErrWithChild(reason string, childErr error) error {
	return WithChildError{
		reason:   reason,
		childErr: childErr,
	}
}

type WithFieldNameError struct {
	fieldName Name
	childErr  error
}

func (err WithFieldNameError) FieldName() Name {
	return err.fieldName
}

func (err WithFieldNameError) Error() string {
	return fmt.Sprintf("error in field %s: %v", err.fieldName, err.childErr)
}

func (err WithFieldNameError) Unwrap() error {
	return err.childErr
}

func NewErrWithFieldName(fieldName Name, err error) error {
	return WithFieldNameError{
		fieldName: fieldName,
		childErr:  err,
	}
}

func NewErrZeroVal(fieldName Name) error {
	return NewErrWithFieldName(fieldName, ErrZeroVal)
}

func NewErrNil(fieldName Name) error {
	return NewErrWithFieldName(fieldName, ErrNilVal)
}

func NewErrInvalidType(fName Name, expected, got any) error {
	return NewErrWithFieldName(
		fName,
		NewErrWithChild(
			fmt.Sprintf("value is not of the expected type: got %s expected %s", reflect.TypeOf(got), reflect.TypeOf(expected)),
			ErrInvalidType,
		),
	)
}

func NewErrInvalidValue(fName Name, val any, reason string) error {
	return NewErrWithFieldName(
		fName,
		NewErrWithChild(
			fmt.Sprintf("value %v is invalid because: %s", val, reason),
			ErrInvalidVal,
		),
	)
}

func NewErrInvalidEmptyString(field Name) error {
	return NewErrWithFieldName(
		field,
		NewErrWithChild(
			"cannot be an empty string",
			ErrZeroVal,
		),
	)
}

func NewErrEnumValueNotAllowed[T ~string](field Name, val T, allowedVals ...T) error {
	return NewErrWithFieldName(
		field,
		NewErrWithChild(
			fmt.Sprintf("value %s is not allowed, allowed values: %v", val, allowedVals),
			ErrEnumValueNotAllowed,
		),
	)
}
