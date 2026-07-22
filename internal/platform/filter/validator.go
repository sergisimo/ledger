package filter

import (
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/utils/varutils"
)

type ValidationFunc func(f FieldFilter[any]) error

func AllValid(fs ...ValidationFunc) ValidationFunc {
	return func(f FieldFilter[any]) error {
		for _, fv := range fs {
			err := fv(f)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func AnyValid(fs ...ValidationFunc) ValidationFunc {
	return func(f FieldFilter[any]) error {
		var err error
		for _, fv := range fs {
			err = fv(f)
			if err == nil {
				break
			}
		}

		return err
	}
}

func ValidateValOneOf[T ~string](allowedVals ...T) ValidationFunc {
	return func(f FieldFilter[any]) error {
		switch reflect.ValueOf(f.Value()).Kind() {
		case reflect.Slice, reflect.Array:
			return validateSliceOneOf(f, allowedVals...)
		default:
			return ValidateSingleOneOf(allowedVals...)(f)
		}
	}
}

func validateSliceOneOf[T ~string](filter FieldFilter[any], allowedVals ...T) error {
	if values, ok := filter.Value().([]T); ok {
		for _, v := range values {
			if !slices.Contains(allowedVals, v) {
				return fields.NewErrEnumValueNotAllowed(filter.Name(), v, allowedVals...)
			}
		}
		return nil
	}

	if strValues, ok := filter.Value().([]string); ok {
		return validateStringSliceConversion(filter, strValues, allowedVals...)
	}

	var zeroVal T
	return fields.NewErrInvalidType(filter.Name(), zeroVal, filter.Value())
}

func validateStringSliceConversion[T ~string](filter FieldFilter[any], strValues []string, allowedVals ...T) error {
	var zeroVal T
	enumType := reflect.TypeOf(zeroVal)

	if enumType.Kind() == reflect.String {
		for _, strVal := range strValues {
			converted := reflect.ValueOf(strVal).Convert(enumType).Interface()
			convertedVal, ok := converted.(T)
			if !ok {
				return fields.NewErrInvalidType(filter.Name(), zeroVal, filter.Value())
			}
			if !slices.Contains(allowedVals, convertedVal) {
				return fields.NewErrEnumValueNotAllowed(filter.Name(), convertedVal, allowedVals...)
			}
		}
		return nil
	}

	return fields.NewErrInvalidType(filter.Name(), zeroVal, filter.Value())
}

func ValidateSingleOneOf[T ~string](allowedVals ...T) ValidationFunc {
	return func(filter FieldFilter[any]) error {
		if value, ok := filter.Value().(T); ok {
			if !slices.Contains(allowedVals, value) {
				return fields.NewErrEnumValueNotAllowed(filter.Name(), value, allowedVals...)
			}
			return nil
		}

		if strValue, ok := filter.Value().(string); ok {
			return validateStringConversion(filter, strValue, allowedVals...)
		}

		var zeroVal T
		return fields.NewErrInvalidType(filter.Name(), zeroVal, filter.Value())
	}
}

func validateStringConversion[T ~string](filter FieldFilter[any], strValue string, allowedVals ...T) error {
	var zeroVal T
	enumType := reflect.TypeOf(zeroVal)

	if enumType.Kind() == reflect.String {
		converted := reflect.ValueOf(strValue).Convert(enumType).Interface()
		convertedVal, ok := converted.(T)
		if !ok {
			return fields.NewErrInvalidType(filter.Name(), zeroVal, converted)
		}
		if !slices.Contains(allowedVals, convertedVal) {
			return fields.NewErrEnumValueNotAllowed(filter.Name(), convertedVal, allowedVals...)
		}
		return nil
	}

	return fields.NewErrInvalidType(filter.Name(), zeroVal, filter.Value())
}

func ValidateTyped[T any](filter FieldFilter[any]) error {
	_, ok := filter.Value().(T)
	if !ok {
		var zeroVal T
		return fields.NewErrInvalidType(filter.Name(), zeroVal, filter.Value())
	}

	return nil
}

func ValidateNotZero(filter FieldFilter[any]) error {
	switch value := filter.Value().(type) {
	case uint, uint8, uint16, uint32, uint64,
		int, int8, int16, int32, int64,
		float32, float64:
		if value == 0 {
			return fields.NewErrZeroVal(filter.Name())
		}
	case string:
		if len(value) < 1 {
			return fields.NewErrZeroVal(filter.Name())
		}
	case []string:
		if len(value) < 1 {
			return fields.NewErrZeroVal(filter.Name())
		}
		for _, val := range value {
			if len(val) < 1 {
				return fields.NewErrZeroVal(filter.Name())
			}
		}
	case varutils.IsZeroChecker:
		if value.IsZero() {
			return fields.NewErrZeroVal(filter.Name())
		}
	default:
		valueRef := reflect.ValueOf(filter.Value())
		//nolint:exhaustive // we only need to check for slices
		switch valueRef.Kind() {
		case reflect.Slice, reflect.Array:
			if valueRef.Len() < 1 {
				return fields.NewErrZeroVal(filter.Name())
			}
		}
	}

	return nil
}

func ValidateArrayField[T any](filter FieldFilter[any]) error {
	switch reflect.ValueOf(filter.Value()).Kind() {
	case reflect.Slice, reflect.Array:
		if _, ok := filter.Value().([]T); !ok {
			var zeroVal []T
			return fields.NewErrInvalidType(filter.Name(), zeroVal, filter.Value())
		}
	default:
		var zeroVal []T
		return fields.NewErrInvalidType(filter.Name(), zeroVal, filter.Value())
	}
	return nil
}

func ValidateArrayOrSingleField[T any](f FieldFilter[any]) error {
	return AnyValid(ValidateTyped[T], ValidateArrayField[T])(f)
}

func validateStringOrSlice(filter FieldFilter[any], validator func(string) error) error {
	switch v := filter.Value().(type) {
	case string:
		return validator(v)
	case []string:
		for _, val := range v {
			err := validator(val)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		return fields.NewErrInvalidType(filter.Name(), "", filter.Value())
	}
}

func ValidateIntegerString(filter FieldFilter[any]) error {
	return validateStringOrSlice(filter, fields.IntValidator(filter.Name()))
}

func ValidateUUID(filter FieldFilter[any]) error {
	return validateStringOrSlice(filter, fields.UUIDValidator(filter.Name()))
}

func ValidateAllowedOperators(allowedOps ...Operator) ValidationFunc {
	return func(f FieldFilter[any]) error {
		if !slices.Contains(allowedOps, f.Operator()) {
			return fields.NewErrInvalidValue(
				f.Name(),
				f.Operator(),
				fmt.Sprintf("operator is not within allowed set of operators: %v", allowedOps),
			)
		}

		return nil
	}
}

func ValidateLen[T any](length int) ValidationFunc {
	return func(filter FieldFilter[any]) error {
		if arrayVal, ok := filter.Value().([]T); ok {
			if len(arrayVal) != length {
				return fields.NewErrInvalidValue(
					filter.Name(),
					filter.Value(),
					fmt.Sprintf("length: %d does not match required length: %d", len(arrayVal), length),
				)
			}
			return nil
		}
		return fields.NewErrInvalidType(filter.Name(), []T{}, filter.Value())
	}
}

func ValidateDateTimePeriod(filter FieldFilter[any]) error {
	const betweenOperatorArrayLen = 2

	switch filter.Value().(type) {
	case []time.Time:
		return AllValid(ValidateAllowedOperators(OpBetween), ValidateLen[time.Time](betweenOperatorArrayLen))(filter)
	default:
		return fields.NewErrInvalidType(filter.Name(), []time.Time{}, filter.Value())
	}
}
