package fields

import (
	"context"
	"fmt"
	"net/mail"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sergisimo/ledger/internal/platform/types"
	"github.com/sergisimo/ledger/internal/platform/types/sets"
	"github.com/sergisimo/ledger/internal/platform/utils/networkutils"
)

// --------------------------------------------------------------- Contract

type Validator[T any] func(T) error

// --------------------------------------------------------------- Validators

func ValidateAny[T any](fName Name, v any, validator Validator[T]) error {
	t, err := Cast[T](fName, v)
	if err != nil {
		return err
	}
	return validator(t)
}

func EmptyStringValidator(fName Name) Validator[string] {
	return func(val string) error {
		if val != "" {
			return NewErrInvalidValue(fName, val, "should be empty string")
		}
		return nil
	}
}

func NotEmptyStringValidator(fName Name) Validator[string] {
	return func(val string) error {
		if len(val) < 1 {
			return NewErrInvalidEmptyString(fName)
		}
		return nil
	}
}

func StringMinLengthValidator(fName Name, minLength int) Validator[string] {
	return func(val string) error {
		if len(val) < minLength {
			return NewErrInvalidValue(fName, val, fmt.Sprintf("must have at least %d chars", minLength))
		}
		return nil
	}
}

func RegexpValidator(fName Name, matchPattern string) Validator[string] {
	return func(val string) error {
		match, err := regexp.MatchString(matchPattern, val)
		if err != nil || !match {
			return NewErrInvalidValue(fName, val, "must match: "+matchPattern)
		}
		return nil
	}
}

func UUIDValidator(fName Name) Validator[string] {
	return func(s string) error {
		_, err := uuid.Parse(s)
		if err != nil {
			return NewErrInvalidValue(fName, s, err.Error())
		}

		return nil
	}
}

func URLValidator(fName Name) Validator[string] {
	return func(s string) error {
		_, err := url.ParseRequestURI(s)
		if err != nil {
			return NewErrInvalidValue(fName, s, "invalid url")
		}

		return nil
	}
}

func NotNilValidator(fName Name) Validator[any] {
	return func(val any) error {
		if isNil(val) {
			return NewErrNil(fName)
		}

		return nil
	}
}

func NilValidator(fName Name) Validator[any] {
	return func(val any) error {
		if !isNil(val) {
			return NewErrInvalidValue(fName, val, "should be nil")
		}

		return nil
	}
}

func isNil(val any) bool {
	if val == nil {
		return true
	}

	switch reflect.TypeOf(val).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(val).IsNil()
	default:
		return false
	}
}

func EnumValidator[T ~string](fName Name, allowedVals ...T) Validator[T] {
	return func(value T) error {
		if slices.Contains(allowedVals, value) {
			return nil
		}
		return NewErrEnumValueNotAllowed(fName, value, allowedVals...)
	}
}

func IntValidator(fName Name) Validator[string] {
	return func(s string) error {
		_, err := strconv.Atoi(s)
		if err != nil {
			return NewErrInvalidValue(fName, s, "invalid integer string")
		}
		return nil
	}
}

func NotEmptySliceValidator[T any](fName Name) Validator[[]T] {
	return func(val []T) error {
		if len(val) < 1 {
			return NewErrNil(fName)
		}
		return nil
	}
}

func NotEmptySetValidator[T comparable](fName Name) Validator[sets.Set[T]] {
	return func(s sets.Set[T]) error {
		if s == nil || s.Len() == 0 {
			return NewErrNil(fName)
		}

		return nil
	}
}

func EmptySliceValidator[T any](fName Name) Validator[[]T] {
	return func(val []T) error {
		if len(val) > 0 {
			return NewErrInvalidValue(fName, val, "should be empty")
		}
		return nil
	}
}

func SliceValidator[T any](validator Validator[T]) Validator[[]T] {
	return func(t []T) error {
		for _, v := range t {
			err := validator(v)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func SetEnumValuesValidator[T types.Enum](fName Name, vals ...T) Validator[sets.Set[T]] {
	return func(s sets.Set[T]) error {
		return SliceValidator(EnumValidator(fName, vals...))(s.Values())
	}
}

func TimeFmtValidator(fName Name, dateFmt string) Validator[string] {
	return func(s string) error {
		_, err := time.Parse(dateFmt, s)
		if err != nil {
			return NewErrInvalidValue(fName, s, "invalid date string for format "+dateFmt)
		}
		return nil
	}
}

func EmailsValidator(fName Name) Validator[[]string] {
	return func(s []string) error {
		for _, email := range s {
			return EmailValidator(fName)(email)
		}
		return nil
	}
}

func EmailValidator(fName Name) Validator[string] {
	return func(s string) error {
		_, err := mail.ParseAddress(s)
		if err != nil {
			return NewErrInvalidValue(fName, s, "invalid email address")
		}
		return nil
	}
}

func DomainValidator(ctx context.Context, resolver networkutils.DomainResolver) Validator[string] {
	return func(domain string) error {
		err := NotEmptyStringValidator(NameDomain)(domain)
		if err != nil {
			return err
		}

		addrs, err := resolver.LookupHost(ctx, domain)
		if err != nil || len(addrs) == 0 {
			return NewErrInvalidValue(NameDomain, domain, "invalid domain")
		}
		return nil
	}
}
