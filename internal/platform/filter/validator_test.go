package filter_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/filter"
)

type testType string

const (
	testTypeOne   testType = "one"
	testTypeTwo   testType = "two"
	testTypeThree testType = "three"
)

func TestAllValid(t *testing.T) {
	t.Parallel()

	fieldFilter := filter.NewFieldFilter[any](filter.OpEq, fields.NameID, "")

	tests := []struct {
		name    string
		in      []filter.ValidationFunc
		wantErr error
	}{
		{
			name: "one validation failing",
			in: []filter.ValidationFunc{
				filter.ValidateTyped[string],
				filter.ValidateValOneOf(""),
				filter.ValidateNotZero,
			},
			wantErr: fields.ErrZeroVal,
		},
		{
			name: "all validations pass",
			in: []filter.ValidationFunc{
				filter.ValidateTyped[string],
				filter.ValidateValOneOf(""),
			},
			wantErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			valFunc := filter.AllValid(test.in...)
			gotErr := valFunc(fieldFilter)
			assert.ErrorIs(t, gotErr, test.wantErr)
		})
	}
}

func TestAnyValid(t *testing.T) {
	t.Parallel()

	fieldFilter := filter.NewFieldFilter[any](filter.OpEq, fields.NameID, "1234")

	tests := []struct {
		name    string
		in      []filter.ValidationFunc
		wantErr error
	}{
		{
			name: "all validations fail",
			in: []filter.ValidationFunc{
				filter.ValidateTyped[int],
				filter.ValidateArrayField[string],
			},
			wantErr: fields.ErrInvalidType,
		},
		{
			name: "one validation passes",
			in: []filter.ValidationFunc{
				filter.ValidateTyped[int],
				filter.ValidateNotZero,
				filter.ValidateArrayField[string],
			},
			wantErr: nil,
		},
		{
			name: "all validations pass",
			in: []filter.ValidationFunc{
				filter.ValidateTyped[string],
				filter.ValidateNotZero,
			},
			wantErr: nil,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			valFunc := filter.AnyValid(testCase.in...)
			gotErr := valFunc(fieldFilter)
			assert.ErrorIs(t, gotErr, testCase.wantErr)
		})
	}
}

func TestValidateValOneOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		allowed       []testType
		inFieldFilter filter.FieldFilter[any]
		wantErr       error
	}{
		{
			name: "invalid type (not enum nor string) fails",
			allowed: []testType{
				testTypeOne, testTypeTwo,
			},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any(123)),
			wantErr:       fields.ErrInvalidType,
		},
		{
			name:          "values within allowed in the set",
			allowed:       []testType{testTypeOne, testTypeTwo},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any(testTypeOne)),
			wantErr:       nil,
		},
		{
			name:          "value not allowed in the set",
			allowed:       []testType{testTypeOne, testTypeTwo},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any(testTypeThree)),
			wantErr:       fields.ErrEnumValueNotAllowed,
		},
		{
			name:          "all values within allowed in the set using array",
			allowed:       []testType{testTypeOne, testTypeTwo, testTypeThree},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any([]testType{testTypeOne, testTypeThree})),
			wantErr:       nil,
		},
		{
			name:          "some values not allowed in the set using array",
			allowed:       []testType{testTypeOne, testTypeTwo},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any([]testType{testTypeOne, testTypeThree})),
			wantErr:       fields.ErrEnumValueNotAllowed,
		},
		{
			name:          "values within allowed in the set using string value",
			allowed:       []testType{testTypeOne, testTypeTwo, testTypeThree},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any("two")),
			wantErr:       nil,
		},
		{
			name:          "value not allowed in the set using string value",
			allowed:       []testType{testTypeOne, testTypeTwo},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any("three")),
			wantErr:       fields.ErrEnumValueNotAllowed,
		},
		{
			name:          "all values within allowed in the set using string array value",
			allowed:       []testType{testTypeOne, testTypeTwo, testTypeThree},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any([]string{"one", "three"})),
			wantErr:       nil,
		},
		{
			name:          "some values not allowed in the set using string array value",
			allowed:       []testType{testTypeOne, testTypeTwo},
			inFieldFilter: filter.NewFieldFilter(filter.OpEq, fields.NameKind, any([]string{"one", "three"})),
			wantErr:       fields.ErrEnumValueNotAllowed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			validate := filter.ValidateValOneOf(test.allowed...)
			err := validate(test.inFieldFilter)
			assert.ErrorIs(t, err, test.wantErr)
		})
	}
}

func TestValidateTyped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      filter.FieldFilter[any]
		wantErr error
	}{
		{
			name:    "if value is not a string, return error",
			in:      filter.NewFieldFilter[any](filter.OpEq, fields.NameID, 1),
			wantErr: fields.NewErrInvalidType(fields.NameID, "", 1),
		},
		{
			name:    "if value is an array of strings, return error",
			in:      filter.NewFieldFilter[any](filter.OpEq, fields.NameID, []string{"1", "2"}),
			wantErr: fields.NewErrInvalidType(fields.NameID, "", []string{"1", "2"}),
		},
		{
			name:    "if value is a string, return nil",
			in:      filter.NewFieldFilter[any](filter.OpEq, fields.NameID, "1"),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := filter.ValidateTyped[string](tt.in)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

type customTypeWithZeroChecker struct{}

func (ctwzc *customTypeWithZeroChecker) IsZero() bool {
	return ctwzc == nil
}

func TestFieldValidateNotZero(t *testing.T) {
	t.Parallel()

	var custom *customTypeWithZeroChecker

	tests := []struct {
		name       string
		val        any
		filterName fields.Name
		wantErr    error
	}{
		{
			name:       "zero val for number",
			val:        0,
			filterName: fields.NameID,
			wantErr:    fields.ErrZeroVal,
		},
		{
			name:       "zero val for string",
			val:        "",
			filterName: fields.NameName,
			wantErr:    fields.ErrZeroVal,
		},
		{
			name:       "empty string array",
			val:        []string{},
			filterName: fields.NameClient,
			wantErr:    fields.ErrZeroVal,
		},
		{
			name:       "string array with empty val",
			val:        []string{"123", ""},
			filterName: fields.NameConfig,
			wantErr:    fields.ErrZeroVal,
		},
		{
			name:       "custom type empty zero val",
			val:        custom,
			filterName: fields.NameConfig,
			wantErr:    fields.ErrZeroVal,
		},
		{
			name:       "not zero",
			val:        time.Now().UTC(),
			filterName: fields.NameCreationTime,
			wantErr:    nil,
		},
		{
			name:       "enum slice is zero",
			val:        []testType{},
			filterName: fields.NameKind,
			wantErr:    fields.ErrZeroVal,
		},
		{
			name:       "enum slice is not zero",
			val:        []testType{testTypeOne},
			filterName: fields.NameKind,
			wantErr:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			f := filter.NewFieldFilter(filter.OpEq, test.filterName, test.val)
			out := filter.ValidateNotZero(f)
			assert.ErrorIs(t, out, test.wantErr)
		})
	}
}

func TestValidateArrayField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      filter.FieldFilter[any]
		wantErr error
	}{
		{
			name:    "if value is not a []string, return error",
			in:      filter.NewFieldFilter[any](filter.OpEq, fields.NameID, "1"),
			wantErr: fields.NewErrInvalidType(fields.NameID, []string{}, "1"),
		},
		{
			name:    "if value is a []string, return nil",
			in:      filter.NewFieldFilter[any](filter.OpEq, fields.NameID, []string{"1", "2"}),
			wantErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := filter.ValidateArrayField[string](test.in)
			assert.ErrorIs(t, err, test.wantErr)
		})
	}
}

func TestValidateArrayOrSingleField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      filter.FieldFilter[any]
		wantErr error
	}{
		{
			name:    "if value is not a string or []string, return error",
			in:      filter.NewFieldFilter[any](filter.OpEq, fields.NameID, 1),
			wantErr: fields.NewErrInvalidType(fields.NameID, []string{}, 1),
		},
		{
			name:    "if value is a string, return nil",
			in:      filter.NewFieldFilter[any](filter.OpEq, fields.NameID, "1"),
			wantErr: nil,
		},
		{
			name:    "if value is a []string, return nil",
			in:      filter.NewFieldFilter[any](filter.OpEq, fields.NameID, []string{"1", "2"}),
			wantErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := filter.ValidateArrayOrSingleField[string](test.in)
			assert.ErrorIs(t, err, test.wantErr)
		})
	}
}

func runStringOrSliceValidationTests(
	t *testing.T,
	validateFunc func(filter.FieldFilter[any]) error,
	invalidValueErrMsg, validString string,
	validSlice []string,
) {
	t.Helper()

	tests := []struct {
		name    string
		in      any
		wantErr error
	}{
		{
			name:    "if value is not a string or []string, return error",
			in:      1,
			wantErr: fields.NewErrInvalidType(fields.NameID, "", 1),
		},
		{
			name:    "if value is a string but invalid, return error",
			in:      "abc",
			wantErr: fields.NewErrInvalidValue(fields.NameID, "abc", invalidValueErrMsg),
		},
		{
			name:    "if value is a []string but not all are valid, return error",
			in:      append([]string{validString}, append([]string{"abc"}, validSlice...)...),
			wantErr: fields.NewErrInvalidValue(fields.NameID, "abc", invalidValueErrMsg),
		},
		{
			name:    "if value is a string and valid, return nil",
			in:      validString,
			wantErr: nil,
		},
		{
			name:    "if value is a []string and all are valid, return nil",
			in:      validSlice,
			wantErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			f := filter.NewFieldFilter(filter.OpEq, fields.NameID, test.in)
			err := validateFunc(f)
			assert.ErrorIs(t, err, test.wantErr)
		})
	}
}

func TestValidateIntegerString(t *testing.T) {
	t.Parallel()

	runStringOrSliceValidationTests(t, filter.ValidateIntegerString, "invalid integer string", "123", []string{"123", "456"})
}

func TestValidateUUID(t *testing.T) {
	t.Parallel()

	runStringOrSliceValidationTests(
		t,
		filter.ValidateUUID,
		"invalid UUID length: 3",
		"c395b1fe-4551-442e-88db-b07fef539337",
		[]string{"c395b1fe-4551-442e-88db-b07fef539337", "d039fec7-2aff-48bf-af90-dc6942c7fbf8"},
	)
}

func TestValidateAllowedOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     filter.FieldFilter[any]
		allowedOps []filter.Operator
		wantErr    error
	}{
		{
			name:       "if filter contains allowed operator, return nil",
			filter:     filter.NewFieldFilter(filter.OpEq, fields.NameID, any("123")),
			allowedOps: []filter.Operator{filter.OpEq, filter.OpIn},
			wantErr:    nil,
		},
		{
			name:       "if filter contains not allowed operator, return error",
			filter:     filter.NewFieldFilter(filter.OpBetween, fields.NameID, any("123")),
			allowedOps: []filter.Operator{filter.OpEq, filter.OpIn},
			wantErr: fields.NewErrInvalidValue(
				fields.NameID,
				filter.OpBetween,
				fmt.Sprintf("operator is not within allowed set of operators: %v", []filter.Operator{filter.OpEq, filter.OpIn}),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := filter.ValidateAllowedOperators(tt.allowedOps...)(tt.filter)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestValidateLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  filter.FieldFilter[any]
		length  int
		wantErr error
	}{
		{
			name:    "if filter value length matches, return nil",
			filter:  filter.NewFieldFilter(filter.OpEq, fields.NameID, any([]string{"1", "2"})),
			length:  2,
			wantErr: nil,
		},
		{
			name:   "if filter value length does not match, return error",
			filter: filter.NewFieldFilter(filter.OpEq, fields.NameID, any([]string{"1", "2", "3"})),
			length: 2,
			wantErr: fields.NewErrInvalidValue(
				fields.NameID,
				[]string{"1", "2", "3"},
				fmt.Sprintf("length: %d does not match required length: %d", len([]string{"1", "2", "3"}), 2),
			),
		},
		{
			name:    "if filter value is not a slice or array, return error",
			filter:  filter.NewFieldFilter(filter.OpEq, fields.NameID, any("1")),
			length:  0,
			wantErr: fields.NewErrInvalidType(fields.NameID, []string{}, "1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := filter.ValidateLen[string](tt.length)(tt.filter)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestValidateDateTimePeriod(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	tests := []struct {
		name    string
		filter  filter.FieldFilter[any]
		wantErr error
	}{
		{
			name:    "if filter is not of type []time.Time, return error",
			filter:  filter.NewFieldFilter[any](filter.OpBetween, fields.NameCreationTime, "not-a-datetime"),
			wantErr: fields.NewErrInvalidType(fields.NameCreationTime, []time.Time{}, "not-a-datetime"),
		},
		{
			name:   "if filter does not use OpBetween, return error",
			filter: filter.NewFieldFilter[any](filter.OpEq, fields.NameCreationTime, []time.Time{now, now.Add(24 * time.Hour)}),
			wantErr: fields.NewErrInvalidValue(
				fields.NameCreationTime,
				filter.OpEq,
				fmt.Sprintf("operator is not within allowed set of operators: %v", []filter.Operator{filter.OpBetween}),
			),
		},
		{
			name:   "if filter value length is not 2, return error",
			filter: filter.NewFieldFilter[any](filter.OpBetween, fields.NameCreationTime, []time.Time{now}),
			wantErr: fields.NewErrInvalidValue(
				fields.NameCreationTime,
				[]time.Time{now},
				fmt.Sprintf("length: %d does not match required length: %d", len([]time.Time{now}), 2),
			),
		},
		{
			name:    "valid filter with []time.Time, return nil",
			filter:  filter.NewFieldFilter[any](filter.OpBetween, fields.NameCreationTime, []time.Time{now, now.Add(24 * time.Hour)}),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := filter.ValidateDateTimePeriod(tt.filter)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}
