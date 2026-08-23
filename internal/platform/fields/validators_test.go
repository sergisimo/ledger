package fields_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/types/sets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEnum string

func (t testEnum) String() string {
	return string(t)
}

const fieldNameTest fields.Name = "test"

func TestValidateAny(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		value       any
		expectedErr error
	}{
		{
			name:        "invalid type",
			value:       1,
			expectedErr: fields.NewErrInvalidType(fieldNameTest, "1", int(1)),
		},
		{
			name:        "valid type",
			value:       "",
			expectedErr: fields.NewErrInvalidEmptyString(fieldNameTest),
		},
		{
			name:        "valid type with no error",
			value:       "test",
			expectedErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := fields.ValidateAny(fieldNameTest, tc.value, fields.NotEmptyStringValidator(fieldNameTest))
			assert.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestEmptyStringValidator(t *testing.T) {
	t.Parallel()

	v := fields.EmptyStringValidator(fieldNameTest)
	require.ErrorIs(t, v("not empty"), fields.NewErrInvalidValue(fieldNameTest, "not empty", "should be empty string"))
	assert.NoError(t, v(""))
}

func TestNotEmptyStringValidator(t *testing.T) {
	t.Parallel()

	v := fields.NotEmptyStringValidator(fieldNameTest)
	require.ErrorIs(t, v(""), fields.NewErrInvalidEmptyString(fieldNameTest))
	assert.NoError(t, v("test"))
}

func TestStringMinLengthValidator(t *testing.T) {
	t.Parallel()

	minLength := 3
	v := fields.StringMinLengthValidator(fieldNameTest, minLength)
	require.ErrorIs(t, v("ab"), fields.NewErrInvalidValue(fieldNameTest, "ab", fmt.Sprintf("must have at least %d chars", minLength)))
	assert.NoError(t, v("abc"))
}

func TestRegexpValidator(t *testing.T) {
	t.Parallel()

	regExp := "[0-9]"
	v := fields.RegexpValidator(fieldNameTest, regExp)
	require.ErrorIs(t, v("a"), fields.NewErrInvalidValue(fieldNameTest, "a", "must match: "+regExp))
	assert.NoError(t, v("0"))
}

func TestUUIDValidator(t *testing.T) {
	t.Parallel()

	v := fields.UUIDValidator(fieldNameTest)
	_, err := uuid.Parse("")
	require.ErrorIs(t, v(""), fields.NewErrInvalidValue(fieldNameTest, "", err.Error()))
	assert.NoError(t, v(uuid.NewString()))
}

func TestURLValidator(t *testing.T) {
	t.Parallel()

	v := fields.URLValidator(fieldNameTest)
	require.ErrorIs(t, v("payemoji"), fields.NewErrInvalidValue(fieldNameTest, "payemoji", "invalid url"))
	assert.NoError(t, v("https://payemoji.com"))
}

func TestNotNilValidator(t *testing.T) {
	t.Parallel()

	type input struct {
		fieldName fields.Name
		val       any
	}

	tests := []struct {
		name string
		in   input
		want error
	}{
		{
			name: "nil value",
			in: input{
				fieldName: fieldNameTest,
				val:       nil,
			},
			want: fields.NewErrNil(fieldNameTest),
		},
		{
			name: "nil pointer",
			in: input{
				fieldName: fields.NameID,
				val: func() any {
					var v *struct{}
					return v
				}(),
			},
			want: fields.NewErrNil(fields.NameID),
		},
		{
			name: "valid",
			in: input{
				fieldName: fields.NameCreationTime,
				val:       time.Now().UTC(),
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := fields.NotNilValidator(tt.in.fieldName)(tt.in.val)
			assert.ErrorIs(t, out, tt.want)
		})
	}
}

func TestNilValidator(t *testing.T) {
	t.Parallel()

	v := fields.NilValidator(fieldNameTest)
	require.ErrorIs(t, v("not nil"), fields.NewErrInvalidValue(fieldNameTest, "not nil", "should be nil"))
	assert.NoError(t, v(nil))
}

func TestEnumValidator(t *testing.T) {
	t.Parallel()

	v := fields.EnumValidator(fieldNameTest, testEnum("test"))
	require.ErrorIs(t, v(testEnum("test_test")), fields.NewErrEnumValueNotAllowed(fieldNameTest, testEnum("test_test"), testEnum("test")))
	assert.NoError(t, v(testEnum("test")))
}

func TestIntValidator(t *testing.T) {
	t.Parallel()

	v := fields.IntValidator(fieldNameTest)
	require.ErrorIs(t, v("not an integer"), fields.NewErrInvalidValue(fieldNameTest, "not an integer", "invalid integer string"))
	assert.NoError(t, v("0"))
}

func TestNotEmptySliceValidator(t *testing.T) {
	t.Parallel()

	v := fields.NotEmptySliceValidator[any](fieldNameTest)
	require.ErrorIs(t, v([]any{}), fields.NewErrNil(fieldNameTest))
	assert.NoError(t, v([]any{1}))
}

func TestNotEmptySetValidator(t *testing.T) {
	t.Parallel()

	v := fields.NotEmptySetValidator[int](fieldNameTest)
	require.ErrorIs(t, v(sets.New[int]()), fields.NewErrNil(fieldNameTest))
	assert.NoError(t, v(sets.New(sets.With(1, 2))))
}

func TestEmptySliceValidator(t *testing.T) {
	t.Parallel()

	v := fields.EmptySliceValidator[any](fieldNameTest)
	require.ErrorIs(t, v([]any{1}), fields.NewErrInvalidValue(fieldNameTest, []any{1}, "should be empty"))
	assert.NoError(t, v([]any{}))
}

func TestSliceValidator(t *testing.T) {
	t.Parallel()

	errVal := func(err error) error {
		return err
	}

	v := fields.SliceValidator(errVal)
	require.ErrorIs(t, v([]error{nil, assert.AnError, nil}), assert.AnError)
	assert.NoError(t, v([]error{nil, nil, nil}))
}

func TestSetNumValuesValidator(t *testing.T) {
	t.Parallel()

	allowedVals := []testEnum{testEnum("val1"), testEnum("val2")}
	v := fields.SetEnumValuesValidator(fieldNameTest, allowedVals...)
	require.ErrorIs(t, v(sets.New(sets.With(testEnum("val3")))), fields.NewErrEnumValueNotAllowed(fieldNameTest, testEnum("val3"), allowedVals...))
	assert.NoError(t, v(sets.New(sets.With(testEnum("val1")))))
}

func TestTimeFmtValidator(t *testing.T) {
	t.Parallel()

	timeFormat := "2006-01-02"
	v := fields.TimeFmtValidator(fieldNameTest, timeFormat)
	require.ErrorIs(t, v("2023/01/01"), fields.NewErrInvalidValue(fieldNameTest, "2023/01/01", "invalid date string for format "+timeFormat))
	assert.NoError(t, v("2023-01-01"))
}

func TestEmailsValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      []string
		wantErr error
	}{
		{
			name:    "if email is invalid, return error",
			in:      []string{"test"},
			wantErr: fields.NewErrInvalidValue(fieldNameTest, "test", "invalid email address"),
		},
		{
			name:    "if email is valid, return nil",
			in:      []string{"test@test", "test@test.com"},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := fields.EmailsValidator(fieldNameTest)(tt.in)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestEmailValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantErr error
	}{
		{
			name:    "if email is empty, return error",
			in:      "",
			wantErr: fields.NewErrInvalidValue(fieldNameTest, "", "invalid email address"),
		},
		{
			name:    "if email test is invalid, return error",
			in:      "test",
			wantErr: fields.NewErrInvalidValue(fieldNameTest, "test", "invalid email address"),
		},
		{
			name:    "if email is valid, return nil",
			in:      "test@test",
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := fields.EmailValidator(fieldNameTest)(tt.in)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestDomainValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantErr error
	}{
		{
			name:    "if domain is empty, return error",
			in:      "",
			wantErr: fields.NewErrInvalidEmptyString(fields.NameDomain),
		},
		{
			name:    "if domain is invalid, return error",
			in:      "invalid",
			wantErr: fields.NewErrInvalidValue(fields.NameDomain, "invalid", "invalid domain"),
		},
		{
			name:    "if domain is valid, return nil",
			in:      "google.com",
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := fields.DomainValidator(t.Context(), new(net.Resolver))(tt.in)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}
