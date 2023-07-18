package transformation

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestConvScalar(t *testing.T) {
	conv := &internal.Conv{}

	tests := []struct {
		dataType string
		val      string
		expected interface{}
		err      error
	}{
		{ddl.Bool, "true", true, nil},
		{ddl.Bool, "false", false, nil},
		{ddl.Bytes, "Hello", []byte("Hello"), nil},
		{ddl.Date, "2023-06-09", civil.Date{Year: 2023, Month: 6, Day: 9}, nil},
		{ddl.Float64, "3.14", 3.14, nil},
		{ddl.Int64, "42", int64(42), nil},
		{ddl.Numeric, "3.14159", big.NewRat(314159, 100000), nil},
		{ddl.String, "Test", "Test", nil},
		{ddl.Timestamp, "2023-06-09 12:34:56", time.Date(2023, 6, 9, 12, 34, 56, 0, time.UTC), nil},
		{"invalid", "abc", "abc", fmt.Errorf("data conversion not implemented for type invalid")},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s - %s", test.dataType, test.val), func(t *testing.T) {
			result, err := convScalar(conv, test.dataType, test.val)
			if test.err != nil {
				assert.Error(t, err, "expected error")
				assert.EqualError(t, err, test.err.Error(), "error message mismatch")
				assert.Equal(t, test.expected, result, "result mismatch")
			} else {
				assert.NoError(t, err, "unexpected error")
				assert.Equal(t, test.expected, result, "result mismatch")
			}
		})
	}
}

func TestConvBool(t *testing.T) {
	tests := []struct {
		val      string
		expected bool
		err      error
	}{
		{"true", true, nil},
		{"false", false, nil},
		{"invalid", false, fmt.Errorf("can't convert to bool: strconv.ParseBool: parsing \"invalid\": invalid syntax")},
	}

	for _, test := range tests {
		result, err := convBool(&internal.Conv{}, test.val)
		if test.err != nil {
			assert.Error(t, err, "expected error")
			assert.EqualError(t, err, test.err.Error(), "error message mismatch")
			assert.Equal(t, test.expected, result, "result mismatch")
		} else {
			assert.NoError(t, err, "unexpected error")
			assert.Equal(t, test.expected, result, "result mismatch")
		}
	}
}

func TestConvBytes(t *testing.T) {
	tests := []struct {
		val      string
		expected []byte
	}{
		{"Hello", []byte("Hello")},
		{"", []byte("")},
	}

	for _, test := range tests {
		result, err := convBytes(test.val)
		assert.NoError(t, err, "unexpected error")
		assert.Equal(t, test.expected, result, "result mismatch")
	}
}

func TestConvDate(t *testing.T) {
	tests := []struct {
		val      string
		expected civil.Date
		err      error
	}{
		{"2023-06-09", civil.Date{Year: 2023, Month: 6, Day: 9}, nil},
		{"invalid", civil.Date{}, fmt.Errorf("can't convert to date: parsing time \"invalid\" as \"2006-01-02\": cannot parse \"invalid\" as \"2006\"")},
	}

	for _, test := range tests {
		result, err := convDate(test.val)
		if test.err != nil {
			assert.Error(t, err, "expected error")
			assert.EqualError(t, err, test.err.Error(), "error message mismatch")
			assert.Equal(t, test.expected, result, "result mismatch")
		} else {
			assert.NoError(t, err, "unexpected error")
			assert.Equal(t, test.expected, result, "result mismatch")
		}
	}
}

func TestConvFloat64(t *testing.T) {
	tests := []struct {
		val      string
		expected float64
		err      error
	}{
		{"3.14", 3.14, nil},
		{"invalid", 0.0, fmt.Errorf("can't convert to float64: strconv.ParseFloat: parsing \"invalid\": invalid syntax")},
	}

	for _, test := range tests {
		result, err := convFloat64(test.val)
		if test.err != nil {
			assert.Error(t, err, "expected error")
			assert.EqualError(t, err, test.err.Error(), "error message mismatch")
			assert.Equal(t, test.expected, result, "result mismatch")
		} else {
			assert.NoError(t, err, "unexpected error")
			assert.Equal(t, test.expected, result, "result mismatch")
		}
	}
}

func TestConvInt64(t *testing.T) {
	tests := []struct {
		val      string
		expected int64
		err      error
	}{
		{"42", int64(42), nil},
		{"invalid", int64(0), fmt.Errorf("can't convert to int64: strconv.ParseInt: parsing \"invalid\": invalid syntax")},
	}

	for _, test := range tests {
		result, err := convInt64(test.val)
		if test.err != nil {
			assert.Error(t, err, "expected error")
			assert.EqualError(t, err, test.err.Error(), "error message mismatch")
			assert.Equal(t, test.expected, result, "result mismatch")
		} else {
			assert.NoError(t, err, "unexpected error")
			assert.Equal(t, test.expected, result, "result mismatch")
		}
	}
}

func TestConvNumeric(t *testing.T) {
	tests := []struct {
		dataType string
		val      string
		expected interface{}
		err      error
	}{
		{constants.DIALECT_POSTGRESQL, "3.14159", spanner.PGNumeric{Numeric: "3.14159", Valid: true}, nil},
		{constants.DIALECT_GOOGLESQL, "3.14159", big.NewRat(314159, 100000), nil},
		{"invalid", "invalid", nil, fmt.Errorf("can't convert \"invalid\" to big.Rat")},
	}

	for _, test := range tests {
		result, err := convNumeric(&internal.Conv{SpDialect: test.dataType}, test.val)
		if test.err != nil {
			assert.Error(t, err, "expected error")
			assert.EqualError(t, err, test.err.Error(), "error message mismatch")
			assert.Nil(t, nil, "expected nil result")
		} else {
			assert.NoError(t, err, "unexpected error")
			if test.dataType == constants.DIALECT_POSTGRESQL {
				assert.IsType(t, spanner.PGNumeric{}, result, "result type mismatch")
				expectedNumeric, _ := test.expected.(spanner.PGNumeric)
				actualNumeric, _ := result.(spanner.PGNumeric)
				assert.Equal(t, expectedNumeric, actualNumeric, "result mismatch")
			} else {
				assert.IsType(t, &big.Rat{}, result, "result type mismatch")
				expectedRat, _ := test.expected.(*big.Rat)
				actualRat, _ := result.(*big.Rat)
				assert.True(t, expectedRat.Cmp(actualRat) == 0, "result mismatch")
			}
		}
	}
}

func TestConvTimestamp(t *testing.T) {
	tests := []struct {
		val      string
		expected time.Time
		err      error
	}{
		{"2023-06-09 12:34:56", time.Date(2023, 6, 9, 12, 34, 56, 0, time.UTC), nil},
		{"invalid", time.Time{}, fmt.Errorf("can't convert to timestamp, value:invalid")},
	}

	for _, test := range tests {
		result, err := convTimestamp(test.val, "")
		if test.err != nil {
			assert.Error(t, err, "expected error")
			assert.EqualError(t, err, test.err.Error(), "error message mismatch")
			assert.Equal(t, test.expected, result, "result mismatch")
		} else {
			assert.NoError(t, err, "unexpected error")
			assert.Equal(t, test.expected, result, "result mismatch")
		}
	}
}
