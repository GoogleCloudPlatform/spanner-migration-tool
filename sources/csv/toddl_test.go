package csv

import (
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	toDDLTests := []struct {
		name       string
		columnType string
		expDDLType ddl.Type
	}{
		// Exact inputs.
		{"bool", "BOOL", ddl.Type{Name: ddl.Bool}},
		{"bytes", "BYTES", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		{"date", "DATE", ddl.Type{Name: ddl.Date}},
		{"float", "FLOAT64", ddl.Type{Name: ddl.Float64}},
		{"int", "INT64", ddl.Type{Name: ddl.Int64}},
		{"numeric", "NUMERIC", ddl.Type{Name: ddl.Numeric}},
		{"string", "STRING", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"timestamp", "TIMESTAMP", ddl.Type{Name: ddl.Timestamp}},
		{"json", "JSON", ddl.Type{Name: ddl.JSON}},
		// Variations in case and field length.
		{"bool mixed case", "BoOl", ddl.Type{Name: ddl.Bool}},
		{"NUMERIC mixed case", "numErIC", ddl.Type{Name: ddl.Numeric}},
		{"timestamp mixed case", "tImEsTamP", ddl.Type{Name: ddl.Timestamp}},
		{"string with length", "STRING(100)", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"mixed case byte with length", "BytES(100)", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		{"mixed case byte with no length", "BytES()", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
	}
	for _, tc := range toDDLTests {
		ty, err := ToSpannerType(tc.columnType)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, tc.expDDLType, ty, tc.name)
	}

	errorTests := []struct {
		columnType string
	}{
		// These columns should error out.
		{"BYTE"},
		{"STRINGS"},
		{"INTEGER"},
		{"INT32"},
		{"INT"},
		{"FLOAT"},
		{"BOOLEAN"},
	}
	for _, tc := range errorTests {
		_, err := ToSpannerType(tc.columnType)
		assert.NotNil(t, err)
	}
}
