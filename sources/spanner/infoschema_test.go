package spanner

import (
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToType(t *testing.T) {
	testCases := []struct {
		name          string
		dataType      string
		expColumnType schema.Type
	}{
		// Scalar inputs.
		{"bool", "BOOL", schema.Type{Name: "BOOL"}},
		{"int", "INT64", schema.Type{Name: "INT64"}},
		{"float", "FLOAT64", schema.Type{Name: "FLOAT64"}},
		{"date", "DATE", schema.Type{Name: "DATE"}},
		{"numeric", "NUMERIC", schema.Type{Name: "NUMERIC"}},
		{"json", "JSON", schema.Type{Name: "JSON"}},
		{"timestamp", "TIMESTAMP", schema.Type{Name: "TIMESTAMP"}},
		{"bytes", "BYTES(100)", schema.Type{Name: "BYTES", Mods: []int64{100}}},
		{"bytes", "BYTES(MAX)", schema.Type{Name: "BYTES", Mods: []int64{ddl.MaxLength}}},
		{"string", "STRING(100)", schema.Type{Name: "STRING", Mods: []int64{100}}},
		{"string", "STRING(MAX)", schema.Type{Name: "STRING", Mods: []int64{ddl.MaxLength}}},
		// Array types.
		{"string_max_arr", "ARRAY<STRING(MAX)>", schema.Type{Name: "STRING", Mods: []int64{ddl.MaxLength}, ArrayBounds: []int64{-1}}},
		{"string_arr", "ARRAY<STRING(100)>", schema.Type{Name: "STRING", Mods: []int64{100}, ArrayBounds: []int64{-1}}},
		{"float_arr", "ARRAY<FLOAT64>", schema.Type{Name: "FLOAT64", ArrayBounds: []int64{-1}}},
		{"numeric_arr", "ARRAY<NUMERIC>", schema.Type{Name: "NUMERIC", ArrayBounds: []int64{-1}}},
	}
	for _, tc := range testCases {
		ty := toType(tc.dataType)
		assert.Equal(t, tc.expColumnType, ty, tc.name)
	}
}
