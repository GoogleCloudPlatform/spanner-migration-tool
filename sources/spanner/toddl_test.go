// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import (
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToSpannerGSQLDialectType(t *testing.T) {
	conv := internal.MakeConv()
	toDDLImpl := ToDdlImpl{}
	toDDLTests := []struct {
		name       string
		columnType schema.Type
		expDDLType ddl.Type
	}{
		// Exact inputs.
		{"bool", schema.Type{Name: "BOOL"}, ddl.Type{Name: ddl.Bool}},
		{"bytes", schema.Type{Name: "BYTES", Mods: []int64{100}}, ddl.Type{Name: ddl.Bytes, Len: 100}},
		{"date", schema.Type{Name: "DATE"}, ddl.Type{Name: ddl.Date}},
		{"float", schema.Type{Name: "FLOAT64"}, ddl.Type{Name: ddl.Float64}},
		{"int", schema.Type{Name: "INT64"}, ddl.Type{Name: ddl.Int64}},
		{"json", schema.Type{Name: "JSON"}, ddl.Type{Name: ddl.JSON}},
		{"numeric", schema.Type{Name: "NUMERIC"}, ddl.Type{Name: ddl.Numeric}},
		{"string", schema.Type{Name: "STRING", Mods: []int64{100}}, ddl.Type{Name: ddl.String, Len: 100}},
		{"timestamp", schema.Type{Name: "TIMESTAMP"}, ddl.Type{Name: ddl.Timestamp}},
	}
	for _, tc := range toDDLTests {
		ty, err := toDDLImpl.ToSpannerGSQLDialectType(conv, "", tc.columnType)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, tc.expDDLType, ty, tc.name)
	}
}

func TestToSpannerPostgreSQLDialectType(t *testing.T) {
	conv := internal.MakeConv()
	toDDLImpl := ToDdlImpl{}
	toDDLTests := []struct {
		name       string
		columnType schema.Type
		expDDLType ddl.Type
	}{
		// Exact inputs.
		{"bool", schema.Type{Name: "BOOL"}, ddl.Type{Name: ddl.PGBool}},
		{"bytes", schema.Type{Name: "BYTEA", Mods: []int64{100}}, ddl.Type{Name: ddl.PGBytea, Len: 100}},
		{"date", schema.Type{Name: "DATE"}, ddl.Type{Name: ddl.PGDate}},
		{"float", schema.Type{Name: "FLOAT8"}, ddl.Type{Name: ddl.PGFloat8}},
		{"int", schema.Type{Name: "INT8"}, ddl.Type{Name: ddl.PGInt8}},
		{"json", schema.Type{Name: "JSONB"}, ddl.Type{Name: ddl.PGJSONB}},
		{"numeric", schema.Type{Name: "NUMERIC"}, ddl.Type{Name: ddl.PGNumeric}},
		{"string", schema.Type{Name: "VARCHAR", Mods: []int64{100}}, ddl.Type{Name: ddl.PGVarchar, Len: 100}},
		{"timestamp", schema.Type{Name: "TIMESTAMPTZ"}, ddl.Type{Name: ddl.PGTimestamptz}},
	}
	for _, tc := range toDDLTests {
		ty, err := toDDLImpl.ToSpannerPostgreSQLDialectType(conv, "", tc.columnType)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, tc.expDDLType, ty, tc.name)
	}
}
