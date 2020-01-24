// Copyright 2019 Google LLC
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

package postgres

import (
	"fmt"
	"harbourbridge/spanner/ddl"
	"math/bits"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/stretchr/testify/assert"
)

func TestConvertData(t *testing.T) {
	singleColTests := []struct {
		name    string
		t       ddl.ScalarType
		isArray bool
		pgt     string      // Postgres type (used by e.g. timestamp conversions).
		in      string      // Input value for conversion.
		e       interface{} // Expected result.
	}{
		{"bool", ddl.Bool{}, false, "", "true", true},
		{"bytes", ddl.Bytes{ddl.MaxLength{}}, false, "", `\\x0001beef`, []byte{0x0, 0x1, 0xbe, 0xef}},
		{"date", ddl.Date{}, false, "", "2019-10-29", getDate("2019-10-29")},
		{"float64", ddl.Float64{}, false, "", "42.6", float64(42.6)},
		{"int64", ddl.Int64{}, false, "", "42", int64(42)},
		{"string", ddl.String{ddl.MaxLength{}}, false, "", "eh", "eh"},
		{"timestamptz", ddl.Timestamp{}, false, "timestamptz", "2019-10-29 05:30:00+10", getTime(t, "2019-10-29T05:30:00+10:00")},
		{"timestamp", ddl.Timestamp{}, false, "timestamp", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00Z")},
		// Add cases for each array type, since each is a separate code path.
		{"bool array", ddl.Bool{}, true, "", "{true,false}", []bool{true, false}},
		{"bytes array", ddl.Bytes{ddl.MaxLength{}}, true, "", `{\\x0001beef}`, [][]byte{{0x0, 0x1, 0xbe, 0xef}}},
		{"date array", ddl.Date{}, true, "", "{2019-10-29,2019-10-28}", []civil.Date{getDate("2019-10-29"), getDate("2019-10-28")}},
		{"float64 array", ddl.Float64{}, true, "", "{1.1,2.2,3.3}", []float64{1.1, 2.2, 3.3}},
		{"int64 array", ddl.Int64{}, true, "", "{1,2,3}", []int64{1, 2, 3}},
		{"string array", ddl.String{ddl.MaxLength{}}, true, "", "{1,2,3}", []string{"1", "2", "3"}},
		{"timestamp array", ddl.Timestamp{}, true, "timestamptz", "{2019-10-29 05:30:00+10}", []time.Time{getTime(t, "2019-10-29T05:30:00+10:00")}},
	}
	for _, tc := range singleColTests {
		table := "testtable"
		col := "a"
		conv := buildConv(table,
			ddl.CreateTable{table, []string{col}, map[string]ddl.ColumnDef{col: ddl.ColumnDef{col, tc.t, tc.isArray, false, ""}}, []ddl.IndexKey{}, ""},
			pgTableDef{map[string]pgColDef{col: pgColDef{id: tc.pgt}}})
		conv.SetLocation(time.UTC)
		ac, av, err := ConvertData(conv, table, table, []string{col}, []string{tc.in})
		checkResults(t, ac, av, err, []string{col}, []interface{}{tc.e}, tc.name)
	}

	timestampTests := []struct {
		name string
		pgt  string
		in   string
		e    interface{}
	}{
		{"timestamptz hour timezone", "timestamptz", "2019-10-29 05:30:00+10:00", getTime(t, "2019-10-29T05:30:00+10:00")},
		{"timestamptz hour/min timezone", "timestamptz", "2019-10-29 05:30:00+10:30", getTime(t, "2019-10-29T05:30:00+10:30")},
		{"timestamptz no timezone", "timestamptz", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00+11:00")},
		{"timestamp", "timestamp", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00Z")},
	}
	for _, tc := range timestampTests {
		table := "testtable"
		col := "a"
		conv := buildConv(table,
			ddl.CreateTable{table, []string{col}, map[string]ddl.ColumnDef{col: ddl.ColumnDef{Name: col, T: ddl.Timestamp{}}}, []ddl.IndexKey{}, ""},
			pgTableDef{map[string]pgColDef{col: pgColDef{id: tc.pgt}}})
		loc, _ := time.LoadLocation("Australia/Sydney")
		conv.SetLocation(loc)
		ac, av, err := ConvertData(conv, table, table, []string{col}, []string{tc.in})
		assert.Nil(t, err, tc.name)
		assert.Equal(t, []string{col}, ac, tc.name+": column mismatch")
		// Avoid assert.Equal for time.Time (it forces location equality).
		// Instead use Time.Equals, which determines equality based on whether
		// two times represent the same instant.
		assert.Equal(t, 1, len(av))
		at, ok1 := av[0].(time.Time)
		et, ok2 := tc.e.(time.Time)
		assert.True(t, ok1 && ok2, tc.name+": cast to Time failed")
		assert.True(t, at.Equal(et), tc.name+": value mismatch")
	}

	multiColTests := []struct {
		name  string
		cols  []string      // Input columns.
		vals  []string      // Input values.
		ecols []string      // Expected columns.
		evals []interface{} // Expected values.
	}{
		{
			name:  "Cols in order",
			cols:  []string{"a", "b", "c"},
			vals:  []string{"6", "6.6", "true"},
			ecols: []string{"a", "b", "c"},
			evals: []interface{}{int64(6), float64(6.6), true},
		},
		{
			name:  "Cols out of order",
			cols:  []string{"b", "c", "a"},
			vals:  []string{"6.6", "true", "6"},
			ecols: []string{"b", "c", "a"},
			evals: []interface{}{float64(6.6), true, int64(6)},
		},
		{
			name:  "Null column",
			cols:  []string{"a", "b", "c"},
			vals:  []string{"6", "\\N", "true"},
			ecols: []string{"a", "c"},
			evals: []interface{}{int64(6), true},
		},
		{
			name:  "Missing columns",
			cols:  []string{"a"},
			vals:  []string{"6"},
			ecols: []string{"a"},
			evals: []interface{}{int64(6)},
		},
	}
	spSchema := ddl.CreateTable{
		"testtable",
		[]string{"a", "b", "c"},
		map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Float64{}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Bool{}},
		},
		[]ddl.IndexKey{},
		"",
	}
	pgSchema := pgTableDef{
		map[string]pgColDef{
			"a": pgColDef{id: "int8"},
			"b": pgColDef{id: "float8"},
			"c": pgColDef{id: "bool"},
		}}
	for _, tc := range multiColTests {
		conv := buildConv(spSchema.Name, spSchema, pgSchema)
		acols, avals, err := ConvertData(conv, spSchema.Name, spSchema.Name, tc.cols, tc.vals)
		checkResults(t, acols, avals, err, tc.ecols, tc.evals, tc.name)
	}

	errorTests := []struct {
		name string
		cols []string // Input columns.
		vals []string // Input values.
	}{
		{
			name: "Error in int64",
			cols: []string{"a", "b", "c"},
			vals: []string{" 6", "6.6", "true"},
		},
		{
			name: "Error in float64",
			cols: []string{"a", "b", "c"},
			vals: []string{"6", "6.6e", "true"},
		},
		{
			name: "Error in bool",
			cols: []string{"a", "b", "c"},
			vals: []string{" 6", "6.6", "truee"},
		},
	}
	for _, tc := range errorTests {
		conv := buildConv(spSchema.Name, spSchema, pgSchema)
		acols, avals, err := ConvertData(conv, spSchema.Name, spSchema.Name, tc.cols, tc.vals)
		assert.NotNil(t, err, tc.name)
		assert.Equal(t, []string{}, acols, tc.name+": column mismatch")
		assert.Equal(t, []interface{}{}, avals, tc.name+": value mismatch")
	}

	syntheticPKeyTests := []struct {
		name  string
		cols  []string      // Input columns.
		vals  []string      // Input values.
		ecols []string      // Expected columns.
		evals []interface{} // Expected values.
	}{
		{
			name:  "Sequence 0",
			cols:  []string{"a", "b", "c"},
			vals:  []string{"6", "6.6", "true"},
			ecols: []string{"a", "b", "c", "synth_id"},
			evals: []interface{}{int64(6), float64(6.6), true, int64(0)},
		},
		{
			name:  "Sequence 1",
			cols:  []string{"a"},
			vals:  []string{"7"},
			ecols: []string{"a", "synth_id"},
			evals: []interface{}{int64(7), int64(bits.Reverse64(1))},
		},
	}
	conv := buildConv(spSchema.Name, spSchema, pgSchema)
	conv.syntheticPKeys[spSchema.Name] = syntheticPKey{col: "synth_id", sequence: 0}
	for _, tc := range syntheticPKeyTests {
		acols, avals, err := ConvertData(conv, spSchema.Name, spSchema.Name, tc.cols, tc.vals)
		checkResults(t, acols, avals, err, tc.ecols, tc.evals, tc.name)
	}
}

func buildConv(table string, spSchema ddl.CreateTable, pgSchema pgTableDef) *Conv {
	conv := MakeConv()
	conv.spSchema[table] = spSchema
	conv.pgSchema[table] = pgSchema
	return conv
}

func checkResults(t *testing.T, acols []string, avals []interface{}, err error, ecols []string, evals []interface{}, name string) {
	assert.Nil(t, err, name)
	assert.Equal(t, ecols, acols, name+": column mismatch")
	assert.Equal(t, evals, avals, name+": value mismatch")
}

func getTime(t *testing.T, s string) time.Time {
	x, err := time.Parse(time.RFC3339, s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}

func getDate(s string) civil.Date {
	d, _ := civil.ParseDate(s)
	return d
}
