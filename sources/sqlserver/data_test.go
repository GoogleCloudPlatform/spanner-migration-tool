// Copyright 2021 Google LLC
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

package sqlserver

import (
	"fmt"
	"math/big"
	"math/bits"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type spannerData struct {
	table string
	cols  []string
	vals  []interface{}
}

// Basic smoke test of ProcessDataRow. The core part of this code path
// (ConvertData) is tested in TestConvertData.
func TestProcessDataRow(t *testing.T) {
	tableName := "testtable"
	cols := []string{"a", "b", "c"}
	conv := buildConv(
		ddl.CreateTable{
			Name:   tableName,
			ColIds: cols,
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.Float64}},
				"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}},
				"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			}},
		schema.Table{
			Name:   tableName,
			ColIds: cols,
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: "float"}},
				"b": {Name: "b", Type: schema.Type{Name: "int"}},
				"c": {Name: "c", Type: schema.Type{Name: "text"}},
			}})
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(func(table string, cols []string, vals []interface{}) {
		rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
	})
	ProcessDataRow(conv, tableName, cols, conv.SrcSchema[tableName], tableName, cols, conv.SpSchema[tableName], []string{"4.2", "6", "prisoner zero"})
	assert.Equal(t, []spannerData{{table: tableName, cols: cols, vals: []interface{}{float64(4.2), int64(6), "prisoner zero"}}}, rows)
}

func TestConvertData(t *testing.T) {

	numStr := "33753785954695469456094506940.33333333982343435"
	numVal := new(big.Rat)
	numVal.SetString(numStr)

	singleColTests := []struct {
		name  string
		ty    ddl.Type
		srcTy string      // Source DB type
		in    string      // Input value for conversion.
		e     interface{} // Expected result.
	}{
		{"bit 0", ddl.Type{Name: ddl.Bool}, "", "0", false},
		{"bit 1", ddl.Type{Name: ddl.Bool}, "", "1", true},
		{"bytes", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, "", string([]byte{137, 80}), []byte{0x89, 0x50}}, // need some other approach to testblob type
		{"date", ddl.Type{Name: ddl.Date}, "", "2019-10-29", getDate("2019-10-29")},
		{"float64", ddl.Type{Name: ddl.Float64}, "", "42.6", float64(42.6)},
		{"int64", ddl.Type{Name: ddl.Int64}, "", "42", int64(42)},
		{"string", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, "", "eh", "eh"},
		{"datetime", ddl.Type{Name: ddl.Timestamp}, "datetime", "2019-10-29T05:30:00", getTimeWithoutTimezone(t, "2019-10-29T05:30:00")},
		{"datetimeoffset", ddl.Type{Name: ddl.Timestamp}, "datetimeoffset", "2021-12-15T07:39:52.9433333+01:20", getTimeWithTimezone(t, "2021-12-15T07:39:52.9433333+01:20")},
		{"decimal", ddl.Type{Name: ddl.Numeric}, "decimal", "234.90909090909", big.NewRat(23490909090909, 100000000000)},
		{"numeric", ddl.Type{Name: ddl.Numeric}, "numeric", numStr, numVal},
	}
	tableName := "testtable"
	for _, tc := range singleColTests {
		col := "a"
		conv := buildConv(
			ddl.CreateTable{
				Name:        tableName,
				ColIds:      []string{col},
				ColDefs:     map[string]ddl.ColumnDef{col: {Name: col, T: tc.ty, NotNull: false}},
				PrimaryKeys: []ddl.IndexKey{}},
			schema.Table{Name: tableName, ColIds: []string{col}, ColDefs: map[string]schema.Column{col: {Type: schema.Type{Name: tc.srcTy}}}})
		conv.TimezoneOffset = "+05:30"
		t.Run(tc.in, func(t *testing.T) {
			at, ac, av, err := ConvertData(conv, tableName, []string{col}, conv.SrcSchema[tableName], tableName, []string{col}, conv.SpSchema[tableName], []string{tc.in})
			checkResults(t, at, ac, av, err, tableName, []string{col}, []interface{}{tc.e}, tc.name)
		})
	}
}

func TestConvertMultiColData(t *testing.T) {
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
			vals:  []string{"6", "6.6", "1"},
			ecols: []string{"a", "b", "c"},
			evals: []interface{}{int64(6), float64(6.6), true},
		},
		{
			name:  "Cols out of order",
			cols:  []string{"b", "c", "a"},
			vals:  []string{"6.6", "1", "6"},
			ecols: []string{"b", "c", "a"},
			evals: []interface{}{float64(6.6), true, int64(6)},
		},
		{
			name:  "Null column",
			cols:  []string{"a", "b", "c"},
			vals:  []string{"6", "NULL", "1"},
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
	tableName := "testtable"
	spTable := ddl.CreateTable{
		Name:   tableName,
		ColIds: []string{"a", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.Bool}},
		}}
	srcTable := schema.Table{
		Name:   tableName,
		ColIds: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": {Type: schema.Type{Name: "int"}},
			"b": {Type: schema.Type{Name: "float"}},
			"c": {Type: schema.Type{Name: "bool"}},
		}}
	for _, tc := range multiColTests {
		t.Run(tc.name, func(t *testing.T) {
			conv := buildConv(spTable, srcTable)
			atable, acols, avals, err := ConvertData(conv, srcTable.Name, tc.cols, conv.SrcSchema[tableName], spTable.Name, tc.cols, conv.SpSchema[tableName], tc.vals)
			checkResults(t, atable, acols, avals, err, tableName, tc.ecols, tc.evals, tc.name)
		})
	}
}

func TestConvertError(t *testing.T) {
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
			vals: []string{"6", "6.6", "truee"},
		},
		{
			name: "Error in bool 128",
			cols: []string{"a", "b", "c"},
			vals: []string{"6", "6.6", "128"},
		},
	}
	tableName := "testtable"
	spTable := ddl.CreateTable{
		Name:   tableName,
		ColIds: []string{"a", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.Bool}},
		}}
	srcTable := schema.Table{
		Name:   tableName,
		ColIds: []string{"c1", "c2", "c3"},
		ColDefs: map[string]schema.Column{
			"c1": {Type: schema.Type{Name: "int"}},
			"c2": {Type: schema.Type{Name: "float"}},
			"c3": {Type: schema.Type{Name: "bool"}},
		}}
	for _, tc := range errorTests {
		t.Run(tc.name, func(t *testing.T) {
			conv := buildConv(spTable, srcTable)
			_, _, _, err := ConvertData(conv, srcTable.Name, tc.cols, conv.SrcSchema[tableName], spTable.Name, tc.cols, conv.SpSchema[tableName], tc.vals)
			assert.NotNil(t, err, tc.name)
		})
	}
}

func TestConvertsyntheticPKey(t *testing.T) {
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
			evals: []interface{}{int64(6), float64(6.6), true, "0"},
		},
		{
			name:  "Sequence 1",
			cols:  []string{"a"},
			vals:  []string{"7"},
			ecols: []string{"a", "synth_id"},
			evals: []interface{}{int64(7), fmt.Sprintf("%d", int64(bits.Reverse64(1)))},
		},
	}
	tableName := "testtable"
	spTable := ddl.CreateTable{
		Name:   tableName,
		ColIds: []string{"a", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.Bool}},
		}}
	srcTable := schema.Table{
		Name:   tableName,
		ColIds: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": {Type: schema.Type{Name: "int"}},
			"b": {Type: schema.Type{Name: "float"}},
			"c": {Type: schema.Type{Name: "bool"}},
		}}
	conv := buildConv(spTable, srcTable)
	conv.SyntheticPKeys[spTable.Name] = internal.SyntheticPKey{ColId: "synth_id", Sequence: 0}
	for _, tc := range syntheticPKeyTests {
		t.Run(tc.name, func(t *testing.T) {
			atable, acols, avals, err := ConvertData(conv, srcTable.Name, tc.cols, conv.SrcSchema[tableName], spTable.Name, tc.cols, conv.SpSchema[tableName], tc.vals)
			checkResults(t, atable, acols, avals, err, tableName, tc.ecols, tc.evals, tc.name)
		})
	}
}

func buildConv(spTable ddl.CreateTable, srcTable schema.Table) *internal.Conv {
	conv := internal.MakeConv()
	conv.SpSchema[spTable.Name] = spTable
	conv.SrcSchema[srcTable.Name] = srcTable
	return conv
}

func checkResults(t *testing.T, atable string, acols []string, avals []interface{}, err error, etable string, ecols []string, evals []interface{}, name string) {
	assert.Nil(t, err, name)
	assert.Equal(t, atable, etable, name+": table mismatch")
	assert.Equal(t, ecols, acols, name+": column mismatch")
	assert.Equal(t, evals, avals, name+": value mismatch")
}

func getDate(s string) civil.Date {
	d, _ := civil.ParseDate(s)
	return d
}

func getTimeWithoutTimezone(t *testing.T, s string) time.Time {
	x, err := time.Parse("2006-01-02T15:04:05", s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}

func getTimeWithTimezone(t *testing.T, s string) time.Time {
	x, err := time.Parse(time.RFC3339, s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}
