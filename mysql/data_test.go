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

package mysql

import (
	"fmt"
	"math/bits"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
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
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]ddl.ColumnDef{
				"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Float64}},
				"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}},
				"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			}},
		schema.Table{
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]schema.Column{
				"a": schema.Column{Name: "a", Type: schema.Type{Name: "float"}},
				"b": schema.Column{Name: "b", Type: schema.Type{Name: "int"}},
				"c": schema.Column{Name: "c", Type: schema.Type{Name: "text"}},
			}})
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(func(table string, cols []string, vals []interface{}) {
		rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
	})
	ProcessDataRow(conv, tableName, cols, conv.SrcSchema[tableName], tableName, cols, conv.SpSchema[tableName], []string{"4.2", "6", "prisoner zero"})
	assert.Equal(t, []spannerData{spannerData{table: tableName, cols: cols, vals: []interface{}{float64(4.2), int64(6), "prisoner zero"}}}, rows)
}

func TestConvertData(t *testing.T) {
	singleColTests := []struct {
		name  string
		ty    ddl.Type
		srcTy string      // Source DB type (Used by e.g. timestamp conversions).
		in    string      // Input value for conversion.
		e     interface{} // Expected result.
	}{
		{"bool", ddl.Type{Name: ddl.Bool}, "", "1", true},
		{"bytes", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, "", string([]byte{137, 80}), []byte{0x89, 0x50}}, // need some other approach to testblob type
		{"date", ddl.Type{Name: ddl.Date}, "", "2019-10-29", getDate("2019-10-29")},
		{"float64", ddl.Type{Name: ddl.Float64}, "", "42.6", float64(42.6)},
		{"int64", ddl.Type{Name: ddl.Int64}, "", "42", int64(42)},
		{"string", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, "", "eh", "eh"},
		{"datetime", ddl.Type{Name: ddl.Timestamp}, "datetime", "2019-10-29 05:30:00", getTimeWithoutTimezone(t, "2019-10-29 05:30:00")},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}, "timestamp", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00+05:30")},
		{"string array(set)", ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, "", "1,Travel,3,Dance", []spanner.NullString{
			spanner.NullString{StringVal: "1", Valid: true},
			spanner.NullString{StringVal: "Travel", Valid: true},
			spanner.NullString{StringVal: "3", Valid: true},
			spanner.NullString{StringVal: "Dance", Valid: true}}},
	}
	tableName := "testtable"
	for _, tc := range singleColTests {
		col := "a"
		conv := buildConv(
			ddl.CreateTable{
				Name:     tableName,
				ColNames: []string{col},
				ColDefs:  map[string]ddl.ColumnDef{col: ddl.ColumnDef{Name: col, T: tc.ty, NotNull: false}},
				Pks:      []ddl.IndexKey{}},
			schema.Table{Name: tableName, ColNames: []string{col}, ColDefs: map[string]schema.Column{col: schema.Column{Type: schema.Type{Name: tc.srcTy}}}})
		conv.TimezoneOffset = "+05:30"
		t.Run(tc.in, func(t *testing.T) {
			at, ac, av, err := ConvertData(conv, tableName, []string{col}, conv.SrcSchema[tableName], tableName, []string{col}, conv.SpSchema[tableName], []string{tc.in})
			checkResults(t, at, ac, av, err, tableName, []string{col}, []interface{}{tc.e}, tc.name)
		})
	}
}

func TestConvertTimestampData(t *testing.T) {
	timestampTests := []struct {
		name  string
		srcTy string
		in    string
		e     interface{}
	}{
		{"timestampt", "timestamp", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00+10:00")},
		{"datetime", "datetime", "2019-10-29 05:30:00", getTimeWithoutTimezone(t, "2019-10-29 05:30:00")},
	}
	tableName := "testtable"
	for _, tc := range timestampTests {
		col := "a"
		conv := buildConv(
			ddl.CreateTable{
				Name:     tableName,
				ColNames: []string{col},
				ColDefs:  map[string]ddl.ColumnDef{col: ddl.ColumnDef{Name: col, T: ddl.Type{Name: ddl.Timestamp}}}},
			schema.Table{
				Name:     tableName,
				ColNames: []string{col},
				ColDefs:  map[string]schema.Column{col: schema.Column{Type: schema.Type{Name: tc.srcTy}}}})
		conv.TimezoneOffset = "+10:00" // Set offset so test is robust i.e. doesn't depent on local timezone.
		t.Run(tc.in, func(t *testing.T) {
			atable, ac, av, err := ConvertData(conv, tableName, []string{col}, conv.SrcSchema[tableName], tableName, []string{col}, conv.SpSchema[tableName], []string{tc.in})
			assert.Nil(t, err, tc.name)
			assert.Equal(t, atable, tableName, tc.name+": table mismatch")
			assert.Equal(t, []string{col}, ac, tc.name+": column mismatch")
			assert.Equal(t, 1, len(av))
			at, ok1 := av[0].(time.Time)
			et, ok2 := tc.e.(time.Time)
			assert.True(t, ok1 && ok2, tc.name+": cast to Time failed")
			assert.True(t, at.Equal(et), tc.name+": value mismatch")
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
			vals:  []string{"6", "<nil>", "1"},
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
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
		}}
	srcTable := schema.Table{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": schema.Column{Type: schema.Type{Name: "int"}},
			"b": schema.Column{Type: schema.Type{Name: "float"}},
			"c": schema.Column{Type: schema.Type{Name: "bool"}},
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
	}
	tableName := "testtable"
	spTable := ddl.CreateTable{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
		}}
	srcTable := schema.Table{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": schema.Column{Type: schema.Type{Name: "int"}},
			"b": schema.Column{Type: schema.Type{Name: "float"}},
			"c": schema.Column{Type: schema.Type{Name: "bool"}},
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
	tableName := "testtable"
	spTable := ddl.CreateTable{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
		}}
	srcTable := schema.Table{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": schema.Column{Type: schema.Type{Name: "int"}},
			"b": schema.Column{Type: schema.Type{Name: "float"}},
			"c": schema.Column{Type: schema.Type{Name: "bool"}},
		}}
	conv := buildConv(spTable, srcTable)
	conv.SyntheticPKeys[spTable.Name] = internal.SyntheticPKey{Col: "synth_id", Sequence: 0}
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
	conv.ToSource[spTable.Name] = internal.NameAndCols{Name: srcTable.Name, Cols: make(map[string]string)}
	conv.ToSpanner[srcTable.Name] = internal.NameAndCols{Name: spTable.Name, Cols: make(map[string]string)}
	for i := range spTable.ColNames {
		conv.ToSource[spTable.Name].Cols[spTable.ColNames[i]] = srcTable.ColNames[i]
		conv.ToSpanner[srcTable.Name].Cols[srcTable.ColNames[i]] = spTable.ColNames[i]
	}
	return conv
}

func checkResults(t *testing.T, atable string, acols []string, avals []interface{}, err error, etable string, ecols []string, evals []interface{}, name string) {
	assert.Nil(t, err, name)
	assert.Equal(t, atable, etable, name+": table mismatch")
	assert.Equal(t, ecols, acols, name+": column mismatch")
	assert.Equal(t, evals, avals, name+": value mismatch")
}

func getTime(t *testing.T, s string) time.Time {
	x, err := time.Parse(time.RFC3339, s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}

func getTimeWithoutTimezone(t *testing.T, s string) time.Time {
	x, err := time.Parse("2006-01-02 15:04:05", s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}

func getDate(s string) civil.Date {
	d, _ := civil.ParseDate(s)
	return d
}
