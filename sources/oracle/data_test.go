// Copyright 2022 Google LLC
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

package oracle

import (
	"fmt"
	"math/big"
	"math/bits"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
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

func TestProcessDataRow(t *testing.T) {
	tableName := "testtable"
	cols := []string{"a", "b", "c", "d"}
	conv := buildConv(
		ddl.CreateTable{
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.Float64}},
				"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}},
				"c": {Name: "b", T: ddl.Type{Name: ddl.Timestamp}},
				"d": {Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
			}},
		schema.Table{
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: "FLOAT"}},
				"b": {Name: "b", Type: schema.Type{Name: "NUMER"}},
				"c": {Name: "c", Type: schema.Type{Name: "TIMESTAMP(6)"}},
				"d": {Name: "d", Type: schema.Type{Name: "CHAR", Mods: []int64{1}}},
			}})
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(func(table string, cols []string, vals []interface{}) {
		rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
	})
	ProcessDataRow(conv, tableName, cols, conv.SrcSchema[tableName], tableName, cols, conv.SpSchema[tableName], []string{"4.2", "6", "2022-01-19T09:34:06.47Z", "p"})
	assert.Equal(t, []spannerData{{table: tableName, cols: cols, vals: []interface{}{float64(4.2), int64(6), getTime("2022-01-19T09:34:06.47Z"), "p"}}}, rows)
}

func TestConvertData(t *testing.T) {
	numStr := "33753785954695469456.33333333982343435"
	numVal := new(big.Rat)
	numVal.SetString(numStr)
	outputJson := "{\"PERSON_TYP\": {\"IDNO\": \"1\", \"NAME\": \"test\", \"PHONE\": \"123456\"}}\n"

	singleColTests := []struct {
		name  string
		ty    ddl.Type
		srcTy string      // Source DB type
		in    string      // Input value for conversion.
		e     interface{} // Expected result.
	}{
		{"bytes", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, "", string([]byte{137, 80}), []byte{0x89, 0x50}}, // need some other approach to testblob type
		{"date", ddl.Type{Name: ddl.Date}, "", "2019-10-29", getDate("2019-10-29")},
		{"float", ddl.Type{Name: ddl.Float64}, "", "42.6", float64(42.6)},
		{"int", ddl.Type{Name: ddl.Int64}, "", "42", int64(42)},
		{"string", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, "VARCHAR2", "eh", "eh"},
		{"number", ddl.Type{Name: ddl.Numeric}, "NUMBER", numStr, numVal},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}, "TIMESTAMP(6)", "2022-01-19T09:34:06.47Z", getTime("2022-01-19T09:34:06.47Z")},
		{"json", ddl.Type{Name: ddl.JSON}, "VARCHAR2", "{\"abc\": 123}", "{\"abc\": 123}"},
		{"bool", ddl.Type{Name: ddl.Bool}, "CHAR(1)", "T", true},
		{"arrayStr", ddl.Type{Name: ddl.String, IsArray: true}, "", "[\"CA\",\"CDSC\",\"DSCCS\"]", []spanner.NullString{{StringVal: "CA", Valid: true}, {StringVal: "CDSC", Valid: true}, {StringVal: "DSCCS", Valid: true}}},
		{"arrayInt", ddl.Type{Name: ddl.Int64, IsArray: true}, "", "[1,2,3]", []spanner.NullInt64{{Int64: 1, Valid: true}, {Int64: 2, Valid: true}, {Int64: 3, Valid: true}}},
		{"arrayFloat", ddl.Type{Name: ddl.Float64, IsArray: true}, "", "[1.5,0.00002,357657]", []spanner.NullFloat64{{Float64: 1.5, Valid: true}, {Float64: 0.00002, Valid: true}, {Float64: 357657, Valid: true}}},
		{"arrayFloat", ddl.Type{Name: ddl.Date, IsArray: true}, "", "[\"2022-04-12\", \"2022-11-12\", \"2022-09-12\"]", getDateArray()},
		{"object", ddl.Type{Name: ddl.JSON}, "OBJECT", "<PERSON_TYP><IDNO>1</IDNO><NAME>test</NAME><PHONE>123456</PHONE></PERSON_TYP>", outputJson},
	}
	tableName := "testtable"
	for _, tc := range singleColTests {
		col := "a"
		conv := buildConv(
			ddl.CreateTable{
				Name:     tableName,
				ColNames: []string{col},
				ColDefs:  map[string]ddl.ColumnDef{col: {Name: col, T: tc.ty, NotNull: false}},
				Pks:      []ddl.IndexKey{}},
			schema.Table{Name: tableName, ColNames: []string{col}, ColDefs: map[string]schema.Column{col: {Type: schema.Type{Name: tc.srcTy}}}})
		conv.TimezoneOffset = "+05:30"
		t.Run(tc.in, func(t *testing.T) {
			at, ac, av, err := convertData(conv, tableName, []string{col}, conv.SrcSchema[tableName], tableName, []string{col}, conv.SpSchema[tableName], []string{tc.in})
			if tc.srcTy == "OBJECT" {
				assert.Nil(t, err, tc.name)
				assert.Equal(t, at, tableName, tc.name+": table mismatch")
				assert.Equal(t, ac, []string{col}, tc.name+": column mismatch")
				assert.Equal(t, len(fmt.Sprint(av[0])), len(outputJson), tc.name+": value mismatch")
			} else {
				checkResults(t, at, ac, av, err, tableName, []string{col}, []interface{}{tc.e}, tc.name)
			}
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
			vals:  []string{"6", "6.6", "t"},
			ecols: []string{"a", "b", "c", "synth_id"},
			evals: []interface{}{int64(6), float64(6.6), "t", int64(0)},
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
			"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
		}}
	srcTable := schema.Table{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": {Type: schema.Type{Name: "NUMBER"}},
			"b": {Type: schema.Type{Name: "FLOAT"}},
			"c": {Type: schema.Type{Name: "CHAR", Mods: []int64{1}}},
		}}
	conv := buildConv(spTable, srcTable)
	conv.SyntheticPKeys[spTable.Name] = internal.SyntheticPKey{Col: "synth_id", Sequence: 0}
	for _, tc := range syntheticPKeyTests {
		t.Run(tc.name, func(t *testing.T) {
			atable, acols, avals, err := convertData(conv, srcTable.Name, tc.cols, conv.SrcSchema[tableName], spTable.Name, tc.cols, conv.SpSchema[tableName], tc.vals)
			checkResults(t, atable, acols, avals, err, tableName, tc.ecols, tc.evals, tc.name)
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
			evals: []interface{}{int64(6), float64(6.6), "1"},
		},
		{
			name:  "Cols out of order",
			cols:  []string{"b", "c", "a"},
			vals:  []string{"6.6", "1", "6"},
			ecols: []string{"b", "c", "a"},
			evals: []interface{}{float64(6.6), "1", int64(6)},
		},
		{
			name:  "Null column",
			cols:  []string{"a", "b", "c"},
			vals:  []string{"6", "NULL", "1"},
			ecols: []string{"a", "c"},
			evals: []interface{}{int64(6), "1"},
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
			"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
		}}
	srcTable := schema.Table{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": {Type: schema.Type{Name: "NUMBER", Mods: []int64{5}}},
			"b": {Type: schema.Type{Name: "FLOAT"}},
			"c": {Type: schema.Type{Name: "CHAR", Mods: []int64{1}}},
		}}
	for _, tc := range multiColTests {
		t.Run(tc.name, func(t *testing.T) {
			conv := buildConv(spTable, srcTable)
			atable, acols, avals, err := convertData(conv, srcTable.Name, tc.cols, conv.SrcSchema[tableName], spTable.Name, tc.cols, conv.SpSchema[tableName], tc.vals)
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
			name: "Error in timestamp",
			cols: []string{"a", "b", "c"},
			vals: []string{"6", "6.6", "2022-01-199:34:06.47Z"},
		},
	}
	tableName := "testtable"
	spTable := ddl.CreateTable{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.Timestamp}},
		}}
	srcTable := schema.Table{
		Name:     tableName,
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": {Type: schema.Type{Name: "NUMBER"}},
			"b": {Type: schema.Type{Name: "FLOAT"}},
			"c": {Type: schema.Type{Name: "TIMESTAMP(6)"}},
		}}
	for _, tc := range errorTests {
		t.Run(tc.name, func(t *testing.T) {
			conv := buildConv(spTable, srcTable)
			_, _, _, err := convertData(conv, srcTable.Name, tc.cols, conv.SrcSchema[tableName], spTable.Name, tc.cols, conv.SpSchema[tableName], tc.vals)
			assert.NotNil(t, err, tc.name)
		})
	}
}
func checkResults(t *testing.T, atable string, acols []string, avals []interface{}, err error, etable string, ecols []string, evals []interface{}, name string) {
	assert.Nil(t, err, name)
	assert.Equal(t, atable, etable, name+": table mismatch")
	assert.Equal(t, ecols, acols, name+": column mismatch")
	assert.Equal(t, evals, avals, name+": value mismatch")
}

func buildConv(spTable ddl.CreateTable, srcTable schema.Table) *internal.Conv {
	conv := internal.MakeConv()
	conv.SpSchema[spTable.Name] = spTable
	conv.SrcSchema[srcTable.Name] = srcTable
	return conv
}

func getTime(val string) time.Time {
	t, _ := time.Parse(time.RFC3339, val)
	return t
}

func getDate(s string) civil.Date {
	d, _ := civil.ParseDate(s)
	return d
}

func getDateArray() []spanner.NullDate {
	ds := []string{"2022-04-12", "2022-11-12", "2022-09-12"}
	var dates []spanner.NullDate
	for _, d := range ds {
		date, _ := civil.ParseDate(d)
		dates = append(dates, spanner.NullDate{Date: date, Valid: true})
	}
	return dates
}
