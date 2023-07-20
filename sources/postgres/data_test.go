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

package postgres

import (
	"fmt"
	"math/bits"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

// Basic smoke test of ProcessDataRow. The core part of this code path
// (ConvertData) is tested in TestConvertData.
func TestProcessDataRow(t *testing.T) {
	tableName := "testtable"
	tableId := "t1"
	cols := []string{"a", "b", "c"}
	colIds := []string{"c1", "c2", "c3"}
	conv := buildConv(
		ddl.CreateTable{
			Name:   tableName,
			Id:     "t1",
			ColIds: colIds,
			ColDefs: map[string]ddl.ColumnDef{
				"c1": ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Float64}},
				"c2": ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}},
				"c3": ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			}},
		schema.Table{
			Name:   tableName,
			Id:     "t1",
			ColIds: colIds,
			ColDefs: map[string]schema.Column{
				"c1": schema.Column{Name: "a", Id: "c1", Type: schema.Type{Name: "float"}},
				"c2": schema.Column{Name: "b", Id: "c2", Type: schema.Type{Name: "int"}},
				"c3": schema.Column{Name: "c", Id: "c3", Type: schema.Type{Name: "text"}},
			}})
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	ProcessDataRow(conv, tableId, colIds, []string{"4.2", "6", "prisoner zero"})
	assert.Equal(t, []spannerData{spannerData{table: tableName, cols: cols, vals: []interface{}{float64(4.2), int64(6), "prisoner zero"}}}, rows)
}

func TestConvertData(t *testing.T) {
	singleColTests := []struct {
		name  string
		ty    ddl.Type
		srcTy string      // Source DB type (used by e.g. timestamp conversions).
		in    string      // Input value for conversion.
		e     interface{} // Expected result.
	}{
		{"bool", ddl.Type{Name: ddl.Bool}, "", "true", true},
		{"bytes", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, "", `\x0001beef`, []byte{0x0, 0x1, 0xbe, 0xef}},
		{"date", ddl.Type{Name: ddl.Date}, "", "2019-10-29", getDate("2019-10-29")},
		{"float64", ddl.Type{Name: ddl.Float64}, "", "42.6", float64(42.6)},
		{"int64", ddl.Type{Name: ddl.Int64}, "", "42", int64(42)},
		{"string", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, "", "eh", "eh"},
		{"timestamptz", ddl.Type{Name: ddl.Timestamp}, "timestamptz", "2019-10-29 05:30:00+10", getTime(t, "2019-10-29T05:30:00+10:00")},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}, "timestamp", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00Z")},

		// Add cases for each array type, since each is a separate code path.
		// Note: the PostgreSQL array output routine puts double quotes around
		// elements if they have white space e.g. timestamps, bytea arrays.
		{"bool array", ddl.Type{Name: ddl.Bool, IsArray: true}, "", "{true,false,NULL}", []spanner.NullBool{
			spanner.NullBool{Bool: true, Valid: true}, spanner.NullBool{Bool: false, Valid: true},
			spanner.NullBool{Valid: false}}},
		{"bytes array", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength, IsArray: true}, "", `{"\\x0001beef",NULL}`, [][]byte{{0x0, 0x1, 0xbe, 0xef}, nil}},
		{"date array", ddl.Type{Name: ddl.Date, IsArray: true}, "", "{2019-10-29,NULL,2019-10-28}", []spanner.NullDate{
			spanner.NullDate{Date: getDate("2019-10-29"), Valid: true},
			spanner.NullDate{Valid: false},
			spanner.NullDate{Date: getDate("2019-10-28"), Valid: true}}},
		{"float64 array", ddl.Type{Name: ddl.Float64, IsArray: true}, "", "{1.1,NULL,2.2,3.3}", []spanner.NullFloat64{
			spanner.NullFloat64{Float64: 1.1, Valid: true},
			spanner.NullFloat64{Valid: false},
			spanner.NullFloat64{Float64: 2.2, Valid: true},
			spanner.NullFloat64{Float64: 3.3, Valid: true}}},
		{"int64 array", ddl.Type{Name: ddl.Int64, IsArray: true}, "", "{NULL,1,2,3}", []spanner.NullInt64{
			spanner.NullInt64{Valid: false},
			spanner.NullInt64{Int64: 1, Valid: true},
			spanner.NullInt64{Int64: 2, Valid: true},
			spanner.NullInt64{Int64: 3, Valid: true}}},
		{"string array", ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, "", `{1,NULL,3,"NULL"}`, []spanner.NullString{
			spanner.NullString{StringVal: "1", Valid: true},
			spanner.NullString{Valid: false},
			spanner.NullString{StringVal: "3", Valid: true},
			spanner.NullString{StringVal: "NULL", Valid: true}}},
		{"timestamp array", ddl.Type{Name: ddl.Timestamp, IsArray: true}, "timestamptz", `{"2019-10-29 05:30:00+10",NULL}`, []spanner.NullTime{
			spanner.NullTime{Time: getTime(t, "2019-10-29T05:30:00+10:00"), Valid: true},
			spanner.NullTime{Valid: false}}},
		{"empty array", ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, "", "{}", []spanner.NullString{}},
	}
	tableName := "testtable"
	tableId := "t1"
	for _, tc := range singleColTests {
		col := "a"
		colId := "c1"
		conv := buildConv(
			ddl.CreateTable{
				Name:        tableName,
				Id:          tableId,
				ColIds:      []string{colId},
				ColDefs:     map[string]ddl.ColumnDef{colId: ddl.ColumnDef{Name: col, Id: colId, T: tc.ty, NotNull: false}},
				PrimaryKeys: []ddl.IndexKey{}},
			schema.Table{Name: tableName,
				Id:      tableId,
				ColIds:  []string{colId},
				ColDefs: map[string]schema.Column{colId: schema.Column{Name: col, Id: colId, Type: schema.Type{Name: tc.srcTy}}}})
		conv.SetLocation(time.UTC)
		at, ac, av, err := ConvertData(conv, tableId, []string{colId}, []string{tc.in})
		checkResults(t, at, ac, av, err, tableName, []string{col}, []interface{}{tc.e}, tc.name)
	}

	timestampTests := []struct {
		name  string
		srcTy string
		in    string
		e     interface{}
	}{
		{"timestamptz hour timezone", "timestamptz", "2019-10-29 05:30:00+10:00", getTime(t, "2019-10-29T05:30:00+10:00")},
		{"timestamptz hour/min timezone", "timestamptz", "2019-10-29 05:30:00+10:30", getTime(t, "2019-10-29T05:30:00+10:30")},
		{"timestamptz no timezone", "timestamptz", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00+11:00")},
		{"timestamp", "timestamp", "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00Z")},
	}
	for _, tc := range timestampTests {
		col := "a"
		colId := "c1"
		conv := buildConv(
			ddl.CreateTable{
				Name:    tableName,
				Id:      tableId,
				ColIds:  []string{colId},
				ColDefs: map[string]ddl.ColumnDef{colId: ddl.ColumnDef{Name: col, Id: colId, T: ddl.Type{Name: ddl.Timestamp}}}},
			schema.Table{
				Name:    tableName,
				Id:      tableId,
				ColIds:  []string{colId},
				ColDefs: map[string]schema.Column{colId: schema.Column{Type: schema.Type{Name: tc.srcTy}, Name: col, Id: colId}}})
		loc, _ := time.LoadLocation("Australia/Sydney")
		conv.SetLocation(loc) // Set location so test is robust i.e. doesn't depent on local timezone.
		atable, ac, av, err := ConvertData(conv, tableId, []string{colId}, []string{tc.in})
		assert.Nil(t, err, tc.name)
		assert.Equal(t, atable, tableName, tc.name+": table mismatch")
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
		name   string
		cols   []string // Input columns.
		colIds []string
		vals   []string      // Input values.
		ecols  []string      // Expected columns.
		evals  []interface{} // Expected values.
	}{
		{
			name:   "Cols in order",
			cols:   []string{"a", "b", "c"},
			colIds: []string{"c1", "c2", "c3"},
			vals:   []string{"6", "6.6", "true"},
			ecols:  []string{"a", "b", "c"},
			evals:  []interface{}{int64(6), float64(6.6), true},
		},
		{
			name:   "Cols out of order",
			cols:   []string{"b", "c", "a"},
			colIds: []string{"c2", "c3", "c1"},
			vals:   []string{"6.6", "true", "6"},
			ecols:  []string{"b", "c", "a"},
			evals:  []interface{}{float64(6.6), true, int64(6)},
		},
		{
			name:   "Null column",
			cols:   []string{"a", "b", "c"},
			colIds: []string{"c1", "c2", "c3"},
			vals:   []string{"6", "\\N", "true"},
			ecols:  []string{"a", "c"},
			evals:  []interface{}{int64(6), true},
		},
		{
			name:   "Missing columns",
			cols:   []string{"a"},
			colIds: []string{"c1"},
			vals:   []string{"6"},
			ecols:  []string{"a"},
			evals:  []interface{}{int64(6)},
		},
	}
	spTable := ddl.CreateTable{
		Name:   tableName,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, Id: "c1"},
			"c2": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}, Id: "c2"},
			"c3": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}, Id: "c3"},
		}}
	srcTable := schema.Table{
		Name:   tableName,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3"},
		ColDefs: map[string]schema.Column{
			"c1": schema.Column{Type: schema.Type{Name: "int8"}, Name: "a", Id: "c1"},
			"c2": schema.Column{Type: schema.Type{Name: "float8"}, Name: "b", Id: "c2"},
			"c3": schema.Column{Type: schema.Type{Name: "bool"}, Name: "c", Id: "c3"},
		}}
	for _, tc := range multiColTests {
		conv := buildConv(spTable, srcTable)
		atable, acols, avals, err := ConvertData(conv, tableId, tc.colIds, tc.vals)
		checkResults(t, atable, acols, avals, err, tableName, tc.ecols, tc.evals, tc.name)
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
			vals: []string{"6", "6.6", "truee"},
		},
	}
	for _, tc := range errorTests {
		conv := buildConv(spTable, srcTable)
		_, _, _, err := ConvertData(conv, tableId, tc.cols, tc.vals)
		assert.NotNil(t, err, tc.name)
	}

	syntheticPKeyTests := []struct {
		name   string
		cols   []string // Input columns.
		colIds []string
		vals   []string      // Input values.
		ecols  []string      // Expected columns.
		evals  []interface{} // Expected values.
	}{
		{
			name:   "Sequence 0",
			cols:   []string{"a", "b", "c"},
			colIds: []string{"c1", "c2", "c3"},
			vals:   []string{"6", "6.6", "true"},
			ecols:  []string{"a", "b", "c", "synth_id"},
			evals:  []interface{}{int64(6), float64(6.6), true, "0"},
		},
		{
			name:   "Sequence 1",
			cols:   []string{"a"},
			colIds: []string{"c1"},
			vals:   []string{"7"},
			ecols:  []string{"a", "synth_id"},
			evals:  []interface{}{int64(7), fmt.Sprintf("%d", int64(bits.Reverse64(1)))},
		},
	}
	spTable.ColDefs["c4"] = ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50, IsArray: false}}
	conv := buildConv(spTable, srcTable)
	conv.SyntheticPKeys[spTable.Id] = internal.SyntheticPKey{ColId: "c4", Sequence: 0}
	for _, tc := range syntheticPKeyTests {
		atable, acols, avals, err := ConvertData(conv, tableId, tc.colIds, tc.vals)
		checkResults(t, atable, acols, avals, err, tableName, tc.ecols, tc.evals, tc.name)
	}
}

func buildConv(spTable ddl.CreateTable, srcTable schema.Table) *internal.Conv {
	conv := internal.MakeConv()
	conv.SpSchema[spTable.Id] = spTable
	conv.SrcSchema[srcTable.Id] = srcTable
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

func getDate(s string) civil.Date {
	d, _ := civil.ParseDate(s)
	return d
}
