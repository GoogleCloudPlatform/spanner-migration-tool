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

package csv

import (
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type spannerData struct {
	table string
	cols  []string
	vals  []interface{}
}

const (
	ALL_TYPES_TABLE string = "all_data_types"
	SINGERS_TABLE   string = "singers"

	ALL_TYPES_CSV string = ALL_TYPES_TABLE + ".csv"
	SINGERS_1_CSV string = SINGERS_TABLE + "_1.csv"
	SINGERS_2_CSV string = SINGERS_TABLE + "_2.csv"
)

func init() {
	logger.Log = zap.NewNop()
}

func getManifestTables() []utils.ManifestTable {
	return []utils.ManifestTable{
		{
			Table_name:    ALL_TYPES_TABLE,
			File_patterns: []string{ALL_TYPES_CSV},
		},
		{
			Table_name:    SINGERS_TABLE,
			File_patterns: []string{SINGERS_1_CSV, SINGERS_2_CSV},
		},
	}
}

func writeCSVs(t *testing.T) {
	csvInput := []struct {
		fileName string
		data     []string
	}{
		{
			ALL_TYPES_CSV,
			[]string{
				"bool_col,byte_col,date_col,float_col,int_col,numeric_col,string_col,timestamp_col,json_col\n",
				"true,test,2019-10-29,15.13,100,39.94,Helloworld,2019-10-29 05:30:00,\"{\"\"key1\"\": \"\"value1\"\", \"\"key2\"\": \"\"value2\"\"}\"",
			},
		},
		{
			SINGERS_1_CSV,
			[]string{
				"SingerId,FirstName,LastName\n",
				"1,\"fn1\",ln1",
			},
		},
		{
			SINGERS_2_CSV,
			[]string{
				"SingerId,FirstName,LastName\n",
				"2,fn2,\"ln2\"",
			},
		},
	}
	for _, in := range csvInput {
		f, err := os.Create(in.fileName)
		if err != nil {
			t.Fatalf("Could not create %s: %v", in.fileName, err)
		}
		if _, err := f.WriteString(strings.Join(in.data, "")); err != nil {
			t.Fatalf("Could not write to %s: %v", in.fileName, err)
		}
	}
}

func cleanupCSVs() {
	for _, fn := range []string{ALL_TYPES_CSV, SINGERS_1_CSV, SINGERS_2_CSV} {
		os.Remove(fn)
	}
}

func TestSetRowStats(t *testing.T) {
	csv := CsvImpl{}
	conv := buildConv(getCreateTable())
	writeCSVs(t)
	defer cleanupCSVs()
	csv.SetRowStats(conv, getManifestTables(), ',')
	assert.Equal(t, map[string]int64{ALL_TYPES_TABLE: 1, SINGERS_TABLE: 2}, conv.Stats.Rows)
}

func TestProcessCSV(t *testing.T) {
	writeCSVs(t)
	defer cleanupCSVs()
	tables := getManifestTables()

	conv := buildConv(getCreateTable())
	var rows []spannerData
	conv.SetDataMode()
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	csv := CsvImpl{}
	err := csv.ProcessCSV(conv, tables, "", ',')
	assert.Nil(t, err)
	assert.Equal(t, []spannerData{
		{
			table: ALL_TYPES_TABLE,
			cols:  []string{"bool_col", "byte_col", "date_col", "float_col", "int_col", "numeric_col", "string_col", "timestamp_col", "json_col"},
			vals:  []interface{}{true, []uint8{0x74, 0x65, 0x73, 0x74}, getDate("2019-10-29"), 15.13, int64(100), *big.NewRat(3994, 100), "Helloworld", getTime(t, "2019-10-29T05:30:00Z"), "{\"key1\": \"value1\", \"key2\": \"value2\"}"},
		},
		{table: SINGERS_TABLE, cols: []string{"SingerId", "FirstName", "LastName"}, vals: []interface{}{int64(1), "fn1", "ln1"}},
		{table: SINGERS_TABLE, cols: []string{"SingerId", "FirstName", "LastName"}, vals: []interface{}{int64(2), "fn2", "ln2"}},
	}, rows)
}

func TestConvertData(t *testing.T) {
	singleColTests := []struct {
		name string
		ty   ddl.Type
		in   string      // Input value for conversion.
		ev   interface{} // Expected values.
	}{
		{"null", ddl.Type{Name: ddl.Bool}, "", nil},
		{"bool", ddl.Type{Name: ddl.Bool}, "true", true},
		{"bytes", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, string([]byte{137, 80}), []byte{0x89, 0x50}},
		{"date", ddl.Type{Name: ddl.Date}, "2019-10-29", getDate("2019-10-29")},
		{"float64", ddl.Type{Name: ddl.Float64}, "42.6", float64(42.6)},
		{"int64", ddl.Type{Name: ddl.Int64}, "42", int64(42)},
		{"numeric", ddl.Type{Name: ddl.Numeric}, "42.6", *big.NewRat(426, 10)},
		{"string", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, "eh", "eh"},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}, "2019-10-29 05:30:00", getTime(t, "2019-10-29T05:30:00Z")},
		{"json", ddl.Type{Name: ddl.JSON}, "{\"key1\": \"value1\"}", "{\"key1\": \"value1\"}"},
		{"int_array", ddl.Type{Name: ddl.Int64, IsArray: true}, "{1,2,NULL}", []spanner.NullInt64{{Int64: int64(1), Valid: true}, {Int64: int64(2), Valid: true}, {Valid: false}}},
		{"string_array", ddl.Type{Name: ddl.String, IsArray: true}, "[ab,cd]", []spanner.NullString{{StringVal: "ab", Valid: true}, {StringVal: "cd", Valid: true}}},
		{"float_array", ddl.Type{Name: ddl.Float64, IsArray: true}, "{1.3,2.5}", []spanner.NullFloat64{{Float64: float64(1.3), Valid: true}, {Float64: float64(2.5), Valid: true}}},
		{"numeric_array", ddl.Type{Name: ddl.Numeric, IsArray: true}, "[1.7]", []spanner.NullNumeric{{Numeric: *big.NewRat(17, 10), Valid: true}}},
	}
	tableName := "testtable"
	for _, tc := range singleColTests {
		col := "a"
		colId := "c1"
		conv := buildConv([]ddl.CreateTable{{
			Name:    tableName,
			Id:      "t1",
			ColDefs: map[string]ddl.ColumnDef{colId: ddl.ColumnDef{Name: col, Id: colId, T: tc.ty}}}})
		_, av, err := convertData(conv, "", tableName, []string{col}, []string{tc.in})
		// NULL scenario.
		if tc.ev == nil {
			var empty []interface{}
			assert.Equal(t, empty, av)
			continue
		}
		assert.Nil(t, err, tc.name)
		assert.Equal(t, []interface{}{tc.ev}, av, tc.name+": value mismatch")
	}

	cols := []string{"a", "b", "c"}
	spTable := ddl.CreateTable{
		Name: tableName,
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
		}}
	errorTests := []struct {
		name string
		cols []string // Input columns.
		vals []string // Input values.
	}{
		{
			name: "Error in int64",
			vals: []string{" 6", "6.6", "true"},
		},
		{
			name: "Error in float64",
			vals: []string{"6", "6.6e", "true"},
		},
		{
			name: "Error in bool",
			vals: []string{"6", "6.6", "truee"},
		},
	}
	for _, tc := range errorTests {
		conv := buildConv([]ddl.CreateTable{spTable})
		_, _, err := convertData(conv, "", tableName, cols, tc.vals)
		assert.NotNil(t, err, tc.name)
	}
}

func getCreateTable() []ddl.CreateTable {
	return []ddl.CreateTable{
		{
			Name:   ALL_TYPES_TABLE,
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1": {Name: "bool_col", Id: "c1", T: ddl.Type{Name: ddl.Bool}},
				"c2": {Name: "byte_col", Id: "c2", T: ddl.Type{Name: ddl.Bytes}},
				"c3": {Name: "date_col", Id: "c3", T: ddl.Type{Name: ddl.Date}},
				"c4": {Name: "float_col", Id: "c4", T: ddl.Type{Name: ddl.Float64}},
				"c5": {Name: "int_col", Id: "c5", T: ddl.Type{Name: ddl.Int64}},
				"c6": {Name: "numeric_col", Id: "c6", T: ddl.Type{Name: ddl.Numeric}},
				"c7": {Name: "string_col", Id: "c7", T: ddl.Type{Name: ddl.String}},
				"c8": {Name: "timestamp_col", Id: "c8", T: ddl.Type{Name: ddl.Timestamp}},
				"c9": {Name: "json_col", Id: "c9", T: ddl.Type{Name: ddl.JSON}},
			},
		},
		{
			Name:   SINGERS_TABLE,
			Id:     "t2",
			ColIds: []string{"c10", "c11", "c12"},
			ColDefs: map[string]ddl.ColumnDef{
				"c10": {Name: "SingerId", Id: "c10", T: ddl.Type{Name: ddl.Int64}},
				"c11": {Name: "FirstName", Id: "c11", T: ddl.Type{Name: ddl.String}},
				"c12": {Name: "LastName", Id: "c12", T: ddl.Type{Name: ddl.String}},
			},
		},
	}
}

func buildConv(spTables []ddl.CreateTable) *internal.Conv {
	conv := internal.MakeConv()
	for _, spTable := range spTables {
		conv.SpSchema[spTable.Id] = spTable
	}
	return conv
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
