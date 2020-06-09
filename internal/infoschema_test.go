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

package internal

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type mockSpec struct {
	query string
	args  []driver.Value   // Query args.
	cols  []string         // Columns names for returned rows.
	rows  [][]driver.Value // Set of rows returned.
}

func TestProcessInfoSchema(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE'",
			cols:  []string{"table_schema", "table_name"},
			rows: [][]driver.Value{
				{"public", "cart"},
				{"public", "test"}},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "cart"},
			cols:  []string{"column_name", "data_type", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"productid", "text", nil, "NO", nil, nil, nil, nil},
				{"userid", "text", nil, "NO", nil, nil, nil, nil},
				{"quantity", "bigint", nil, "YES", nil, nil, 64, 0}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "cart"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"productid", "PRIMARY KEY"},
				{"userid", "PRIMARY KEY"}},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "test"},
			cols:  []string{"column_name", "data_type", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"id", "bigint", nil, "NO", nil, nil, 64, 0},
				{"aint", "ARRAY", "integer", "YES", nil, nil, nil, nil},
				{"atext", "ARRAY", "text", "YES", nil, nil, nil, nil},
				{"b", "boolean", nil, "YES", nil, nil, nil, nil},
				{"bs", "bigint", nil, "NO", "nextval('test11_bs_seq'::regclass)", nil, 64, 0},
				{"by", "bytea", nil, "YES", nil, nil, nil, nil},
				{"c", "character", nil, "YES", nil, 1, nil, nil},
				{"c8", "character", nil, "YES", nil, 8, nil, nil},
				{"d", "date", nil, "YES", nil, nil, nil, nil},
				{"f8", "double precision", nil, "YES", nil, nil, 53, nil},
				{"f4", "real", nil, "YES", nil, nil, 24, nil},
				{"i8", "bigint", nil, "YES", nil, nil, 64, 0},
				{"i4", "integer", nil, "YES", nil, nil, 32, 0},
				{"i2", "smallint", nil, "YES", nil, nil, 16, 0},
				{"num", "numeric", nil, "YES", nil, nil, nil, nil},
				{"s", "integer", nil, "NO", "nextval('test11_s_seq'::regclass)", nil, 32, 0},
				{"ts", "timestamp without time zone", nil, "YES", nil, nil, nil, nil},
				{"tz", "timestamp with time zone", nil, "YES", nil, nil, nil, nil},
				{"txt", "text", nil, "YES", nil, nil, nil, nil},
				{"vc", "character varying", nil, "YES", nil, nil, nil, nil},
				{"vc6", "character varying", nil, "YES", nil, 6, nil, nil}},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "test"},
			cols:  []string{"column_name", "constraint_type"},
			rows:  [][]driver.Value{{"id", "PRIMARY KEY"}},
		},
	}
	db := mkMockDB(t, ms)
	conv := MakeConv()
	err := ProcessInfoSchema(conv, db)
	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"cart": ddl.CreateTable{
			Name:     "cart",
			ColNames: []string{"productid", "userid", "quantity"},
			ColDefs: map[string]ddl.ColumnDef{
				"productid": ddl.ColumnDef{Name: "productid", T: ddl.String{Len: ddl.MaxLength{}}, NotNull: true},
				"userid":    ddl.ColumnDef{Name: "userid", T: ddl.String{Len: ddl.MaxLength{}}, NotNull: true},
				"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Int64{}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "productid"}, ddl.IndexKey{Col: "userid"}}},
		"test": ddl.CreateTable{
			Name:     "test",
			ColNames: []string{"id", "aint", "atext", "b", "bs", "by", "c", "c8", "d", "f8", "f4", "i8", "i4", "i2", "num", "s", "ts", "tz", "txt", "vc", "vc6"},
			ColDefs: map[string]ddl.ColumnDef{
				"id":    ddl.ColumnDef{Name: "id", T: ddl.Int64{}, NotNull: true},
				"aint":  ddl.ColumnDef{Name: "aint", T: ddl.Int64{}, IsArray: true},
				"atext": ddl.ColumnDef{Name: "atext", T: ddl.String{Len: ddl.MaxLength{}}, IsArray: true},
				"b":     ddl.ColumnDef{Name: "b", T: ddl.Bool{}},
				"bs":    ddl.ColumnDef{Name: "bs", T: ddl.Int64{}, NotNull: true},
				"by":    ddl.ColumnDef{Name: "by", T: ddl.Bytes{Len: ddl.MaxLength{}}},
				"c":     ddl.ColumnDef{Name: "c", T: ddl.String{Len: ddl.Int64Length{Value: 1}}},
				"c8":    ddl.ColumnDef{Name: "c8", T: ddl.String{Len: ddl.Int64Length{Value: 8}}},
				"d":     ddl.ColumnDef{Name: "d", T: ddl.Date{}},
				"f8":    ddl.ColumnDef{Name: "f8", T: ddl.Float64{}},
				"f4":    ddl.ColumnDef{Name: "f4", T: ddl.Float64{}},
				"i8":    ddl.ColumnDef{Name: "i8", T: ddl.Int64{}},
				"i4":    ddl.ColumnDef{Name: "i4", T: ddl.Int64{}},
				"i2":    ddl.ColumnDef{Name: "i2", T: ddl.Int64{}},
				"num":   ddl.ColumnDef{Name: "num", T: ddl.Float64{}},
				"s":     ddl.ColumnDef{Name: "s", T: ddl.Int64{}, NotNull: true},
				"ts":    ddl.ColumnDef{Name: "ts", T: ddl.Timestamp{}},
				"tz":    ddl.ColumnDef{Name: "tz", T: ddl.Timestamp{}},
				"txt":   ddl.ColumnDef{Name: "txt", T: ddl.String{Len: ddl.MaxLength{}}},
				"vc":    ddl.ColumnDef{Name: "vc", T: ddl.String{Len: ddl.MaxLength{}}},
				"vc6":   ddl.ColumnDef{Name: "vc6", T: ddl.String{Len: ddl.Int64Length{Value: 6}}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "id"}}},
	}
	assert.Equal(t, expectedSchema, stripSchemaComments(conv.SpSchema))
	assert.Equal(t, len(conv.Issues["cart"]), 0)
	expectedIssues := map[string][]SchemaIssue{
		"aint": []SchemaIssue{Widened},
		"bs":   []SchemaIssue{DefaultValue},
		"f4":   []SchemaIssue{Widened},
		"i4":   []SchemaIssue{Widened},
		"i2":   []SchemaIssue{Widened},
		"num":  []SchemaIssue{Numeric},
		"s":    []SchemaIssue{Widened, DefaultValue},
		"ts":   []SchemaIssue{Timestamp},
	}
	assert.Equal(t, expectedIssues, conv.Issues["test"])
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

// TestProcessSqlData is a basic test of ProcessSqlData that checks
// handling of bad rows and table and column renaming. The core data
// conversion work of ProcessSqlData is done by ConvertData, which is
// extensively is tested by TestConvertSqlRow.
func TestProcessSqlData(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE'",
			cols:  []string{"table_schema", "table_name"},
			rows:  [][]driver.Value{{"public", "te st"}},
		}, {
			query: `SELECT [*] FROM "public"."te st"`, // query is a regexp!
			cols:  []string{"a a", " b", " c "},
			rows: [][]driver.Value{
				{42.3, 3, "cat"},
				{6.6, 22, "dog"},
				{6.6, "2006-01-02", "dog"}}, // Test bad row logic.
		},
	}
	db := mkMockDB(t, ms)
	conv := buildConv(
		ddl.CreateTable{
			Name:     "te_st",
			ColNames: []string{"a a", " b", " c "},
			ColDefs: map[string]ddl.ColumnDef{
				"a_a": ddl.ColumnDef{Name: "a_a", T: ddl.Float64{}},
				"Ab":  ddl.ColumnDef{Name: "Ab", T: ddl.Int64{}},
				"Ac_": ddl.ColumnDef{Name: "Ac_", T: ddl.String{Len: ddl.MaxLength{}}},
			}},
		schema.Table{
			Name:     "te st",
			ColNames: []string{"a_a", "_b", "_c_"},
			ColDefs: map[string]schema.Column{
				"a a": schema.Column{Name: "a a", Type: schema.Type{Name: "float4"}},
				" b":  schema.Column{Name: " b", Type: schema.Type{Name: "int8"}},
				" c ": schema.Column{Name: " c ", Type: schema.Type{Name: "text"}},
			}})
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	ProcessSqlData(conv, db)
	assert.Equal(t,
		[]spannerData{
			spannerData{table: "te_st", cols: []string{"a a", " b", " c "}, vals: []interface{}{float64(42.3), int64(3), "cat"}},
			spannerData{table: "te_st", cols: []string{"a a", " b", " c "}, vals: []interface{}{float64(6.6), int64(22), "dog"}},
		},
		rows)
	assert.Equal(t, conv.BadRows(), int64(1))
	assert.Equal(t, conv.SampleBadRows(10), []string{"table=te st cols=[a a  b  c ] data=[6.6 2006-01-02 dog]\n"})
	assert.Equal(t, int64(1), conv.Unexpecteds()) // Bad row generates an entry in unexpected.
}

func TestConvertSqlRow_SingleCol(t *testing.T) {
	tDate, _ := time.Parse("2006-01-02", "2019-10-29")
	tc := []struct {
		name    string
		srcType schema.Type
		spType  ddl.ScalarType
		isArray bool
		in      interface{} // Input value for conversion.
		e       interface{} // Expected result.
	}{
		{name: "bool", srcType: schema.Type{Name: "bool"}, spType: ddl.Bool{}, in: true, e: true},
		{name: "bool string", srcType: schema.Type{Name: "bool"}, spType: ddl.Bool{}, in: "true", e: true},
		{name: "bytes", srcType: schema.Type{Name: "bytea"}, spType: ddl.Bytes{Len: ddl.MaxLength{}}, in: []byte{0x0, 0x1, 0xbe, 0xef}, e: []byte{0x0, 0x1, 0xbe, 0xef}},
		{name: "date", srcType: schema.Type{Name: "date"}, spType: ddl.Date{}, in: tDate, e: getDate("2019-10-29")},
		{name: "date string", srcType: schema.Type{Name: "date"}, spType: ddl.Date{}, in: "2019-10-29", e: getDate("2019-10-29")},
		{name: "int64", srcType: schema.Type{Name: "bigint"}, spType: ddl.Int64{}, in: int64(42), e: int64(42)},
		{name: "int64 string", srcType: schema.Type{Name: "text"}, spType: ddl.Int64{}, in: "42", e: int64(42)},
		{name: "int64 float64", srcType: schema.Type{Name: "float8"}, spType: ddl.Int64{}, in: float64(42), e: int64(42)},
		{name: "int64 byte", srcType: schema.Type{Name: "bytea"}, spType: ddl.Int64{}, in: []byte("42"), e: int64(42)},
		{name: "float64", srcType: schema.Type{Name: "float8"}, spType: ddl.Float64{}, in: float64(42.6), e: float64(42.6)},
		{name: "float64 string", srcType: schema.Type{Name: "text"}, spType: ddl.Float64{}, in: "42.6", e: float64(42.6)},
		{name: "float64 int", srcType: schema.Type{Name: "bigint"}, spType: ddl.Float64{}, in: int64(42), e: float64(42)},
		{name: "float64 byte", srcType: schema.Type{Name: "numeric"}, spType: ddl.Float64{}, in: []byte("42.6"), e: float64(42.6)},
		{name: "string", srcType: schema.Type{Name: "text"}, spType: ddl.String{Len: ddl.MaxLength{}}, in: "eh", e: "eh"},
		{name: "string bool", srcType: schema.Type{Name: "bool"}, spType: ddl.String{Len: ddl.MaxLength{}}, in: true, e: "true"},
		{name: "string byte", srcType: schema.Type{Name: "bytea"}, spType: ddl.String{Len: ddl.MaxLength{}}, in: []byte("abc"), e: "abc"},
		{name: "string int64", srcType: schema.Type{Name: "bigint"}, spType: ddl.String{Len: ddl.MaxLength{}}, in: int64(42), e: "42"},
		{name: "string float64", srcType: schema.Type{Name: "float8"}, spType: ddl.String{Len: ddl.MaxLength{}}, in: float64(42.3), e: "42.3"},
		{name: "string time", srcType: schema.Type{Name: "timestamp"}, spType: ddl.String{Len: ddl.MaxLength{}},
			in: getTime(t, "2019-10-29T05:30:00+10:00"), e: "2019-10-29 05:30:00 +1000 +1000"},
		{name: "timestamptz", srcType: schema.Type{Name: "timestamptz"}, spType: ddl.Timestamp{},
			in: getTime(t, "2019-10-29T05:30:00+10:00"), e: getTime(t, "2019-10-29T05:30:00+10:00")},
		{name: "timestamptz string", srcType: schema.Type{Name: "timestamptz"}, spType: ddl.Timestamp{},
			in: "2019-10-29 05:30:00+10:00", e: getTime(t, "2019-10-29T05:30:00+10:00")},
		{name: "timestamp", srcType: schema.Type{Name: "timestamptz"}, spType: ddl.Timestamp{},
			in: getTime(t, "2019-10-29T05:30:00Z"), e: getTime(t, "2019-10-29T05:30:00Z")},
		{name: "timestamp string", srcType: schema.Type{Name: "timestamptz"}, spType: ddl.Timestamp{},
			in: "2019-10-29 05:30:00", e: getTime(t, "2019-10-29T05:30:00Z")},

		// ConvertSqlRow uses convArray for conversion of array types.
		// Since convArray is extensively tested in data_test.go, we
		// only test a few cases here.
		{name: "array bool", srcType: schema.Type{Name: "bool", ArrayBounds: []int64{-1}}, spType: ddl.Bool{}, isArray: true,
			in: []byte("{true,false,NULL}"), e: []spanner.NullBool{
				spanner.NullBool{Bool: true, Valid: true},
				spanner.NullBool{Bool: false, Valid: true},
				spanner.NullBool{Valid: false}}},
		{name: "timestamp array", srcType: schema.Type{Name: "timestamptz", ArrayBounds: []int64{-1}}, spType: ddl.Timestamp{}, isArray: true,
			in: []byte(`{"2019-10-29 05:30:00+10",NULL}`),
			e: []spanner.NullTime{
				spanner.NullTime{Time: getTime(t, "2019-10-29T05:30:00+10:00"), Valid: true},
				spanner.NullTime{Valid: false}}},
	}
	tableName := "testtable"
	for _, tc := range tc {
		col := "a"
		conv := MakeConv()
		conv.SetLocation(time.UTC)
		cols := []string{col}
		srcSchema := schema.Table{Name: tableName, ColNames: []string{col}, ColDefs: map[string]schema.Column{col: schema.Column{Type: tc.srcType}}}
		spSchema := ddl.CreateTable{
			Name:     tableName,
			ColNames: []string{col},
			ColDefs:  map[string]ddl.ColumnDef{col: ddl.ColumnDef{Name: col, T: tc.spType, IsArray: tc.isArray}}}
		ac, av, err := ConvertSqlRow(conv, tableName, cols, srcSchema, tableName, cols, spSchema, []interface{}{tc.in})
		assert.Equal(t, cols, ac)
		assert.Equal(t, []interface{}{tc.e}, av)
		assert.Nil(t, err)
	}
}

func TestConvertSqlRow_MultiCol(t *testing.T) {
	// Tests multi-column behavior of ConvertSqlRow (including
	// handling of null columns and synthetic keys). Also tests
	// the combination of ProcessInfoSchema and ConvertSqlRow
	// i.e. ConvertSqlRow uses the schemas built by
	// ProcessInfoSchema.
	ms := []mockSpec{
		{
			query: "SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE'",
			cols:  []string{"table_schema", "table_name"},
			rows:  [][]driver.Value{{"public", "test"}},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "test"},
			cols:  []string{"column_name", "data_type", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"a", "text", nil, "NO", nil, nil, nil, nil},
				{"b", "double precision", nil, "YES", nil, nil, 53, nil},
				{"c", "bigint", nil, "YES", nil, nil, 64, 0}},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "test"},
			cols:  []string{"column_name", "constraint_type"},
			rows:  [][]driver.Value{}, // No primary key --> force generation of synthetic key.
		},
		// Note: go-sqlmock mocks specify an ordered sequence
		// of queries and results.  This (repeated) entry is
		// needed because ProcessSqlData (redundantly) gets
		// the set of tables via a SQL query.
		{
			query: "SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE'",
			cols:  []string{"table_schema", "table_name"},
			rows:  [][]driver.Value{{"public", "test"}},
		}, {
			query: `SELECT [*] FROM "public"."test"`, // query is a regexp!
			cols:  []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"cat", 42.3, nil},
				{"dog", nil, 22}},
		},
	}
	db := mkMockDB(t, ms)
	conv := MakeConv()
	err := ProcessInfoSchema(conv, db)
	assert.Nil(t, err)
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	ProcessSqlData(conv, db)
	assert.Equal(t, []spannerData{
		{table: "test", cols: []string{"a", "b", "synth_id"}, vals: []interface{}{"cat", float64(42.3), int64(0)}},
		{table: "test", cols: []string{"a", "c", "synth_id"}, vals: []interface{}{"dog", int64(22), int64(-9223372036854775808)}}},
		rows)
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func TestSetRowStats(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE'",
			cols:  []string{"table_schema", "table_name"},
			rows:  [][]driver.Value{{"public", "test1"}, {"public", "test2"}},
		}, {
			query: `SELECT COUNT[(][*][)] FROM "public"."test1"`,
			cols:  []string{"count"},
			rows:  [][]driver.Value{{5}},
		}, {
			query: `SELECT COUNT[(][*][)] FROM "public"."test2"`,
			cols:  []string{"count"},
			rows:  [][]driver.Value{{142}},
		},
	}
	db := mkMockDB(t, ms)
	conv := MakeConv()
	conv.SetDataMode()
	SetRowStats(conv, db)
	assert.Equal(t, int64(5), conv.Stats.Rows["test1"])
	assert.Equal(t, int64(142), conv.Stats.Rows["test2"])
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func mkMockDB(t *testing.T, ms []mockSpec) *sql.DB {
	db, mock, err := sqlmock.New()
	assert.Nil(t, err)
	for _, m := range ms {
		rows := sqlmock.NewRows(m.cols)
		for _, r := range m.rows {
			rows.AddRow(r...)
		}
		if len(m.args) > 0 {
			mock.ExpectQuery(m.query).WithArgs(m.args...).WillReturnRows(rows)
		} else {
			mock.ExpectQuery(m.query).WillReturnRows(rows)
		}

	}
	return db
}
