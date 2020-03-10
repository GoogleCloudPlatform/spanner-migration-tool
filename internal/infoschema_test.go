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
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestProcessInfoSchema(t *testing.T) {
	type mockSpec struct {
		query string
		args  []driver.Value   // Query args.
		cols  []string         // Columns names for returned rows.
		rows  [][]driver.Value // Set of rows returned.
	}
	db, mock, err := sqlmock.New()
	assert.Nil(t, err)

	for _, m := range []mockSpec{
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
	} {
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
	conv := MakeConv()
	err = ProcessInfoSchema(conv, db)
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
	assert.Equal(t, expectedSchema, stripSchemaComments(conv.spSchema))
	assert.Equal(t, len(conv.issues["cart"]), 0)
	expectedIssues := map[string][]schemaIssue{
		"aint": []schemaIssue{widened},
		"bs":   []schemaIssue{defaultValue},
		"f4":   []schemaIssue{widened},
		"i4":   []schemaIssue{widened},
		"i2":   []schemaIssue{widened},
		"num":  []schemaIssue{numeric},
		"s":    []schemaIssue{widened, defaultValue},
		"ts":   []schemaIssue{timestamp},
	}
	assert.Equal(t, expectedIssues, conv.issues["test"])
}
