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
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type mockSpec struct {
	query string
	args  []driver.Value   // Query args.
	cols  []string         // Columns names for returned rows.
	rows  [][]driver.Value // Set of rows returned.
}

func TestProcessSchemaOracle(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT table_name FROM all_tables (.+)",
			args:  []driver.Value{},
			cols:  []string{"table_name"},
			rows: [][]driver.Value{
				{"USER"},
				{"TEST"},
				{"TEST2"}},
		},
		// USER table
		{
			query: `SELECT (.+) FROM all_constraints (.+)`,
			args:  []driver.Value{},
			cols:  []string{"column_name", "contraint_type", "condition"},
			rows: [][]driver.Value{
				{"USER_ID", "P", "USER ID IS NOT NULL"},
				{"REF", "F", ""},
			},
		},
		{
			query: `SELECT (.+) all_cons_columns A JOIN all_constraints C ON (.+) JOIN all_cons_columns B (.+)`,
			args:  []driver.Value{},
			cols:  []string{"ref_table", "column_name", "ref_column_name", "name"},
			rows: [][]driver.Value{
				{"TEST", "REF", "ID", "fk_test"},
			},
		},
		{
			query: `SELECT (.+) LEFT JOIN all_ind_expressions IE (.+) LEFT JOIN all_indexes I (.+)`,
			args:  []driver.Value{},
			cols:  []string{"name", "column_name", "column_position", "descend", "uniqueness", "column_expression", "index_type"},
			rows: [][]driver.Value{
				{"INDEX1_LAST", "SYS_NC00009$", 1, "DESC", "NONUNIQUE", "\"NAME\"", "FUNCTION-BASED NORMAL"},
				{"INDEX_CONTACT_TEST", "SYS_NC00008$", 1, "ASC", "UNIQUE", "UPPER(\"USER_ID\")", "FUNCTION-BASED NORMAL"},
				{"INDEX_CONTACT_TEST", "REF", 2, "ASC", "UNIQUE", nil, "FUNCTION-BASED NORMAL"},
				{"INDEX_CONTACT_TEST", "NAME", 3, "ASC", "UNIQUE", nil, "FUNCTION-BASED NORMAL"},
				{"INDEX_TEST_2", "SYS_NC00007$", 1, "DESC", "NONUNIQUE", "\"NAME\"", "FUNCTION-BASED NORMAL"},
				{"INDEX_TEST_2", "SYS_NC00009$", 2, "DESC", "NONUNIQUE", "\"USER_ID\"", "FUNCTION-BASED NORMAL"},
			},
		},
		{
			query: "SELECT (.+) FROM all_tab_columns (.+)",
			args:  []driver.Value{},
			cols:  []string{"column_name", "data_type", "nullable", "data_default", "data_length", "data_precision", "data_scale", "typecode", "element_type", "element_length", "element_precision", "element_scale"},
			rows: [][]driver.Value{
				{"USER_ID", "VARCHAR2", "N", nil, nil, nil, nil, nil, nil, nil, nil, nil},
				{"NAME", "VARCHAR2", "N", nil, nil, nil, nil, nil, nil, nil, nil, nil},
				{"REF", "NUMBER", "Y", nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},

		// test table
		{
			query: `SELECT (.+) FROM all_constraints (.+)`,
			args:  []driver.Value{},
			cols:  []string{"column_name", "contraint_type", "condition"},
			rows: [][]driver.Value{
				{"ID", "P", "ID IS NOT NULL"}},
		},
		{
			query: `SELECT (.+) all_cons_columns A JOIN all_constraints C ON (.+) JOIN all_cons_columns B (.+)`,
			args:  []driver.Value{},
			cols:  []string{"ref_table", "column_name", "ref_column_name", "name"},
			rows:  [][]driver.Value{},
		},
		{
			query: `SELECT (.+) LEFT JOIN all_ind_expressions IE (.+) LEFT JOIN all_indexes I (.+)`,
			args:  []driver.Value{},
			cols:  []string{"name", "column_name", "column_position", "descend", "uniqueness", "column_expression", "index_type"},
			rows:  [][]driver.Value{},
		},
		{
			query: "SELECT (.+) FROM all_tab_columns (.+)",
			args:  []driver.Value{},
			cols:  []string{"column_name", "data_type", "nullable", "data_default", "data_length", "data_precision", "data_scale", "typecode", "element_type", "element_length", "element_precision", "element_scale"},
			rows: [][]driver.Value{
				{"ID", "NUMBER", "N", nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},

		// test2 table [json column test]
		{
			query: `SELECT (.+) FROM all_constraints (.+)`,
			args:  []driver.Value{},
			cols:  []string{"column_name", "contraint_type", "condition"},
			rows: [][]driver.Value{
				{"ID", "P", "ID IS NOT NULL"},
				{"JSON", "C", "JSON IS JSON"}},
		},
		{
			query: `SELECT (.+) all_cons_columns A JOIN all_constraints C ON (.+) JOIN all_cons_columns B (.+)`,
			args:  []driver.Value{},
			cols:  []string{"ref_table", "column_name", "ref_column_name", "name"},
			rows:  [][]driver.Value{},
		},
		{
			query: `SELECT (.+) LEFT JOIN all_ind_expressions IE (.+) LEFT JOIN all_indexes I (.+)`,
			args:  []driver.Value{},
			cols:  []string{"name", "column_name", "column_position", "descend", "uniqueness", "column_expression", "index_type"},
			rows:  [][]driver.Value{},
		},
		{
			query: "SELECT (.+) FROM all_tab_columns (.+)",
			args:  []driver.Value{},
			cols:  []string{"column_name", "data_type", "nullable", "data_default", "data_length", "data_precision", "data_scale", "typecode", "element_type", "element_length", "element_precision", "element_scale"},
			rows: [][]driver.Value{
				{"ID", "NUMBER", "N", nil, nil, nil, nil, nil, nil, nil, nil, nil},
				{"JSON", "VARCHAR2", "N", nil, nil, nil, nil, nil, nil, nil, nil, nil},
				{"REALJSON", "JSON", "N", nil, nil, nil, nil, nil, nil, nil, nil, nil},
				{"ARRAY_NUM", "STUDENT", "N", nil, nil, nil, nil, "COLLECTION", "NUMBER", nil, 10, 5},
				{"ARRAY_FLOAT", "STUDENT", "N", nil, nil, nil, nil, "COLLECTION", "FLOAT", nil, nil, nil},
				{"ARRAY_STRING", "STUDENT", "N", nil, nil, nil, nil, "COLLECTION", "VARCHAR2", 15, nil, nil},
				{"ARRAY_DATE", "STUDENT", "N", nil, nil, nil, nil, "COLLECTION", "DATE", nil, nil, nil},
				{"ARRAY_INT", "STUDENT", "N", nil, nil, nil, nil, "COLLECTION", "NUMBER", nil, 10, 0},
				{"OBJECT", "CONTACTS", "N", nil, nil, nil, nil, "OBJECT", nil, nil, nil, nil}}},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	err := common.ProcessSchema(conv, InfoSchemaImpl{"test", db, profiles.SourceProfile{}, profiles.TargetProfile{}})
	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"USER": {
			Name:        "USER",
			ColIds:      []string{"USER_ID", "NAME", "REF"},
			ColDefs:     map[string]ddl.ColumnDef{"USER_ID": {Name: "USER_ID", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: false}, NotNull: true}, "NAME": {Name: "NAME", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: false}, NotNull: true}, "REF": {Name: "REF", T: ddl.Type{Name: ddl.Numeric}}},
			PrimaryKeys: []ddl.IndexKey{{ColId: "USER_ID"}},
			ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"REF"}, ReferTableId: "TEST", ReferColumnIds: []string{"ID"}}},
			Indexes: []ddl.CreateIndex{{
				Name:    "INDEX1_LAST",
				TableId: "USER",
				Unique:  false,
				Keys:    []ddl.IndexKey{{ColId: "NAME", Desc: true}},
			}, {
				Name:    "INDEX_TEST_2",
				TableId: "USER",
				Unique:  false,
				Keys:    []ddl.IndexKey{{ColId: "NAME", Desc: true}, {ColId: "USER_ID", Desc: true}},
			}},
		},
		"TEST": {
			Name:   "TEST",
			ColIds: []string{"ID"},
			ColDefs: map[string]ddl.ColumnDef{
				"ID": {Name: "ID", T: ddl.Type{Name: ddl.Numeric}, NotNull: true}},
			PrimaryKeys: []ddl.IndexKey{{ColId: "ID"}},
		},
		"TEST2": {
			Name:   "TEST2",
			ColIds: []string{"ID", "JSON", "REALJSON", "ARRAY_NUM", "ARRAY_FLOAT", "ARRAY_STRING", "ARRAY_DATE", "ARRAY_INT", "OBJECT"},
			ColDefs: map[string]ddl.ColumnDef{
				"ID":           {Name: "ID", T: ddl.Type{Name: ddl.Numeric}, NotNull: true},
				"JSON":         {Name: "JSON", T: ddl.Type{Name: ddl.JSON}, NotNull: true},
				"REALJSON":     {Name: "REALJSON", T: ddl.Type{Name: ddl.JSON}, NotNull: true},
				"ARRAY_NUM":    {Name: "ARRAY_NUM", T: ddl.Type{Name: ddl.Numeric, IsArray: true}, NotNull: true},
				"ARRAY_FLOAT":  {Name: "ARRAY_FLOAT", T: ddl.Type{Name: ddl.Float64, IsArray: true}, NotNull: true},
				"ARRAY_STRING": {Name: "ARRAY_STRING", T: ddl.Type{Name: ddl.String, Len: int64(15), IsArray: true}, NotNull: true},
				"ARRAY_DATE":   {Name: "ARRAY_DATE", T: ddl.Type{Name: ddl.Date, IsArray: true}, NotNull: true},
				"ARRAY_INT":    {Name: "ARRAY_INT", T: ddl.Type{Name: ddl.Int64, IsArray: true}, NotNull: true},
				"OBJECT":       {Name: "OBJECT", T: ddl.Type{Name: ddl.JSON}, NotNull: true},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "ID"}},
		},
	}
	internal.AssertSpSchema(conv, t, expectedSchema, stripSchemaComments(conv.SpSchema))
	userTableId := internal.GetSpTableIdFromName(conv, "USER")
	testTableId := internal.GetSpTableIdFromName(conv, "TEST")
	test2TableId := internal.GetSpTableIdFromName(conv, "TEST2")
	assert.NotEqual(t, "", userTableId)
	assert.NotEqual(t, "", testTableId)
	assert.NotEqual(t, "", testTableId)

	assert.Equal(t, len(conv.SchemaIssues[userTableId]), 0)
	assert.Equal(t, len(conv.SchemaIssues[testTableId]), 0)
	assert.Equal(t, len(conv.SchemaIssues[test2TableId]), 0)
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

// stripSchemaComments returns a schema with all comments removed.
// We mostly ignore schema comments in testing since schema comments
// are often changed and are not a core part of conversion functionality.
func stripSchemaComments(spSchema map[string]ddl.CreateTable) map[string]ddl.CreateTable {
	for t, ct := range spSchema {
		for c, cd := range ct.ColDefs {
			cd.Comment = ""
			ct.ColDefs[c] = cd
		}
		ct.Comment = ""
		spSchema[t] = ct
	}
	return spSchema
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
