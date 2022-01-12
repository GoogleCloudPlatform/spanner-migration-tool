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

package oracle

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type mockSpec struct {
	query string
	args  []driver.Value   // Query args.
	cols  []string         // Columns names for returned rows.
	rows  [][]driver.Value // Set of rows returned.
}

func TestProcessSchemaMYSQL(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT table_name FROM all_tables (.+)",
			args:  []driver.Value{},
			cols:  []string{"table_name"},
			rows: [][]driver.Value{
				{"USER"},
				{"TEST"},
			},
		},
		// USER table
		{
			query: `SELECT (.+) FROM all_constraints (.+)`,
			args:  []driver.Value{},
			cols:  []string{"column_name", "contraint_type"},
			rows: [][]driver.Value{
				{"USER_ID", "P"},
				{"REF", "F"},
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
			cols:  []string{"column_name", "data_type", "nullable", "data_default", "data_length", "data_precision", "data_scale"},
			rows: [][]driver.Value{
				{"USER_ID", "VARCHAR2", "N", nil, nil, nil, nil},
				{"NAME", "VARCHAR2", "N", nil, nil, nil, nil},
				{"REF", "NUMBER", "Y", nil, nil, nil, nil}},
		},

		// test table
		{
			query: `SELECT (.+) FROM all_constraints (.+)`,
			args:  []driver.Value{},
			cols:  []string{"column_name", "contraint_type"},
			rows: [][]driver.Value{
				{"ID", "P"}},
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
			cols:  []string{"column_name", "data_type", "nullable", "data_default", "data_length", "data_precision", "data_scale"},
			rows: [][]driver.Value{
				{"ID", "NUMBER", "N", nil, nil, nil, nil}},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	err := common.ProcessSchema(conv, InfoSchemaImpl{"test", db})
	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"USER": {
			Name:     "USER",
			ColNames: []string{"USER_ID", "NAME", "REF"},
			ColDefs:  map[string]ddl.ColumnDef{"USER_ID": {Name: "USER_ID", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: false}, NotNull: true}, "NAME": {Name: "NAME", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: false}, NotNull: true}, "REF": {Name: "REF", T: ddl.Type{Name: ddl.Numeric}}},
			Pks:      []ddl.IndexKey{{Col: "USER_ID"}},
			Fks:      []ddl.Foreignkey{{Name: "fk_test", Columns: []string{"REF"}, ReferTable: "TEST", ReferColumns: []string{"ID"}}},
			Indexes: []ddl.CreateIndex{{
				Name:   "INDEX1_LAST",
				Table:  "USER",
				Unique: false,
				Keys:   []ddl.IndexKey{{Col: "NAME", Desc: true}},
			}, {
				Name:   "INDEX_TEST_2",
				Table:  "USER",
				Unique: false,
				Keys:   []ddl.IndexKey{{Col: "NAME", Desc: true}, {Col: "USER_ID", Desc: true}},
			}},
		},
		"TEST": {
			Name:     "TEST",
			ColNames: []string{"ID"},
			ColDefs: map[string]ddl.ColumnDef{
				"ID": {Name: "ID", T: ddl.Type{Name: ddl.Numeric}, NotNull: true}},
			Pks: []ddl.IndexKey{{Col: "ID"}},
		},
	}
	assert.Equal(t, expectedSchema, stripSchemaComments(conv.SpSchema))
	assert.Equal(t, len(conv.Issues["USER"]), 0)
	assert.Equal(t, len(conv.Issues["TEST"]), 0)
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
