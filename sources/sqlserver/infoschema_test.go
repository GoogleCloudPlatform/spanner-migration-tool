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

package sqlserver

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

func TestProcessSchema(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE'",
			cols:  []string{"table_schema", "table_name"},
			rows: [][]driver.Value{
				{"public", "user"},
				{"public", "test"}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "user"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"user_id", "PRIMARY KEY"},
				{"ref", "FOREIGN KEY"}},
		},

		{
			query: "SELECT (.+) FROM sys.foreign_key_columns (.+)",
			args:  []driver.Value{"user"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows: [][]driver.Value{
				{"public", "test", "ref", "id", "fk_test"},
			},
		},
		{
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"user"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order"},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "user"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"user_id", "text", "0", nil, nil, nil, nil},
				{"name", "text", "0", nil, nil, nil, nil},
				{"ref", "bigint", "1", nil, nil, nil, nil}},
		},
		{
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "test"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"id", "bigint", "0", nil, nil, 64, 0},
			},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "test"},
			cols:  []string{"column_name", "constraint_type"},
			rows:  [][]driver.Value{},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	err := common.ProcessSchema(conv, InfoSchemaImpl{"test", db})
	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"user": ddl.CreateTable{
			Name:     "user",
			ColNames: []string{"user_id", "name", "ref"},
			ColDefs: map[string]ddl.ColumnDef{
				"user_id": ddl.ColumnDef{Name: "user_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"name":    ddl.ColumnDef{Name: "name", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"ref":     ddl.ColumnDef{Name: "ref", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "user_id"}},
			Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"ref"}, ReferTable: "test", ReferColumns: []string{"id"}}}},
		"test": ddl.CreateTable{
			Name:     "test",
			ColNames: []string{"id"},
			ColDefs: map[string]ddl.ColumnDef{
				"id": ddl.ColumnDef{Name: "id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
			},
			Pks: nil,
			Fks: nil},
	}
	assert.Equal(t, expectedSchema, stripSchemaComments(conv.SpSchema))
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
