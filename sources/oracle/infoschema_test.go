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
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"database/sql/driver"
	"testing"
	"regexp"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/stretchr/testify/assert"
)

type mockSpec struct {
	query string
	args  []driver.Value
	cols  []string
	rows  [][]driver.Value
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

func TestGetTableName(t *testing.T) {
	isi := InfoSchemaImpl{}
	assert.Equal(t, "test_table", isi.GetTableName("test_db", "test_table"))
}

func TestToType(t *testing.T) {
	ty := toType("VARCHAR2", sql.NullInt64{Int64: 255, Valid: true}, sql.NullInt64{}, sql.NullInt64{})
	assert.Equal(t, "VARCHAR2", ty.Name)

	ty = toType("NUMBER", sql.NullInt64{}, sql.NullInt64{Int64: 38, Valid: true}, sql.NullInt64{Int64: 2, Valid: true})
	assert.Equal(t, "NUMBER", ty.Name)
	assert.Equal(t, []int64{38, 2}, ty.Mods)
}

func TestGenerateSrcSchema(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = UPPER[(]:1[)]",
			args:  []driver.Value{"testdb"},
			cols:  []string{"TABLE_NAME"},
			rows:  [][]driver.Value{{"TEST_TABLE"}},
		},
		{
			query: regexp.QuoteMeta(`SELECT COUNT(*) FROM all_tab_cols WHERE table_name = 'ALL_TAB_COLS' AND column_name = 'IDENTITY_COLUMN'`),
			args:  []driver.Value{},
			cols:  []string{"COUNT"},
			rows:  [][]driver.Value{{1}},
		},
		{
			query: regexp.QuoteMeta(`SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, IDENTITY_COLUMN 
		FROM ALL_TAB_COLS 
		WHERE OWNER = :1 AND TABLE_NAME IN (:2) 
		ORDER BY TABLE_NAME, COLUMN_ID`),
			args: []driver.Value{"TESTDB", "TEST_TABLE"},
			cols: []string{"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "NULLABLE", "DATA_DEFAULT", "DATA_LENGTH", "DATA_PRECISION", "DATA_SCALE", "IDENTITY_COLUMN"},
			rows: [][]driver.Value{
				{"TEST_TABLE", "ID", "NUMBER", "N", nil, nil, 38, 0, "YES"},
				{"TEST_TABLE", "NAME", "VARCHAR2", "Y", nil, 50, nil, nil, "NO"},
			},
		},
		{
			query: regexp.QuoteMeta(`SELECT 
			k.table_name,
			k.column_name,
			t.constraint_type,
			t.search_condition,
			t.constraint_name
		FROM all_constraints t
		INNER JOIN all_cons_columns k
		ON (k.constraint_name = t.constraint_name AND k.owner = t.owner AND k.table_name = t.table_name)
		WHERE t.owner = :1 AND k.table_name IN (:2)
		ORDER BY k.table_name, k.position`),
			args: []driver.Value{"TESTDB", "TEST_TABLE"},
			cols: []string{"table_name", "column_name", "constraint_type", "search_condition", "constraint_name"},
			rows: [][]driver.Value{
				{"TEST_TABLE", "ID", "P", nil, "PK_TEST"},
				{"TEST_TABLE", "NAME", "C", "IS JSON", "SYS_C1"},
			},
		},
		{
			query: regexp.QuoteMeta(`SELECT 
			A.table_name,
			B.table_name AS ref_table, 
			A.column_name AS col_name,
			B.column_name AS ref_col_name,
			A.constraint_name AS name
		FROM all_cons_columns A 
		JOIN all_constraints C ON A.owner = C.owner AND A.constraint_name = C.constraint_name
		JOIN all_cons_columns B ON B.owner = C.owner AND B.constraint_name = C.r_constraint_name
		WHERE A.owner = :1 AND A.table_name IN (:2)
		ORDER BY A.table_name, A.position`),
			args: []driver.Value{"TESTDB", "TEST_TABLE"},
			cols: []string{"table_name", "ref_table", "col_name", "ref_col_name", "name"},
			rows: [][]driver.Value{},
		},
		{
			query: regexp.QuoteMeta(`SELECT 
			IC.table_name,
			IC.index_name,
			IC.column_name,
			IC.descend,
			I.uniqueness,
			IE.column_expression,
			I.index_type
		FROM all_ind_columns IC 
		LEFT JOIN all_ind_expressions IE ON IC.index_name = IE.index_name AND IC.column_position = IE.column_position AND IC.index_owner = IE.index_owner
		LEFT JOIN all_indexes I ON IC.index_name = I.index_name AND I.table_owner = IC.index_owner
		WHERE IC.index_owner = :1 AND IC.table_name IN (:2)
		ORDER BY IC.table_name, IC.index_name, IC.column_position`),
			args: []driver.Value{"TESTDB", "TEST_TABLE"},
			cols: []string{"table_name", "index_name", "column_name", "descend", "uniqueness", "column_expression", "index_type"},
			rows: [][]driver.Value{
				{"TEST_TABLE", "IDX_NAME", "NAME", "ASC", "NONUNIQUE", nil, "NORMAL"},
			},
		},
	}

	db := mkMockDB(t, ms)
	defer db.Close()

	conv := internal.MakeConv()
	conv.SetSchemaMode()
	isi := InfoSchemaImpl{"testdb", db, "migration-project-id", profiles.SourceProfile{}, profiles.TargetProfile{}}
	commonInfoSchema := common.InfoSchemaImpl{}

	_, err := commonInfoSchema.GenerateSrcSchema(conv, isi, 1)
	assert.Nil(t, err)

	
var table schema.Table
var ok bool
for _, t := range conv.SrcSchema {
	if t.Name == "TEST_TABLE" {
		table = t
		ok = true
	}
}

	if fmt.Printf("SrcSchema = %+v\n", conv.SrcSchema)
	assert.True(t, ok) {
				assert.Equal(t, "TEST_TABLE", table.Name)
		assert.Equal(t, 2, len(table.ColDefs))
		
		colIdMapped := table.ColNameIdMap["ID"]
		assert.Equal(t, "NUMBER", table.ColDefs[colIdMapped].Type.Name)
		assert.Equal(t, true, table.ColDefs[colIdMapped].NotNull)
		
		colNameMapped := table.ColNameIdMap["NAME"]
		assert.Equal(t, "VARCHAR2", table.ColDefs[colNameMapped].Type.Name)
		assert.Equal(t, false, table.ColDefs[colNameMapped].NotNull)

		// Check primary key constraint accurately
		assert.Equal(t, 1, len(table.PrimaryKeys))
		assert.Equal(t, colIdMapped, table.PrimaryKeys[0].ColId)
		
		// Check JSON check constraint structures organically mapped
		assert.Equal(t, 1, len(table.CheckConstraints))
		assert.Equal(t, "(IS JSON)", table.CheckConstraints[0].Expr)
	}
}
