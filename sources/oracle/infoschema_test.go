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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
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

	null := sql.NullInt64{}
	val := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	tests := []struct {
		name      string
		dataType  string
		length    sql.NullInt64
		precision sql.NullInt64
		scale     sql.NullInt64
		expected  schema.Type
	}{
		{"NUMBER(9)", "NUMBER", val(0), val(9), val(0), schema.Type{Name: "NUMBER", Mods: []int64{9}}},
		{"NUMBER(10,2)", "NUMBER", val(0), val(10), val(2), schema.Type{Name: "NUMBER", Mods: []int64{10, 2}}},
		{"NUMBER(5,-2)", "NUMBER", val(0), val(5), val(-2), schema.Type{Name: "NUMBER", Mods: []int64{5, -2}}},
		{"NUMBER", "NUMBER", val(0), null, null, schema.Type{Name: "NUMBER"}},
		{"INTEGER / NUMBER(*,0)", "NUMBER", val(0), null, val(0), schema.Type{Name: "INTEGER"}},
		{"NUMBER(*,2)", "NUMBER", val(0), null, val(2), schema.Type{Name: "NUMBER"}},
		{"FLOAT(126)", "FLOAT", val(0), val(126), null, schema.Type{Name: "FLOAT", Mods: []int64{126}}},
		{"BINARY_DOUBLE", "BINARY_DOUBLE", val(0), null, null, schema.Type{Name: "BINARY_DOUBLE"}},
		{"INTERVAL DAY(3) TO SECOND(2)", "INTERVAL DAY(3) TO SECOND(2)", val(0), val(3), val(2), schema.Type{Name: "INTERVAL DAY(3) TO SECOND(2)"}},
		{"INTERVAL YEAR(4) TO MONTH", "INTERVAL YEAR(4) TO MONTH", val(0), val(4), val(0), schema.Type{Name: "INTERVAL YEAR(4) TO MONTH"}},
		{"VARCHAR2(100 CHAR)", "VARCHAR2", val(100), null, null, schema.Type{Name: "VARCHAR2", Mods: []int64{100}}},
		{"RAW(16)", "RAW", val(16), null, null, schema.Type{Name: "RAW", Mods: []int64{16}}},
		{"BLOB has no length", "BLOB", val(4000), null, null, schema.Type{Name: "BLOB"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, toType(tc.dataType, tc.length, tc.precision, tc.scale))
		})
	}
}

func TestGetSelectQueryNumbersAsText(t *testing.T) {
	colDefs := map[string]schema.Column{
		"c1": {Name: "N", Type: schema.Type{Name: "NUMBER", Mods: []int64{10, 2}}},
		"c2": {Name: "I", Type: schema.Type{Name: "INTEGER"}},
		"c3": {Name: "F", Type: schema.Type{Name: "FLOAT", Mods: []int64{126}}},
		"c4": {Name: "BD", Type: schema.Type{Name: "BINARY_DOUBLE"}},
	}
	q := getSelectQuery("DB", "SCH", "T", []string{"c1", "c2", "c3", "c4"}, colDefs)
	assert.Equal(t, `SELECT TO_CHAR("N") AS "N", TO_CHAR("I") AS "I", TO_CHAR("F") AS "F", "BD" FROM "SCH"."T"`, q)
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
			query: regexp.QuoteMeta(`SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT, CASE WHEN DATA_TYPE = 'RAW' THEN DATA_LENGTH ELSE CHAR_LENGTH END AS COL_LENGTH, DATA_PRECISION, DATA_SCALE, IDENTITY_COLUMN, VIRTUAL_COLUMN 
		FROM ALL_TAB_COLS 
		WHERE OWNER = :1 AND TABLE_NAME IN (:2) 
		ORDER BY TABLE_NAME, COLUMN_ID`),
			args: []driver.Value{"TESTDB", "TEST_TABLE"},
			cols: []string{"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "NULLABLE", "DATA_DEFAULT", "COL_LENGTH", "DATA_PRECISION", "DATA_SCALE", "IDENTITY_COLUMN", "VIRTUAL_COLUMN"},
			rows: [][]driver.Value{
				{"TEST_TABLE", "ID", "NUMBER", "N", `"TESTDB"."ISEQ$$_123".nextval`, nil, 38, 0, "YES", "NO"},
				{"TEST_TABLE", "NAME", "VARCHAR2", "Y", "'ACTIVE' ", 50, nil, nil, "NO", "NO"},
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
		JOIN all_cons_columns B ON B.owner = C.owner AND B.constraint_name = C.r_constraint_name AND B.position = A.position
		WHERE A.owner = :1 AND A.table_name IN (:2)
		ORDER BY A.table_name, A.constraint_name, A.position`),
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
		AND NOT EXISTS (
			SELECT 1 FROM all_constraints C
			WHERE C.constraint_type = 'P' AND C.owner = IC.table_owner AND C.table_name = IC.table_name
			AND C.index_owner = IC.index_owner AND C.index_name = IC.index_name
			AND (SELECT COUNT(*) FROM all_ind_columns X WHERE X.index_owner = IC.index_owner AND X.index_name = IC.index_name)
				= (SELECT COUNT(*) FROM all_cons_columns CC WHERE CC.owner = C.owner AND CC.constraint_name = C.constraint_name)
			AND NOT EXISTS (
				SELECT 1 FROM all_ind_columns X
				WHERE X.index_owner = IC.index_owner AND X.index_name = IC.index_name
				AND NOT EXISTS (
					SELECT 1 FROM all_cons_columns CC
					WHERE CC.owner = C.owner AND CC.constraint_name = C.constraint_name
					AND CC.position = X.column_position AND CC.column_name = X.column_name)))
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
		assert.Equal(t, false, table.ColDefs[colIdMapped].DefaultValue.IsPresent)
		assert.Equal(t, false, table.ColDefs[colIdMapped].Ignored.Default)
		
		colNameMapped := table.ColNameIdMap["NAME"]
		assert.Equal(t, "VARCHAR2", table.ColDefs[colNameMapped].Type.Name)
		assert.Equal(t, false, table.ColDefs[colNameMapped].NotNull)
		assert.Equal(t, true, table.ColDefs[colNameMapped].DefaultValue.IsPresent)
		assert.Equal(t, "'ACTIVE'", table.ColDefs[colNameMapped].DefaultValue.Value.Statement)

		// Check primary key constraint accurately
		assert.Equal(t, 1, len(table.PrimaryKeys))
		assert.Equal(t, colIdMapped, table.PrimaryKeys[0].ColId)
		
		// Check JSON check constraint structures organically mapped
		assert.Equal(t, 1, len(table.CheckConstraints))
		assert.Equal(t, "(IS JSON)", table.CheckConstraints[0].Expr)
	}
}

func TestToSpannerDefault(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		dataType string
		gsqlStmt string
		pgStmt   string
		ok       bool
	}{
		{"numeric zero", "0\n", "NUMBER", "0", "0", true},
		{"negative number", " -1 ", "NUMBER", "-1", "-1", true},
		{"decimal", "1.5", "NUMBER", "1.5", "1.5", true},
		{"paren number", "(5)", "NUMBER", "(5)", "(5)", true},
		{"bare NUMBER numeric literal", "42", "NUMBER", "42", "42", true},
		{"string literal", "'ACTIVE' ", "VARCHAR2", "'ACTIVE'", "'ACTIVE'", true},
		{"string with escaped quote", "'it''s'", "VARCHAR2", `'it\'s'`, "'it''s'", true},
		{"string with backslash", `'a\b'`, "VARCHAR2", `'a\\b'`, `'a\b'`, true},
		{"null keyword", "NULL\n", "VARCHAR2", "", "", true},
		{"null lowercase in parens", "(null)", "VARCHAR2", "", "", true},
		{"empty string literal (null in Oracle)", "''", "VARCHAR2", "", "", true},
		{"whitespace only", "   \n", "VARCHAR2", "", "", true},
		{"SYSDATE", "SYSDATE", "DATE", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"current_date", "current_date", "DATE", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"SYSTIMESTAMP", "SYSTIMESTAMP", "TIMESTAMP(6)", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"SYSTIMESTAMP(3)", "SYSTIMESTAMP(3)", "TIMESTAMP(3)", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", "TIMESTAMP(6)", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"CURRENT_TIMESTAMP(6)", "CURRENT_TIMESTAMP(6)", "TIMESTAMP(6)", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"LOCALTIMESTAMP", "LOCALTIMESTAMP", "TIMESTAMP(6)", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"LOCALTIMESTAMP(3)", "LOCALTIMESTAMP(3)", "TIMESTAMP(3)", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"paren SYSDATE", "(SYSDATE)", "DATE", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP", true},
		{"DATE literal on DATE column", "DATE '2020-01-01'", "DATE", "TIMESTAMP '2020-01-01 00:00:00'", "'2020-01-01 00:00:00'::timestamptz", true},
		{"TO_DATE function", "TO_DATE('2020-01-01', 'YYYY-MM-DD')", "DATE", "", "", false},
		{"sequence nextval quoted", `"SMT"."ORD_SEQ"."NEXTVAL"`, "NUMBER", "", "", false},
		{"sequence nextval unquoted", "ord_seq.nextval", "NUMBER", "", "", false},
		{"sequence nextval in parens", `(ord_seq.nextval)`, "NUMBER", "", "", false},
		{"unsupported function passed to verifier", "SYS_GUID()", "RAW", "SYS_GUID()", "SYS_GUID()", true},
		{"date arithmetic passed to verifier", "SYSDATE + 1", "DATE", "SYSDATE + 1", "SYSDATE + 1", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, ok := toSpannerDefault(tc.raw, tc.dataType, constants.DIALECT_GOOGLESQL)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.gsqlStmt, stmt)

			pgStmt, pgOk := toSpannerDefault(tc.raw, tc.dataType, constants.DIALECT_POSTGRESQL)
			assert.Equal(t, tc.ok, pgOk)
			assert.Equal(t, tc.pgStmt, pgStmt)
		})
	}
}

func TestGetColumnsBatchDefaults(t *testing.T) {
	ms := []mockSpec{
		{
			query: regexp.QuoteMeta(`SELECT COUNT(*) FROM all_tab_cols WHERE table_name = 'ALL_TAB_COLS' AND column_name = 'IDENTITY_COLUMN'`),
			cols:  []string{"COUNT"},
			rows:  [][]driver.Value{{1}},
		},
		{
			query: regexp.QuoteMeta(`SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT, CASE WHEN DATA_TYPE = 'RAW' THEN DATA_LENGTH ELSE CHAR_LENGTH END AS COL_LENGTH, DATA_PRECISION, DATA_SCALE, IDENTITY_COLUMN, VIRTUAL_COLUMN 
		FROM ALL_TAB_COLS 
		WHERE OWNER = :1 AND TABLE_NAME IN (:2) 
		ORDER BY TABLE_NAME, COLUMN_ID`),
			args: []driver.Value{"TESTDB", "DEFS"},
			cols: []string{"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "NULLABLE", "DATA_DEFAULT", "COL_LENGTH", "DATA_PRECISION", "DATA_SCALE", "IDENTITY_COLUMN", "VIRTUAL_COLUMN"},
			rows: [][]driver.Value{
				{"DEFS", "ID", "NUMBER", "N", `"TESTDB"."ISEQ$$_1".nextval`, nil, 10, 0, "YES", "NO"},
				{"DEFS", "STATUS", "VARCHAR2", "N", "'ACTIVE' ", 20, nil, nil, "NO", "NO"},
				{"DEFS", "CREATED_AT", "TIMESTAMP(6)", "N", "SYSTIMESTAMP", nil, nil, 6, "NO", "NO"},
				{"DEFS", "SEQ_COL", "NUMBER", "N", `"TESTDB"."ORD_SEQ"."NEXTVAL"`, nil, 10, 0, "NO", "NO"},
				{"DEFS", "NULL_DEF", "VARCHAR2", "Y", "NULL", 20, nil, nil, "NO", "NO"},
				{"DEFS", "VIRT_COL", "NUMBER", "Y", `"PRICE"*"QTY"`, nil, nil, nil, "NO", "YES"},
			},
		},
	}
	db := mkMockDB(t, ms)
	defer db.Close()

	conv := internal.MakeConv()
	conv.SpDialect = constants.DIALECT_GOOGLESQL
	isi := InfoSchemaImpl{DbName: "testdb", Db: db}
	cols, err := isi.GetColumnsBatch(conv, []common.SchemaAndName{{Schema: "TESTDB", Name: "DEFS"}})
	assert.Nil(t, err)

	byName := map[string]schema.Column{}
	for _, c := range cols["DEFS"].ColDefs {
		byName[c.Name] = c
	}

	assert.False(t, byName["ID"].DefaultValue.IsPresent)
	assert.False(t, byName["ID"].Ignored.Default)

	assert.True(t, byName["STATUS"].DefaultValue.IsPresent)
	assert.Equal(t, "'ACTIVE'", byName["STATUS"].DefaultValue.Value.Statement)

	assert.True(t, byName["CREATED_AT"].DefaultValue.IsPresent)
	assert.Equal(t, "CURRENT_TIMESTAMP()", byName["CREATED_AT"].DefaultValue.Value.Statement)

	assert.False(t, byName["SEQ_COL"].DefaultValue.IsPresent)
	assert.True(t, byName["SEQ_COL"].Ignored.Default)

	assert.False(t, byName["NULL_DEF"].DefaultValue.IsPresent)
	assert.False(t, byName["NULL_DEF"].Ignored.Default)

	assert.False(t, byName["VIRT_COL"].DefaultValue.IsPresent)
	assert.False(t, byName["VIRT_COL"].Ignored.Default)
	assert.True(t, byName["VIRT_COL"].GeneratedColumn.IsPresent)
	assert.Equal(t, ddl.GeneratedColVirtual, byName["VIRT_COL"].GeneratedColumn.Type)
	assert.Equal(t, "PRICE*QTY", byName["VIRT_COL"].GeneratedColumn.Value.Statement)
}

func TestToSpannerGeneratedExpr(t *testing.T) {
	colDefs := map[string]schema.Column{
		"c1": {Id: "c1", Name: "PRICE"},
		"c2": {Id: "c2", Name: "QTY"},
		"c3": {Id: "c3", Name: "FIRST_NAME"},
		"c4": {Id: "c4", Name: "LAST_NAME"},
		"c5": {Id: "c5", Name: "BAD$COL"},
	}
	tests := []struct {
		name     string
		raw      string
		gsqlStmt string
		pgStmt   string
	}{
		{"arithmetic", `"PRICE"*"QTY"`, "PRICE*QTY", `"PRICE"*"QTY"`},
		{"outer parens stripped", `("PRICE" + "QTY")`, "PRICE + QTY", `"PRICE" + "QTY"`},
		{"string concat", `"FIRST_NAME"||' '||"LAST_NAME"`, "FIRST_NAME||' '||LAST_NAME", `"FIRST_NAME"||' '||"LAST_NAME"`},
		{"function call", `UPPER("FIRST_NAME")`, "UPPER(FIRST_NAME)", `UPPER("FIRST_NAME")`},
		{"quotes and escapes inside string literal", `"FIRST_NAME"||' "quoted" it''s a\b'`, `FIRST_NAME||' "quoted" it\'s a\\b'`, `"FIRST_NAME"||' "quoted" it''s a\b'`},
		{"renamed column via FixName", `"BAD$COL"+1`, "BAD_COL+1", `"BAD_COL"+1`},
		{"empty expression", "   ", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.gsqlStmt, toSpannerGeneratedExpr(tc.raw, colDefs, constants.DIALECT_GOOGLESQL))
			assert.Equal(t, tc.pgStmt, toSpannerGeneratedExpr(tc.raw, colDefs, constants.DIALECT_POSTGRESQL))
		})
	}
}

func TestGetForeignKeysBatchComposite(t *testing.T) {
	// Rows as returned by Oracle once the referencing and referenced columns are paired
	// by position: one row per column pair of each foreign key.
	ms := []mockSpec{
		{
			query: regexp.QuoteMeta(`SELECT 
			A.table_name,
			B.table_name AS ref_table, 
			A.column_name AS col_name,
			B.column_name AS ref_col_name,
			A.constraint_name AS name
		FROM all_cons_columns A 
		JOIN all_constraints C ON A.owner = C.owner AND A.constraint_name = C.constraint_name
		JOIN all_cons_columns B ON B.owner = C.owner AND B.constraint_name = C.r_constraint_name AND B.position = A.position
		WHERE A.owner = :1 AND A.table_name IN (:2)
		ORDER BY A.table_name, A.constraint_name, A.position`),
			args: []driver.Value{"TESTDB", "CHILD"},
			cols: []string{"table_name", "ref_table", "col_name", "ref_col_name", "name"},
			rows: [][]driver.Value{
				{"CHILD", "PARENT", "REGION_ID", "REGION_ID", "CHILD_FK"},
				{"CHILD", "PARENT", "ITEM_ID", "ITEM_ID", "CHILD_FK"},
				{"CHILD", "OTHER", "OTHER_ID", "ID", "CHILD_OTHER_FK"},
			},
		},
	}
	db := mkMockDB(t, ms)
	isi := InfoSchemaImpl{DbName: "testdb", Db: db}
	result, err := isi.GetForeignKeysBatch(internal.MakeConv(), []common.SchemaAndName{{Schema: "TESTDB", Name: "CHILD"}})
	assert.Nil(t, err)

	fks := map[string]schema.ForeignKey{}
	for _, fk := range result["CHILD"].ForeignKeys {
		fks[fk.Name] = fk
	}
	assert.Equal(t, 2, len(fks))
	assert.Equal(t, []string{"REGION_ID", "ITEM_ID"}, fks["CHILD_FK"].ColumnNames)
	assert.Equal(t, []string{"REGION_ID", "ITEM_ID"}, fks["CHILD_FK"].ReferColumnNames)
	assert.Equal(t, "PARENT", fks["CHILD_FK"].ReferTableName)
	assert.Equal(t, []string{"OTHER_ID"}, fks["CHILD_OTHER_FK"].ColumnNames)
	assert.Equal(t, []string{"ID"}, fks["CHILD_OTHER_FK"].ReferColumnNames)
}

func TestGetIndexesBatchExpressions(t *testing.T) {
	ms := []mockSpec{
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
		AND NOT EXISTS (
			SELECT 1 FROM all_constraints C
			WHERE C.constraint_type = 'P' AND C.owner = IC.table_owner AND C.table_name = IC.table_name
			AND C.index_owner = IC.index_owner AND C.index_name = IC.index_name
			AND (SELECT COUNT(*) FROM all_ind_columns X WHERE X.index_owner = IC.index_owner AND X.index_name = IC.index_name)
				= (SELECT COUNT(*) FROM all_cons_columns CC WHERE CC.owner = C.owner AND CC.constraint_name = C.constraint_name)
			AND NOT EXISTS (
				SELECT 1 FROM all_ind_columns X
				WHERE X.index_owner = IC.index_owner AND X.index_name = IC.index_name
				AND NOT EXISTS (
					SELECT 1 FROM all_cons_columns CC
					WHERE CC.owner = C.owner AND CC.constraint_name = C.constraint_name
					AND CC.position = X.column_position AND CC.column_name = X.column_name)))
		ORDER BY IC.table_name, IC.index_name, IC.column_position`),
			args: []driver.Value{"TESTDB", "VCOLS"},
			cols: []string{"table_name", "index_name", "column_name", "descend", "uniqueness", "column_expression", "index_type"},
			rows: [][]driver.Value{
				{"VCOLS", "IDX_NORM", "QTY", "ASC", "NONUNIQUE", nil, "NORMAL"},
				{"VCOLS", "IDX_DESC", "SYS_NC00012$", "DESC", "NONUNIQUE", `"FIRST_NAME"`, "FUNCTION-BASED NORMAL"},
				{"VCOLS", "IDX_DESC", "QTY", "ASC", "NONUNIQUE", nil, "FUNCTION-BASED NORMAL"},
				{"VCOLS", "IDX_VCOL", "TOTAL", "ASC", "NONUNIQUE", `"PRICE"*"QTY"`, "FUNCTION-BASED NORMAL"},
				{"VCOLS", "IDX_FUNC", "SYS_NC00014$", "ASC", "NONUNIQUE", `LOWER("LAST_NAME")`, "FUNCTION-BASED NORMAL"},
				{"VCOLS", "IDX_ARITH", "SYS_NC00013$", "ASC", "NONUNIQUE", `"PRICE"+"QTY"`, "FUNCTION-BASED NORMAL"},
				{"VCOLS", "IDX_MIXED", "QTY", "ASC", "NONUNIQUE", nil, "FUNCTION-BASED NORMAL"},
				{"VCOLS", "IDX_MIXED", "SYS_NC00015$", "ASC", "NONUNIQUE", `"PRICE"+"QTY"`, "FUNCTION-BASED NORMAL"},
			},
		},
	}
	db := mkMockDB(t, ms)
	defer db.Close()

	isi := InfoSchemaImpl{DbName: "testdb", Db: db}
	colDefs := map[string]common.TableColumns{
		"VCOLS": {
			ColDefs: map[string]schema.Column{
				"c1": {Id: "c1", Name: "FIRST_NAME"},
				"c2": {Id: "c2", Name: "LAST_NAME"},
				"c3": {Id: "c3", Name: "PRICE"},
				"c4": {Id: "c4", Name: "QTY"},
				"c5": {Id: "c5", Name: "TOTAL"},
				"c6": {Id: "c6", Name: "SYS_NC00012$"},
				"c7": {Id: "c7", Name: "SYS_NC00013$"},
			},
		},
	}
	result, err := isi.GetIndexesBatch(internal.MakeConv(), []common.SchemaAndName{{Schema: "TESTDB", Name: "VCOLS"}}, colDefs)
	assert.Nil(t, err)

	byName := map[string]schema.Index{}
	for _, idx := range result["VCOLS"] {
		byName[idx.Name] = idx
	}
	assert.Equal(t, 3, len(byName))
	assert.Equal(t, []schema.Key{{ColId: "c4", Desc: false}}, byName["IDX_NORM"].Keys)
	assert.Equal(t, []schema.Key{{ColId: "c1", Desc: true}, {ColId: "c4", Desc: false}}, byName["IDX_DESC"].Keys)
	assert.Equal(t, []schema.Key{{ColId: "c5", Desc: false}}, byName["IDX_VCOL"].Keys)
}

func TestGetConstraintsBatchCheckExpressions(t *testing.T) {
	query := regexp.QuoteMeta(`SELECT 
			k.table_name,
			k.column_name,
			t.constraint_type,
			t.search_condition,
			t.constraint_name
		FROM all_constraints t
		INNER JOIN all_cons_columns k
		ON (k.constraint_name = t.constraint_name AND k.owner = t.owner AND k.table_name = t.table_name)
		WHERE t.owner = :1 AND k.table_name IN (:2)
		ORDER BY k.table_name, k.position`)
	rows := [][]driver.Value{
		{"ORDERS", "QTY", "C", `"QTY" > 0`, "CK_QTY"},
		{"ORDERS", "STATUS", "C", `"STATUS" IN ('A', 'B') AND "NOTE" <> 'it''s'`, "CK_STATUS"},
	}

	for _, tc := range []struct {
		dialect      string
		expectedQty  string
		expectedStat string
	}{
		{
			dialect:      constants.DIALECT_GOOGLESQL,
			expectedQty:  "(QTY > 0)",
			expectedStat: `(STATUS IN ('A', 'B') AND NOTE <> 'it\'s')`,
		},
		{
			dialect:      constants.DIALECT_POSTGRESQL,
			expectedQty:  `("QTY" > 0)`,
			expectedStat: `("STATUS" IN ('A', 'B') AND "NOTE" <> 'it''s')`,
		},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			db := mkMockDB(t, []mockSpec{{
				query: query,
				args:  []driver.Value{"TESTDB", "ORDERS"},
				cols:  []string{"table_name", "column_name", "constraint_type", "search_condition", "constraint_name"},
				rows:  rows,
			}})
			defer db.Close()

			conv := internal.MakeConv()
			conv.SpDialect = tc.dialect
			isi := InfoSchemaImpl{DbName: "testdb", Db: db}
			res, err := isi.GetConstraintsBatch(conv, []common.SchemaAndName{{Schema: "TESTDB", Name: "ORDERS"}})
			assert.Nil(t, err)

			byName := map[string]string{}
			for _, ck := range res["ORDERS"].CheckConstraints {
				byName[ck.Name] = ck.Expr
			}
			assert.Equal(t, tc.expectedQty, byName["CK_QTY"])
			assert.Equal(t, tc.expectedStat, byName["CK_STATUS"])
		})
	}
}
