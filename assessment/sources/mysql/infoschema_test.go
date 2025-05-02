// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/stretchr/testify/assert"
)

// Helper to create InfoSchemaImpl with mock DB
func newTestInfoSchemaImpl(t *testing.T) (InfoSchemaImpl, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	assert.NoError(t, err)
	return InfoSchemaImpl{Db: db, DbName: "test_db"}, mock
}

func TestInfoSchemaImpl_GetTableInfo(t *testing.T) {
	tableQueryRegex := `SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET FROM INFORMATION_SCHEMA\.TABLES WHERE TABLE_SCHEMA = \? AND TABLE_NAME = \?`
	columnQueryRegex := `SELECT c\.column_type, c\.extra, c\.generation_expression\s+FROM information_schema\.COLUMNS c\s+where table_schema = \? and table_name = \? and column_name = \?\s+ORDER BY c\.ordinal_position;`

	type testCase struct {
		name            string
		conv            *internal.Conv
		dbName          string
		mockTableSetup  func(mock sqlmock.Sqlmock, tableName string, dbName string)
		mockColumnSetup func(mock sqlmock.Sqlmock, tableName, colName, dbName string)
		checkSpecifics  func(t *testing.T, result map[string]utils.TableAssessmentInfo, tableID, colID string)
	}

	testCases := []testCase{
		{
			name: "Success - INT column",
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{"t1": {Name: "table1", Id: "t1", ColDefs: map[string]schema.Column{
					"c1": {Name: "col1", Id: "c1", Type: schema.Type{Name: "INT"}},
				}}},
			},
			dbName: "test_db",
			mockTableSetup: func(mock sqlmock.Sqlmock, tableName string, dbName string) {
				mock.ExpectQuery(tableQueryRegex).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
			},
			mockColumnSetup: func(mock sqlmock.Sqlmock, tableName, colName, dbName string) {
				mock.ExpectQuery(columnQueryRegex).
					WithArgs(dbName, tableName, colName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("int(11)", sql.NullString{}, sql.NullString{}))
			},
			checkSpecifics: func(t *testing.T, result map[string]utils.TableAssessmentInfo, tableID, colID string) {
				assert.Len(t, result, 1)
				tableInfo, ok := result[tableID]
				assert.True(t, ok)
				assert.Equal(t, "table1", tableInfo.Name)
				assert.Equal(t, "utf8mb4", tableInfo.Charset)
				assert.Len(t, tableInfo.ColumnAssessmentInfos, 1)

				colInfo, ok := tableInfo.ColumnAssessmentInfos[colID]
				assert.True(t, ok)
				assert.Equal(t, "col1", colInfo.Name)
				assert.False(t, colInfo.IsUnsigned)
				assert.False(t, colInfo.IsOnUpdateTimestampSet)
				assert.False(t, colInfo.GeneratedColumn.IsPresent)
				assert.Equal(t, getColumnMaxSize("INT", nil, "utf8mb4"), colInfo.MaxColumnSize)
			},
		},
		{
			name: "Success - VARCHAR VIRTUAL GENERATED column",
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{"t_vgen": {Name: "table_vgen", Id: "t_vgen", ColDefs: map[string]schema.Column{
					"c_vgen": {Name: "col_vgen", Id: "c_vgen", Type: schema.Type{Name: "VARCHAR", Mods: []int64{100}}},
				}}},
			},
			dbName: "test_db_latin1",
			mockTableSetup: func(mock sqlmock.Sqlmock, tableName string, dbName string) {
				mock.ExpectQuery(tableQueryRegex).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("latin1_swedish_ci", "latin1"))
			},
			mockColumnSetup: func(mock sqlmock.Sqlmock, tableName, colName, dbName string) {
				mock.ExpectQuery(columnQueryRegex).
					WithArgs(dbName, tableName, colName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).
						AddRow("varchar(100)", sql.NullString{String: "VIRTUAL GENERATED", Valid: true}, sql.NullString{String: "UPPER(other_col)", Valid: true}))
			},
			checkSpecifics: func(t *testing.T, result map[string]utils.TableAssessmentInfo, tableID, colID string) {
				tableInfo := result[tableID]
				assert.Equal(t, "latin1", tableInfo.Charset)
				colInfo := tableInfo.ColumnAssessmentInfos[colID]
				assert.Equal(t, "col_vgen", colInfo.Name)
				assert.True(t, colInfo.GeneratedColumn.IsPresent)
				assert.True(t, colInfo.GeneratedColumn.IsVirtual)
				assert.Equal(t, "UPPER(other_col)", colInfo.GeneratedColumn.Statement)
				assert.Equal(t, getColumnMaxSize("VARCHAR", []int64{100}, "latin1"), colInfo.MaxColumnSize)
			},
		},
		{
			name: "Success - TIMESTAMP ON UPDATE column",
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{"t_ts": {Name: "table_ts", Id: "t_ts", ColDefs: map[string]schema.Column{
					"c_ts": {Name: "col_ts", Id: "c_ts", Type: schema.Type{Name: "TIMESTAMP"}},
				}}},
			},
			dbName: "test_db",
			mockTableSetup: func(mock sqlmock.Sqlmock, tableName string, dbName string) {
				mock.ExpectQuery(tableQueryRegex).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8_general_ci", "utf8"))
			},
			mockColumnSetup: func(mock sqlmock.Sqlmock, tableName, colName, dbName string) {
				mock.ExpectQuery(columnQueryRegex).
					WithArgs(dbName, tableName, colName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).
						AddRow("timestamp", sql.NullString{String: "on update CURRENT_TIMESTAMP", Valid: true}, sql.NullString{}))
			},
			checkSpecifics: func(t *testing.T, result map[string]utils.TableAssessmentInfo, tableID, colID string) {
				colInfo := result[tableID].ColumnAssessmentInfos[colID]
				assert.True(t, colInfo.IsOnUpdateTimestampSet)
			},
		},
		{
			name: "Success - INT UNSIGNED column",
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{"t_uint": {Name: "table_uint", Id: "t_uint", ColDefs: map[string]schema.Column{
					"c_uint": {Name: "col_unsigned", Id: "c_uint", Type: schema.Type{Name: "INT"}}, // SUT checks 'unsigned' in colType string
				}}},
			},
			dbName: "test_db",
			mockTableSetup: func(mock sqlmock.Sqlmock, tableName string, dbName string) {
				mock.ExpectQuery(tableQueryRegex).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_bin", "utf8mb4"))
			},
			mockColumnSetup: func(mock sqlmock.Sqlmock, tableName, colName, dbName string) {
				mock.ExpectQuery(columnQueryRegex).
					WithArgs(dbName, tableName, colName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).
						AddRow("int(10) unsigned", sql.NullString{}, sql.NullString{}))
			},
			checkSpecifics: func(t *testing.T, result map[string]utils.TableAssessmentInfo, tableID, colID string) {
				colInfo := result[tableID].ColumnAssessmentInfos[colID]
				assert.True(t, colInfo.IsUnsigned)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isi, mock := newTestInfoSchemaImpl(t)
			isi.DbName = tc.dbName
			defer isi.Db.Close()

			currentTable, currentTableID := getFirstTable(tc.conv)
			currentColumn, currentColumnID := getFirstColumn(currentTable)

			if tc.mockTableSetup != nil {
				tc.mockTableSetup(mock, currentTable.Name, tc.dbName)
			}
			if tc.mockColumnSetup != nil && currentColumn.Name != "" {
				tc.mockColumnSetup(mock, currentTable.Name, currentColumn.Name, tc.dbName)
			}

			result, err := isi.GetTableInfo(tc.conv)

			assert.NoError(t, err)
			if tc.checkSpecifics != nil {
				tc.checkSpecifics(t, result, currentTableID, currentColumnID)
			}

			err = mock.ExpectationsWereMet()
			assert.NoError(t, err, "SQLMock expectations not met")
		})
	}
}

func getFirstTable(conv *internal.Conv) (schema.Table, string) {
	for id, tbl := range conv.SrcSchema {
		return tbl, id
	}
	return schema.Table{}, ""
}

func getFirstColumn(tbl schema.Table) (schema.Column, string) {
	for id, col := range tbl.ColDefs {
		return col, id
	}
	return schema.Column{}, ""
}

func TestInfoSchemaImpl_GetTableInfoErrorCases(t *testing.T) {
	tableQueryRegex := `SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET FROM INFORMATION_SCHEMA\.TABLES WHERE TABLE_SCHEMA = \? AND TABLE_NAME = \?`
	columnQueryRegex := `SELECT c\.column_type, c\.extra, c\.generation_expression\s+FROM information_schema\.COLUMNS c\s+where table_schema = \? and table_name = \? and column_name = \?\s+ORDER BY c\.ordinal_position;`

	type testCase struct {
		name               string
		conv               *internal.Conv
		dbName             string
		mockTableSetup     func(mock sqlmock.Sqlmock, tableName string, dbName string)
		mockColumnSetup    func(mock sqlmock.Sqlmock, tableName, colName, dbName string)
		wantErrMsgContains string
	}

	testCases := []testCase{
		{
			name: "Error querying table info",
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{"t_err": {Name: "table_err", Id: "t_err", ColDefs: map[string]schema.Column{"c_err": {Name: "col_err", Id: "c_err"}}}},
			},
			dbName: "test_db",
			mockTableSetup: func(mock sqlmock.Sqlmock, tableName string, dbName string) {
				mock.ExpectQuery(tableQueryRegex).
					WithArgs(dbName, tableName).
					WillReturnError(errors.New("db error for table"))
			},
			mockColumnSetup:    nil,
			wantErrMsgContains: "couldn't get schema for table table_err: db error for table",
		},
		{
			name: "Error querying column info",
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{"t_col_err": {Name: "table_col_err", Id: "t_col_err", ColDefs: map[string]schema.Column{"c_col_err": {Name: "col_for_err", Id: "c_col_err"}}}},
			},
			dbName: "test_db",
			mockTableSetup: func(mock sqlmock.Sqlmock, tableName string, dbName string) {
				mock.ExpectQuery(tableQueryRegex).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
			},
			mockColumnSetup: func(mock sqlmock.Sqlmock, tableName, colName, dbName string) {
				mock.ExpectQuery(columnQueryRegex).
					WithArgs(dbName, tableName, colName).
					WillReturnError(errors.New("db error for column"))
			},
			wantErrMsgContains: "couldn't get schema for column table_col_err.col_for_err: db error for column",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isi, mock := newTestInfoSchemaImpl(t)
			isi.DbName = tc.dbName
			defer isi.Db.Close()

			currentTable, _ := getFirstTable(tc.conv)
			currentColumn, _ := getFirstColumn(currentTable)

			if tc.mockTableSetup != nil {
				tc.mockTableSetup(mock, currentTable.Name, tc.dbName)
			}
			if tc.mockColumnSetup != nil && currentColumn.Name != "" {
				tc.mockColumnSetup(mock, currentTable.Name, currentColumn.Name, tc.dbName)
			}

			_, err := isi.GetTableInfo(tc.conv)

			if tc.wantErrMsgContains != "" {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tc.wantErrMsgContains)
				}
			} else {
				assert.NoError(t, err)
			}
			err = mock.ExpectationsWereMet()
			assert.NoError(t, err, "SQLMock expectations not met")
		})
	}
}

func TestInfoSchemaImpl_GetIndexInfo(t *testing.T) {
	type testCase struct {
		name               string
		tableName          string
		indexInput         schema.Index
		dbName             string
		setupMock          func(mock sqlmock.Sqlmock)
		expectedIndexInfo  utils.IndexAssessmentInfo
		wantErrMsgContains string
	}

	testCases := []testCase{
		{
			name:       "Success",
			tableName:  "my_table",
			indexInput: schema.Index{Name: "my_index", Id: "idx_id1"},
			dbName:     "test_db",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE", "INDEX_TYPE"}).
					AddRow("my_index", "col1", "1", sql.NullString{String: "A", Valid: true}, "0", "BTREE")
				mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX;")).
					WithArgs("test_db", "my_table", "my_index").
					WillReturnRows(rows)
			},
			expectedIndexInfo: utils.IndexAssessmentInfo{
				Ty:   "BTREE",
				Name: "my_index",
				Db:   utils.DbIdentifier{DatabaseName: "test_db"},
				IndexDef: schema.Index{
					Name: "my_index", Id: "idx_id1",
				},
			},
		},
		{
			name:       "DB Error",
			tableName:  "my_table",
			indexInput: schema.Index{Name: "my_index", Id: "idx_id1"},
			dbName:     "test_db",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE FROM INFORMATION_SCHEMA.STATISTICS")).
					WithArgs("test_db", "my_table", "my_index").
					WillReturnError(errors.New("index db error"))
			},
			wantErrMsgContains: "couldn't get index for index name my_table.my_index: index db error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isi, mock := newTestInfoSchemaImpl(t)
			isi.DbName = tc.dbName
			defer isi.Db.Close()

			if tc.setupMock != nil {
				tc.setupMock(mock)
			}

			result, err := isi.GetIndexInfo(tc.tableName, tc.indexInput)

			if tc.wantErrMsgContains != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsgContains)
			} else {
				assert.NoError(t, err)
				expected := tc.expectedIndexInfo
				expected.Db = utils.DbIdentifier{DatabaseName: tc.dbName}
				expected.IndexDef.Name = tc.indexInput.Name
				expected.IndexDef.Id = tc.indexInput.Id
				assert.Equal(t, expected, result)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestInfoSchemaImpl_GetTriggerInfo(t *testing.T) {
	queryRegex := regexp.QuoteMeta(`SELECT DISTINCT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT,ACTION_TIMING,EVENT_MANIPULATION FROM INFORMATION_SCHEMA.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = ?`)

	type testCase struct {
		name               string
		dbName             string
		setupMock          func(mock sqlmock.Sqlmock, dbName string)
		expectedResult     []utils.TriggerAssessmentInfo
		wantErrMsgContains string
	}

	testCases := []testCase{
		{
			name:   "Success with multiple triggers",
			dbName: "test_db_multi_trigger",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"TRIGGER_NAME", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "EVENT_MANIPULATION"}).
					AddRow("trigger1", "table1", "INSERT", "BEFORE", "INSERT").
					AddRow("trigger2", "table2", "UPDATE", "AFTER", "UPDATE")
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.TriggerAssessmentInfo{
				{Name: "trigger1", TargetTable: "table1", Operation: "INSERT", ActionTiming: "BEFORE", EventManipulation: "INSERT", Db: utils.DbIdentifier{DatabaseName: "test_db_multi_trigger"}},
				{Name: "trigger2", TargetTable: "table2", Operation: "UPDATE", ActionTiming: "AFTER", EventManipulation: "UPDATE", Db: utils.DbIdentifier{DatabaseName: "test_db_multi_trigger"}},
			},
		},
		{
			name:   "Success with no triggers",
			dbName: "test_db_no_trigger",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"TRIGGER_NAME", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "EVENT_MANIPULATION"})
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.TriggerAssessmentInfo(nil),
		},
		{
			name:   "DB Query Error",
			dbName: "test_db_query_err",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnError(errors.New("trigger db query error"))
			},
			wantErrMsgContains: "trigger db query error",
		},
		{
			name:   "Scan Error skip problematic row",
			dbName: "test_db_scan_err",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"TRIGGER_NAME", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "EVENT_MANIPULATION"}).
					AddRow("trigger_ok_1", "table_ok_1", "DELETE", "INSTEAD OF", "DELETE").
					AddRow(nil, "", "", "", ""). // This row will cause scan error
					AddRow("trigger_ok_2", "table_ok_2", "SELECT", "BEFORE", "SELECT")
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.TriggerAssessmentInfo{ // Only successfully scanned rows
				{Name: "trigger_ok_1", TargetTable: "table_ok_1", Operation: "DELETE", ActionTiming: "INSTEAD OF", EventManipulation: "DELETE"},
				{Name: "trigger_ok_2", TargetTable: "table_ok_2", Operation: "SELECT", ActionTiming: "BEFORE", EventManipulation: "SELECT"},
			},
			wantErrMsgContains: "Can't scan",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isi, mock := newTestInfoSchemaImpl(t)
			isi.DbName = tc.dbName
			defer isi.Db.Close()

			if tc.setupMock != nil {
				tc.setupMock(mock, tc.dbName)
			}

			result, err := isi.GetTriggerInfo()

			if tc.wantErrMsgContains != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsgContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResult, result)
			}
			assert.NoError(t, mock.ExpectationsWereMet(), "SQLMock expectations not met")
		})
	}
}

func TestInfoSchemaImpl_GetStoredProcedureInfo(t *testing.T) {
	queryRegex := regexp.QuoteMeta(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='PROCEDURE' AND ROUTINE_SCHEMA = ?`)

	type testCase struct {
		name               string
		dbName             string
		setupMock          func(mock sqlmock.Sqlmock, dbName string)
		expectedResult     []utils.StoredProcedureAssessmentInfo
		wantErrMsgContains string
	}

	testCases := []testCase{
		{
			name:   "Success with multiple procedures",
			dbName: "test_db_multi_sp",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC"}).
					AddRow("sp1", "BEGIN END -- sp1", "YES").
					AddRow("sp2", "SELECT 1 -- sp2", "NO")
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.StoredProcedureAssessmentInfo{
				{Name: "sp1", Definition: "BEGIN END -- sp1", IsDeterministic: true, Db: utils.DbIdentifier{DatabaseName: "test_db_multi_sp"}},
				{Name: "sp2", Definition: "SELECT 1 -- sp2", IsDeterministic: false, Db: utils.DbIdentifier{DatabaseName: "test_db_multi_sp"}},
			},
		},
		{
			name:   "Success with no procedures",
			dbName: "test_db_no_sp",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC"})
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.StoredProcedureAssessmentInfo(nil),
		},
		{
			name:   "DB Query Error",
			dbName: "test_db_query_err_sp",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnError(errors.New("sp db query error"))
			},
			wantErrMsgContains: "sp db query error",
		},
		{
			name:   "Scan Error skip problematic row",
			dbName: "test_db_scan_err_sp",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC"}).
					AddRow("sp_ok_1", "DEF OK 1", "YES").
					AddRow(nil, "", "").
					AddRow("sp_ok_2", "DEF OK 2", "NO")
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.StoredProcedureAssessmentInfo{
				{Name: "sp_ok_1", Definition: "DEF OK 1", IsDeterministic: true},
				{Name: "sp_ok_2", Definition: "DEF OK 2", IsDeterministic: false},
			},
			wantErrMsgContains: "Can't scan",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isi, mock := newTestInfoSchemaImpl(t)
			isi.DbName = tc.dbName
			defer isi.Db.Close()

			if tc.setupMock != nil {
				tc.setupMock(mock, tc.dbName)
			}

			result, err := isi.GetStoredProcedureInfo()

			if tc.wantErrMsgContains != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsgContains)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResult, result)

			}
			assert.NoError(t, mock.ExpectationsWereMet(), "SQLMock expectations not met")
		})
	}
}

func TestInfoSchemaImpl_GetFunctionInfo(t *testing.T) {
	queryRegex := regexp.QuoteMeta(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC, DTD_IDENTIFIER FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='FUNCTION' AND ROUTINE_SCHEMA = ?`)

	type testCase struct {
		name               string
		dbName             string
		setupMock          func(mock sqlmock.Sqlmock, dbName string)
		expectedResult     []utils.FunctionAssessmentInfo
		wantErrMsgContains string
	}

	testCases := []testCase{
		{
			name:   "Success with multiple functions",
			dbName: "test_db_multi_func",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC", "DTD_IDENTIFIER"}).
					AddRow("func1", "RETURN 42", "YES", "INT").
					AddRow("func2", "RETURN 'hello'", "NO", "VARCHAR(255)")
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.FunctionAssessmentInfo{
				{Name: "func1", Definition: "RETURN 42", IsDeterministic: true, Datatype: "INT", Db: utils.DbIdentifier{DatabaseName: "test_db_multi_func"}},
				{Name: "func2", Definition: "RETURN 'hello'", IsDeterministic: false, Datatype: "VARCHAR(255)", Db: utils.DbIdentifier{DatabaseName: "test_db_multi_func"}},
			},
		},
		{
			name:   "Success with no functions",
			dbName: "test_db_no_func",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC", "DTD_IDENTIFIER"})
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.FunctionAssessmentInfo(nil),
		},
		{
			name:   "DB Query Error",
			dbName: "test_db_query_err_func",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnError(errors.New("func db query error"))
			},
			wantErrMsgContains: "func db query error",
		},
		{
			name:   "Scan Error skip problematic row",
			dbName: "test_db_scan_err_func",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC", "DTD_IDENTIFIER"}).
					AddRow("func_ok_1", "RETURN 10", "YES", "BIGINT").
					AddRow(nil, "", "", ""). // This row will cause scan error
					AddRow("func_ok_2", "RETURN 20", "NO", "SMALLINT")
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.FunctionAssessmentInfo{ // Only successfully scanned rows
				{Name: "func_ok_1", Definition: "RETURN 10", IsDeterministic: true, Datatype: "BIGINT", Db: utils.DbIdentifier{DatabaseName: "test_db_scan_err_func"}},
				{Name: "func_ok_2", Definition: "RETURN 20", IsDeterministic: false, Datatype: "SMALLINT", Db: utils.DbIdentifier{DatabaseName: "test_db_scan_err_func"}},
			},
			wantErrMsgContains: "Can't scan: sql: Scan",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isi, mock := newTestInfoSchemaImpl(t)
			isi.DbName = tc.dbName
			defer isi.Db.Close()

			if tc.setupMock != nil {
				tc.setupMock(mock, tc.dbName)
			}

			result, err := isi.GetFunctionInfo()

			if tc.wantErrMsgContains != "" {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tc.wantErrMsgContains)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResult, result)

			}
			assert.NoError(t, mock.ExpectationsWereMet(), "SQLMock expectations not met")
		})
	}
}

func TestInfoSchemaImpl_GetViewInfo(t *testing.T) {
	queryRegex := regexp.QuoteMeta(`SELECT DISTINCT TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION, IS_UPDATABLE FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ?`)

	type testCase struct {
		name               string
		dbName             string
		setupMock          func(mock sqlmock.Sqlmock, dbName string)
		expectedResult     []utils.ViewAssessmentInfo
		wantErrMsgContains string
	}

	testCases := []testCase{
		{
			name:   "Success with multiple views",
			dbName: "test_db_multi_view",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "IS_UPDATABLE"}).
					AddRow("view1", "SELECT c1 FROM t1", "NONE", "YES").
					AddRow("view2", "SELECT c2, c3 FROM t2", "CASCADED", "NO")
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.ViewAssessmentInfo{
				{Name: "view1", Definition: "SELECT c1 FROM t1", CheckOption: "NONE", IsUpdatable: true, Db: utils.DbIdentifier{DatabaseName: "test_db_multi_view"}},
				{Name: "view2", Definition: "SELECT c2, c3 FROM t2", CheckOption: "CASCADED", IsUpdatable: false, Db: utils.DbIdentifier{DatabaseName: "test_db_multi_view"}},
			},
		},
		{
			name:   "Success with no views",
			dbName: "test_db_no_view",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "IS_UPDATABLE"})
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.ViewAssessmentInfo(nil),
		},
		{
			name:   "DB Query Error",
			dbName: "test_db_query_err_view",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnError(errors.New("view db query error"))
			},
			wantErrMsgContains: "view db query error",
		},
		{
			name:   "Scan Error skip problematic row",
			dbName: "test_db_scan_err_view",
			setupMock: func(mock sqlmock.Sqlmock, dbName string) {
				rows := sqlmock.NewRows([]string{"TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "IS_UPDATABLE"}).
					AddRow("view_ok_1", "SELECT 1", "LOCAL", "YES").
					AddRow(nil, "", "", "").
					AddRow("view_ok_2", "SELECT 2", "NONE", "NO")
				mock.ExpectQuery(queryRegex).WithArgs(dbName).WillReturnRows(rows)
			},
			expectedResult: []utils.ViewAssessmentInfo{
				{Name: "view_ok_1", Definition: "SELECT 1", CheckOption: "LOCAL", IsUpdatable: true, Db: utils.DbIdentifier{DatabaseName: "test_db_scan_err_view"}},
				{Name: "view_ok_2", Definition: "SELECT 2", CheckOption: "NONE", IsUpdatable: false, Db: utils.DbIdentifier{DatabaseName: "test_db_scan_err_view"}},
			},
			wantErrMsgContains: "Can't scan: sql: Scan error on column index 0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isi, mock := newTestInfoSchemaImpl(t)
			isi.DbName = tc.dbName
			defer isi.Db.Close()

			if tc.setupMock != nil {
				tc.setupMock(mock, tc.dbName)
			}

			result, err := isi.GetViewInfo()

			if tc.wantErrMsgContains != "" {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), tc.wantErrMsgContains)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedResult, result)
			}
			assert.NoError(t, mock.ExpectationsWereMet(), "SQLMock expectations not met")
		})
	}
}

func TestGetMaxBytesPerChar(t *testing.T) {
	tests := []struct {
		name    string
		charset string
		want    int64
	}{
		{name: "latin1", charset: "latin1", want: 1},
		{name: "LATIN1 uppercase", charset: "LATIN1", want: 1},
		{name: "ascii", charset: "ascii", want: 1},
		{name: "cp850", charset: "cp850", want: 1},
		{name: "binary charset", charset: "binary", want: 1},
		{name: "ucs2", charset: "ucs2", want: 2},
		{name: "gbk", charset: "gbk", want: 2},
		{name: "sjis", charset: "sjis", want: 2},
		{name: "utf8", charset: "utf8", want: 3},
		{name: "utf8mb3", charset: "utf8mb3", want: 3},
		{name: "utf8mb4", charset: "utf8mb4", want: 4},
		{name: "utf16", charset: "utf16", want: 4},
		{name: "utf32", charset: "utf32", want: 4},
		{name: "gb18030", charset: "gb18030", want: 4},
		{name: "unknown charset", charset: "some_unknown_charset", want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getMaxBytesPerChar(tt.charset)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetColumnMaxSize_Simplified(t *testing.T) {
	tests := []struct {
		name         string
		dataType     string
		mods         []int64
		mysqlCharset string
		want         int64
	}{
		{name: "date", dataType: "date", mods: nil, mysqlCharset: "utf8mb4", want: 4},
		{name: "timestamp", dataType: "timestamp", mods: nil, mysqlCharset: "utf8mb4", want: 8},
		{name: "tinyint", dataType: "tinyint", mods: nil, mysqlCharset: "utf8mb4", want: 1},
		{name: "smallint", dataType: "smallint", mods: nil, mysqlCharset: "utf8mb4", want: 2},
		{name: "mediumint", dataType: "mediumint", mods: nil, mysqlCharset: "utf8mb4", want: 3},
		{name: "int", dataType: "int", mods: nil, mysqlCharset: "utf8mb4", want: 4},
		{name: "bigint", dataType: "bigint", mods: nil, mysqlCharset: "utf8mb4", want: 8},
		{name: "float", dataType: "float", mods: nil, mysqlCharset: "utf8mb4", want: 4},
		{name: "double", dataType: "double", mods: nil, mysqlCharset: "utf8mb4", want: 8},

		{name: "bit no mods", dataType: "bit", mods: nil, mysqlCharset: "utf8mb4", want: 1},
		{name: "bit with mods (BIT(9))", dataType: "bit", mods: []int64{9}, mysqlCharset: "utf8mb4", want: 2},

		{name: "decimal no mods", dataType: "decimal", mods: nil, mysqlCharset: "utf8mb4", want: 8},
		{name: "decimal with P,S (10,2)", dataType: "decimal", mods: []int64{10, 2}, mysqlCharset: "utf8mb4", want: 2},
		{name: "numeric with P only (5)", dataType: "numeric", mods: []int64{5}, mysqlCharset: "utf8mb4", want: 1},

		{name: "char no mods (latin1)", dataType: "char", mods: nil, mysqlCharset: "latin1", want: 1 * 1},
		{name: "char with mods (10, utf8mb4)", dataType: "char", mods: []int64{10}, mysqlCharset: "utf8mb4", want: 10 * 4},

		{name: "varchar no mods (latin1)", dataType: "varchar", mods: nil, mysqlCharset: "latin1", want: 255 * 1},
		{name: "varchar with mods (50, utf8mb4)", dataType: "varchar", mods: []int64{50}, mysqlCharset: "utf8mb4", want: 50 * 4},

		{name: "binary no mods", dataType: "binary", mods: nil, mysqlCharset: "utf8mb4", want: 255},
		{name: "binary with mods (100)", dataType: "binary", mods: []int64{100}, mysqlCharset: "utf8mb4", want: 100},

		{name: "varbinary no mods", dataType: "varbinary", mods: nil, mysqlCharset: "utf8mb4", want: 255},
		{name: "varbinary with mods (150)", dataType: "varbinary", mods: []int64{150}, mysqlCharset: "utf8mb4", want: 150},

		// BLOB types
		{name: "tinyblob", dataType: "tinyblob", mods: nil, mysqlCharset: "utf8mb4", want: 255},
		{name: "blob", dataType: "blob", mods: nil, mysqlCharset: "utf8mb4", want: 65535},
		{name: "mediumblob", dataType: "mediumblob", mods: nil, mysqlCharset: "utf8mb4", want: 16777215},
		{name: "longblob", dataType: "longblob", mods: nil, mysqlCharset: "utf8mb4", want: 4294967295},

		// TEXT types
		{name: "tinytext (utf8mb4)", dataType: "tinytext", mods: nil, mysqlCharset: "utf8mb4", want: 255 * 4},
		{name: "text (latin1)", dataType: "text", mods: nil, mysqlCharset: "latin1", want: 65535 * 1},
		{name: "mediumtext (utf8)", dataType: "mediumtext", mods: nil, mysqlCharset: "utf8", want: 16777215 * 3},
		{name: "longtext (ucs2)", dataType: "longtext", mods: nil, mysqlCharset: "ucs2", want: 4294967295 * 2},

		// JSON type
		{name: "json", dataType: "json", mods: nil, mysqlCharset: "utf8mb4", want: 4294967295},

		// Default case for unknown type
		{name: "unknown type", dataType: "unknown_type", mods: nil, mysqlCharset: "utf8mb4", want: 4},
		{name: "case test dataType (Int)", dataType: "Int", mods: nil, mysqlCharset: "utf8mb4", want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getColumnMaxSize(tt.dataType, tt.mods, tt.mysqlCharset)
			assert.Equal(t, tt.want, got, fmt.Sprintf("dataType: %s, mods: %v, charset: %s", tt.dataType, tt.mods, tt.mysqlCharset))
		})
	}
}

func TestSourceSpecificComparisonImpl_IsDataTypeCodeCompatible(t *testing.T) {
	ssa := SourceSpecificComparisonImpl{}

	tests := []struct {
		name        string
		srcDataType string
		spDataType  string
		want        bool
	}{
		// BOOL compatibility
		{"BOOL/tinyint", "tinyint", "BOOL", true},
		{"BOOL/bit", "bit", "BOOL", true},
		{"BOOL/other", "varchar", "BOOL", false},

		// BYTES compatibility
		{"BYTES/binary", "binary", "BYTES", true},
		{"BYTES/varbinary", "varbinary", "BYTES", true},
		{"BYTES/blob", "blob", "BYTES", true},
		{"BYTES/other", "varchar", "BYTES", false},

		// DATE compatibility
		{"DATE/date", "date", "DATE", true},
		{"DATE/other", "datetime", "DATE", false},

		// FLOAT32 compatibility
		{"FLOAT32/float", "float", "FLOAT32", true},
		{"FLOAT32/double", "double", "FLOAT32", true},
		{"FLOAT32/other", "varchar", "FLOAT32", false},

		// FLOAT64 compatibility
		{"FLOAT64/float", "float", "FLOAT64", true},
		{"FLOAT64/double", "double", "FLOAT64", true},
		{"FLOAT64/other", "varchar", "FLOAT64", false},

		// INT64 compatibility
		{"INT64/int", "int", "INT64", true},
		{"INT64/bigint", "bigint", "INT64", true},
		{"INT64/other", "varchar", "INT64", false},

		// JSON compatibility
		{"JSON/json", "json", "JSON", true},
		{"JSON/varchar", "varchar", "JSON", true},
		{"JSON/other", "text", "JSON", false},

		// NUMERIC compatibility
		{"NUMERIC/float", "float", "NUMERIC", true},
		{"NUMERIC/double", "double", "NUMERIC", true},
		{"NUMERIC/other", "varchar", "NUMERIC", false},

		// STRING compatibility
		{"STRING/varchar", "varchar", "STRING", true},
		{"STRING/text", "text", "STRING", true},
		{"STRING/mediumtext", "mediumtext", "STRING", true},
		{"STRING/longtext", "longtext", "STRING", true},
		{"STRING/other", "int", "STRING", false},

		// TIMESTAMP compatibility
		{"TIMESTAMP/timestamp", "timestamp", "TIMESTAMP", true},
		{"TIMESTAMP/datetime", "datetime", "TIMESTAMP", true},
		{"TIMESTAMP/other", "date", "TIMESTAMP", false},

		// Default case (unsupported SP datatype)
		{"Unsupported SP datatype", "varchar", "UNSUPPORTED", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcColumnDef := utils.SrcColumnDetails{Datatype: tt.srcDataType}
			spColumnDef := utils.SpColumnDetails{Datatype: tt.spDataType}
			got := ssa.IsDataTypeCodeCompatible(srcColumnDef, spColumnDef)
			if got != tt.want {
				t.Errorf("IsDataTypeCodeCompatible() got = %v, want %v for src='%s', sp='%s'", got, tt.want, tt.srcDataType, tt.spDataType)
			}
		})
	}
}
