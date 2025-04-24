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
	// Define common conv structures or generate them within test cases for more complex scenarios.
	sampleConv := &internal.Conv{
		SrcSchema: map[string]schema.Table{
			"t1_id": {
				Name: "table1",
				Id:   "t1_id",
				ColDefs: map[string]schema.Column{
					"c1_id": {Name: "col1", Id: "c1_id", Type: schema.Type{Name: "INT"}},
					"c2_id": {Name: "col2", Id: "c2_id", Type: schema.Type{Name: "VARCHAR", Mods: []int64{255}}},
					"c3_id": {Name: "col3", Id: "c3_id", Type: schema.Type{Name: "TIMESTAMP"}},
					"c4_id": {Name: "col4", Id: "c4_id", Type: schema.Type{Name: "TEXT"}},
					"c5_id": {Name: "col5", Id: "c5_id", Type: schema.Type{Name: "INT"}}, // Will be overridden with "unsigned" in mock
				},
			},
		},
	}

	type testCase struct {
		name                 string
		conv                 *internal.Conv
		dbName               string
		setupMock            func(mock sqlmock.Sqlmock, currentConv *internal.Conv, currentDbName string)
		expectedTableInfoMap map[string]utils.TableAssessmentInfo                            // For brevity, might only check key fields or length
		checkSpecifics       func(t *testing.T, result map[string]utils.TableAssessmentInfo) // For detailed checks
		wantErrMsgContains   string
	}

	testCases := []testCase{
		{
			name:   "Success - single table with various column types",
			conv:   sampleConv,
			dbName: "test_db",
			setupMock: func(mock sqlmock.Sqlmock, currentConv *internal.Conv, currentDbName string) {
				table := currentConv.SrcSchema["t1_id"]
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \? AND TABLE_NAME = \?`).
					WithArgs(currentDbName, table.Name).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression FROM information_schema.COLUMNS c where table_schema = \? and table_name = \? and column_name = \?`).
					WithArgs(currentDbName, table.Name, "col1").
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("int(11)", sql.NullString{}, sql.NullString{}))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression FROM information_schema.COLUMNS c where table_schema = \? and table_name = \? and column_name = \?`).
					WithArgs(currentDbName, table.Name, "col2").
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("varchar(255)", sql.NullString{String: "VIRTUAL GENERATED", Valid: true}, sql.NullString{String: "CONCAT(col1, 'test')", Valid: true}))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression FROM information_schema.COLUMNS c where table_schema = \? and table_name = \? and column_name = \?`).
					WithArgs(currentDbName, table.Name, "col3").
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("timestamp", sql.NullString{String: "on update CURRENT_TIMESTAMP", Valid: true}, sql.NullString{}))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression FROM information_schema.COLUMNS c where table_schema = \? and table_name = \? and column_name = \?`).
					WithArgs(currentDbName, table.Name, "col4").
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("text", sql.NullString{String: "STORED GENERATED", Valid: true}, sql.NullString{String: "UPPER(col2)", Valid: true}))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression FROM information_schema.COLUMNS c where table_schema = \? and table_name = \? and column_name = \?`).
					WithArgs(currentDbName, table.Name, "col5").
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("int(10) unsigned", sql.NullString{}, sql.NullString{}))
			},
			checkSpecifics: func(t *testing.T, result map[string]utils.TableAssessmentInfo) {
				assert.Len(t, result, 1)
				tableInfo, ok := result["t1_id"]
				assert.True(t, ok)
				assert.Equal(t, "table1", tableInfo.Name)
				assert.Equal(t, "utf8mb4", tableInfo.Charset)
				assert.Equal(t, "utf8mb4_general_ci", tableInfo.Collation)
				assert.Len(t, tableInfo.ColumnAssessmentInfos, 5)

				col1Info := tableInfo.ColumnAssessmentInfos["c1_id"]
				assert.Equal(t, "col1", col1Info.Name)
				assert.False(t, col1Info.IsUnsigned)
				assert.False(t, col1Info.IsOnUpdateTimestampSet)
				assert.False(t, col1Info.GeneratedColumn.IsPresent)
				assert.Equal(t, getColumnMaxSize("INT", nil, "utf8mb4"), col1Info.MaxColumnSize)

				col2Info := tableInfo.ColumnAssessmentInfos["c2_id"]
				assert.True(t, col2Info.GeneratedColumn.IsPresent)
				assert.True(t, col2Info.GeneratedColumn.IsVirtual)
				assert.Equal(t, "CONCAT(col1, 'test')", col2Info.GeneratedColumn.Statement)
				assert.Equal(t, getColumnMaxSize("VARCHAR", []int64{255}, "utf8mb4"), col2Info.MaxColumnSize)

				col3Info := tableInfo.ColumnAssessmentInfos["c3_id"]
				assert.True(t, col3Info.IsOnUpdateTimestampSet)

				col4Info := tableInfo.ColumnAssessmentInfos["c4_id"]
				assert.True(t, col4Info.GeneratedColumn.IsPresent)
				assert.False(t, col4Info.GeneratedColumn.IsVirtual)
				assert.Equal(t, "UPPER(col2)", col4Info.GeneratedColumn.Statement)
				assert.Equal(t, getColumnMaxSize("TEXT", nil, "utf8mb4"), col4Info.MaxColumnSize)

				col5Info := tableInfo.ColumnAssessmentInfos["c5_id"]
				assert.True(t, col5Info.IsUnsigned)
			},
		},
		{
			name:   "Error querying table info",
			conv:   sampleConv,
			dbName: "test_db",
			setupMock: func(mock sqlmock.Sqlmock, currentConv *internal.Conv, currentDbName string) {
				table := currentConv.SrcSchema["t1_id"]
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(currentDbName, table.Name).
					WillReturnError(errors.New("db error for table"))
			},
			wantErrMsgContains: "couldn't get schema for table table1: db error for table",
		},
		{
			name:   "Error querying column info",
			conv:   sampleConv,
			dbName: "test_db",
			setupMock: func(mock sqlmock.Sqlmock, currentConv *internal.Conv, currentDbName string) {
				table := currentConv.SrcSchema["t1_id"]
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(currentDbName, table.Name).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))

				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression`).
					WithArgs(currentDbName, table.Name, "col1"). // First column
					WillReturnError(errors.New("db error for column"))
			},
			wantErrMsgContains: "couldn't get schema for column table1.col1: db error for column",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isi, mock := newTestInfoSchemaImpl(t)
			isi.DbName = tc.dbName
			defer isi.Db.Close()

			if tc.setupMock != nil {
				tc.setupMock(mock, tc.conv, tc.dbName)
			}

			result, err := isi.GetTableInfo(tc.conv)

			if tc.wantErrMsgContains != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsgContains)
			} else {
				assert.NoError(t, err)
				if tc.checkSpecifics != nil {
					tc.checkSpecifics(t, result)
				} else if tc.expectedTableInfoMap != nil {
					assert.Equal(t, tc.expectedTableInfoMap, result)
				}
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
