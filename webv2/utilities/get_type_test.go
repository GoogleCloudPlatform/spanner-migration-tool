// Copyright 2024 Google LLC
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

package utilities

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestGetType(t *testing.T) {
	tableId := "t1"
	colId := "c1"

	testCases := []struct {
		name       string
		driver     string
		source     string
		dialect    string
		srcCol     schema.Column
		newType    string
		wantType   ddl.Type
		wantErr    bool
		wantIssues []internal.SchemaIssue
	}{
		{
			name:       "MySQL simple type",
			driver:     constants.MYSQL,
			source:     constants.MYSQL,
			dialect:    constants.DIALECT_GOOGLESQL,
			srcCol:     schema.Column{Name: "col1", Type: schema.Type{Name: "int"}},
			newType:    "",
			wantType:   ddl.Type{Name: ddl.Int64},
			wantErr:    false,
			wantIssues: []internal.SchemaIssue{internal.Widened},
		},
		{
			name:    "PostgreSQL array type with PG dialect",
			driver:  constants.POSTGRES,
			source:  constants.POSTGRES,
			dialect: constants.DIALECT_POSTGRESQL,
			srcCol:  schema.Column{Name: "col1", Type: schema.Type{Name: "text", ArrayBounds: []int64{-1}}},
			newType: "",
			wantType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true},
			wantErr:  false,
			wantIssues: []internal.SchemaIssue{internal.ArrayTypeNotSupported},
		},
		{
			name:    "PostgreSQL multi-dimensional array",
			driver:  constants.POSTGRES,
			source:  constants.POSTGRES,
			dialect: constants.DIALECT_GOOGLESQL,
			srcCol:  schema.Column{Name: "col1", Type: schema.Type{Name: "text", ArrayBounds: []int64{-1, -1}}},
			newType: "",
			wantType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: false},
			wantErr:  false,
			wantIssues: []internal.SchemaIssue{internal.MultiDimensionalArray, internal.MultiDimensionalArray},
		},
		{
			name:    "Cassandra array type",
			driver:  constants.CASSANDRA,
			source:  constants.CASSANDRA,
			dialect: constants.DIALECT_GOOGLESQL,
			srcCol:  schema.Column{Name: "col1", Type: schema.Type{Name: "list<text>"}},
			newType: "",
			wantType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true},
			wantErr:  false,
		},
		{
			name:       "SQL Server simple type",
			driver:     constants.SQLSERVER,
			source:     constants.SQLSERVER,
			dialect:    constants.DIALECT_GOOGLESQL,
			srcCol:     schema.Column{Name: "col1", Type: schema.Type{Name: "int"}},
			newType:    "",
			wantType:   ddl.Type{Name: ddl.Int64},
			wantErr:    false,
			wantIssues: []internal.SchemaIssue{internal.Widened},
		},
		{
			name:    "Oracle simple type",
			driver:  constants.ORACLE,
			source:  constants.ORACLE,
			dialect: constants.DIALECT_GOOGLESQL,
			srcCol:  schema.Column{Name: "col1", Type: schema.Type{Name: "NUMBER"}},
			newType: "",
			wantType: ddl.Type{Name: ddl.Numeric},
			wantErr: false,
		},
		{
			name:    "Unsupported driver",
			driver:  "unsupported_driver",
			source:  "unsupported_driver",
			dialect: constants.DIALECT_GOOGLESQL,
			srcCol:  schema.Column{Name: "col1", Type: schema.Type{Name: "int"}},
			newType: "",
			wantErr: true,
		},
		{
			name:       "MySQL with ignored flags",
			driver:     constants.MYSQL,
			source:     constants.MYSQL,
			dialect:    constants.DIALECT_GOOGLESQL,
			srcCol:     schema.Column{Name: "col1", Type: schema.Type{Name: "int"}, Ignored: schema.Ignored{Default: true, AutoIncrement: true}},
			newType:    "",
			wantType:   ddl.Type{Name: ddl.Int64},
			wantErr:    false,
			wantIssues: []internal.SchemaIssue{internal.Widened, internal.DefaultValue, internal.AutoIncrement},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sessionState := session.GetSessionState()
			sessionState.Driver = tc.driver

			conv := &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					tableId: {Id: tableId, Name: "t1"},
				},
				SrcSchema: map[string]schema.Table{
					tableId: {
						Id:   tableId,
						Name: "t1",
						ColDefs: map[string]schema.Column{
							colId: tc.srcCol,
						},
						ColIds: []string{colId},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					tableId: {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				SpDialect: tc.dialect,
				Source:    tc.source,
			}

			_, gotType, gotErr := GetType(conv, tc.newType, tableId, colId)

			if tc.wantErr {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
				assert.Equal(t, tc.wantType, gotType)
				if tc.wantIssues != nil {
					assert.ElementsMatch(t, tc.wantIssues, conv.SchemaIssues[tableId].ColumnLevelIssues[colId])
				} else {
					assert.Empty(t, conv.SchemaIssues[tableId].ColumnLevelIssues[colId])
				}
			}
		})
	}
}