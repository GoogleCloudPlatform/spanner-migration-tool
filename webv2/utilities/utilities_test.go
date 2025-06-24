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

func TestUpdateDataType(t *testing.T) {
	tableId := "t1"
	colId := "c1"

	testCases := []struct {
		name     string
		driver   string
		source   string
		dialect  string
		srcCol   schema.Column
		spColDef ddl.ColumnDef
		newType  string
		wantType ddl.Type
		wantOpts map[string]string
		wantErr  bool
		errText  string
	}{
		{
			name:     "MySQL type update",
			driver:   constants.MYSQL,
			source:   constants.MYSQL,
			dialect:  constants.DIALECT_GOOGLESQL,
			srcCol:   schema.Column{Name: "col1", Type: schema.Type{Name: "int"}},
			spColDef: ddl.ColumnDef{Name: "col1", T: ddl.Type{Name: ddl.Int64}},
			newType:  "STRING",
			wantType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			wantErr:  false,
		},
		{
			name:     "Cassandra type update with options",
			driver:   constants.CASSANDRA,
			source:   constants.CASSANDRA,
			dialect:  constants.DIALECT_GOOGLESQL,
			srcCol:   schema.Column{Name: "col1", Type: schema.Type{Name: "uuid"}},
			spColDef: ddl.ColumnDef{Name: "col1", T: ddl.Type{Name: ddl.String, Len: 36}},
			newType:  "TEXT",
			wantType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			wantOpts: map[string]string{"cassandra_type": "uuid"},
			wantErr:  false,
		},
		{
			name:     "Error from GetType",
			driver:   "unsupported",
			source:   "unsupported",
			dialect:  constants.DIALECT_GOOGLESQL,
			srcCol:   schema.Column{Name: "col1", Type: schema.Type{Name: "int"}},
			spColDef: ddl.ColumnDef{Name: "col1", T: ddl.Type{Name: ddl.Int64}},
			newType:  "STRING",
			wantErr:  true,
			errText:  "driver : 'unsupported' is not supported",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sessionState := session.GetSessionState()
			sessionState.Driver = tc.driver

			conv := &internal.Conv{
				SpSchema:  map[string]ddl.CreateTable{tableId: {Id: tableId, Name: "t1", ColDefs: map[string]ddl.ColumnDef{colId: tc.spColDef}}},
				SrcSchema: map[string]schema.Table{tableId: {Id: tableId, Name: "t1", ColDefs: map[string]schema.Column{colId: tc.srcCol}, ColIds: []string{colId}}},
				SchemaIssues: map[string]internal.TableIssues{
					tableId: {ColumnLevelIssues: make(map[string][]internal.SchemaIssue)},
				},
				SpDialect: tc.dialect,
				Source:    tc.source,
			}

			err := UpdateDataType(conv, tc.newType, tableId, colId)

			if tc.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errText)
			} else {
				assert.NoError(t, err)
				updatedColDef := conv.SpSchema[tableId].ColDefs[colId]
				assert.Equal(t, tc.wantType, updatedColDef.T)
				if tc.wantOpts != nil {
					assert.Equal(t, tc.wantOpts, updatedColDef.Opts)
				}
			}
		})
	}
}