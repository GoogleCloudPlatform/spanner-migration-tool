// Copyright 2025 Google LLC
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

package table

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestAddNewColumn(t *testing.T) {
	tableId := "t1"

	testCases := []struct {
		name                 string
		payload              string
		initialConv          *internal.Conv
		initialCounterState  string
		expectedNewColId     string
		expectedStatusCode   int
		expectedBodyContains string
		checkConv            func(t *testing.T, conv *internal.Conv, newColId string)
	}{
		{
			name:    "Add new column to Cassandra table",
			payload: `{"Name": "new_col", "Datatype": "STRING", "Length": 50, "IsNullable": true}`,
			initialConv: &internal.Conv{
				Source: constants.CASSANDRA,
				SpSchema: map[string]ddl.CreateTable{
					tableId: {
						Id: tableId, 
						Name: "my_table", 
						ColIds: []string{"c1"}, 
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {
								Id: "c1", 
								Name: "existing_col", 
								T: ddl.Type{
									Name: ddl.Int64,
								},
							},
						},
					},
				},
			},
			initialCounterState: "1",
			expectedNewColId:    "c2",
			expectedStatusCode: http.StatusOK,
			checkConv: func(t *testing.T, conv *internal.Conv, newColId string) {
				newCol, ok := conv.SpSchema[tableId].ColDefs[newColId]
				assert.True(t, ok, "new column with predicted ID should exist")
				assert.Equal(t, "STRING", newCol.T.Name)
				assert.Equal(t, int64(50), newCol.T.Len)
				assert.False(t, newCol.NotNull)
				assert.NotNil(t, newCol.Opts)
				assert.Equal(t, "text", newCol.Opts["cassandra_type"])
			},
		},
		{
			name:    "Add new column with unsupported datatype to Cassandra table",
			payload: `{"Name": "new_col", "Datatype": "JSON", "Length": 50, "IsNullable": true}`,
			initialConv: &internal.Conv{
				Source: constants.CASSANDRA,
				SpSchema: map[string]ddl.CreateTable{
					tableId: {
						Id: tableId, 
						Name: "my_table", 
						ColIds: []string{"c1"}, 
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {
								Id: "c1", 
								Name: "existing_col", 
								T: ddl.Type{
									Name: ddl.Int64,
								},
							},
						},
					},
				},
			},
			initialCounterState: "1",
			expectedNewColId:    "c2",
			expectedStatusCode: http.StatusOK,
			checkConv: func(t *testing.T, conv *internal.Conv, newColId string) {
				newCol, ok := conv.SpSchema[tableId].ColDefs[newColId]
				assert.True(t, ok, "new column with predicted ID should exist")
				assert.Equal(t, "JSON", newCol.T.Name)
				assert.False(t, newCol.NotNull)
				assert.NotNil(t, newCol.Opts)
				assert.Equal(t, "", newCol.Opts["cassandra_type"])
			},
		},
		{
			name:                 "Error on duplicate column name",
			payload:              `{"Name": "existing_col"}`,
			initialConv:          &internal.Conv{SpSchema: map[string]ddl.CreateTable{tableId: {Id: tableId, Name: "my_table", ColIds: []string{"c1"}, ColDefs: map[string]ddl.ColumnDef{"c1": {Id: "c1", Name: "existing_col"}}}}},
			initialCounterState:  "1",
			expectedStatusCode:   http.StatusBadRequest,
			expectedBodyContains: "Multiple columns with similar name cannot exist",
		},
		{
			name:                 "Error on used identifier",
			payload:              `{"Name": "another_table"}`,
			initialConv:          &internal.Conv{SpSchema: map[string]ddl.CreateTable{tableId: {Id: tableId, Name: "my_table"}, "t2": {Id: "t2", Name: "another_table"}}},
			initialCounterState:  "2",
			expectedStatusCode:   http.StatusBadRequest,
			expectedBodyContains: "is an existing identifier",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sessionState := session.GetSessionState()
			sessionState.Conv = tc.initialConv
			internal.Cntr.ObjectId = tc.initialCounterState

			req, err := http.NewRequest("POST", "/add-column?table="+tableId, strings.NewReader(tc.payload))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(AddNewColumn)
			handler.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBodyContains != "" {
				assert.Contains(t, rr.Body.String(), tc.expectedBodyContains)
			}

			if tc.checkConv != nil {
				var res session.ConvWithMetadata
				err := json.Unmarshal(rr.Body.Bytes(), &res)
				if err != nil {
					t.Fatalf("Failed to unmarshal response body: %v", err)
				}
				tc.checkConv(t, res.Conv, tc.expectedNewColId)
			}
		})
	}
}