// Copyright 2022 Google LLC
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
package primarykey

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func TestDetectHotspot(t *testing.T) {
	tc := []struct {
		name          string
		tableId       string
		columnId      string
		conv          internal.Conv
		statusCode    int
		expectedIssue []internal.SchemaIssue
	}{
		{
			name:     "timeStamp_t1",
			tableId:  "t1",
			columnId: "c3",
			conv: internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Id:     "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c1"},
							"c2": {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c2"},
							"c3": {Name: "last_update", T: ddl.Type{Name: ddl.Timestamp, Len: ddl.MaxLength}, Id: "c3"},
						},
						PrimaryKeys: []ddl.IndexKey{
							{ColId: "c3", Order: 1, Desc: false},
							{ColId: "c2", Order: 2, Desc: false},
						},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
			},
			statusCode:    http.StatusOK,
			expectedIssue: []internal.SchemaIssue{internal.HotspotTimestamp},
		},
	}

	for _, tt := range tc {
		sessionState := session.GetSessionState()
		sessionState.Conv = &tt.conv
		DetectHotspot()
		actual := sessionState.Conv.SchemaIssues[tt.tableId].ColumnLevelIssues[tt.columnId]
		if !reflect.DeepEqual(actual, tt.expectedIssue) {
			t.Errorf("%s failed, expected: %v, got: %v", tt.name, tt.expectedIssue, actual)
		}
	}
}
