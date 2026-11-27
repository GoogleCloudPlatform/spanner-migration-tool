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
	"reflect"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
)

// hotspotConv builds a one-table conv: id INT64, ts TIMESTAMP, ai INT64 whose
// source auto-increment was not migrated (Ignored.AutoIncrement), and name
// STRING, with the given primary key.
func hotspotConv(pks []ddl.IndexKey) *internal.Conv {
	return &internal.Conv{
		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "id", T: ddl.Type{Name: ddl.Int64}, Id: "c1"},
					"c2": {Name: "ts", T: ddl.Type{Name: ddl.Timestamp}, Id: "c2"},
					"c3": {Name: "ai", T: ddl.Type{Name: ddl.Int64}, Id: "c3"},
					"c4": {Name: "name", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c4"},
				},
				PrimaryKeys: pks,
			}},
		SrcSchema: map[string]schema.Table{
			"t1": {
				Id: "t1",
				ColDefs: map[string]schema.Column{
					"c1": {Name: "id", Id: "c1"},
					"c2": {Name: "ts", Id: "c2"},
					"c3": {Name: "ai", Id: "c3", Ignored: schema.Ignored{AutoIncrement: true}},
					"c4": {Name: "name", Id: "c4"},
				},
			}},
		SchemaIssues: map[string]internal.TableIssues{
			"t1": {ColumnLevelIssues: make(map[string][]internal.SchemaIssue)},
		},
	}
}

// hotspotIssues returns the hotspot issues per column, leaving out columns without any.
func hotspotIssues(conv *internal.Conv) map[string][]internal.SchemaIssue {
	got := map[string][]internal.SchemaIssue{}
	for colId, issues := range conv.SchemaIssues["t1"].ColumnLevelIssues {
		for _, issue := range issues {
			if issue == internal.HotspotTimestamp || issue == internal.HotspotAutoIncrement {
				got[colId] = append(got[colId], issue)
			}
		}
	}
	return got
}

func TestDetectHotspot(t *testing.T) {
	ts := []internal.SchemaIssue{internal.HotspotTimestamp}
	ai := []internal.SchemaIssue{internal.HotspotAutoIncrement}
	tc := []struct {
		name     string
		pks      []ddl.IndexKey
		expected map[string][]internal.SchemaIssue
	}{
		{
			name:     "single TIMESTAMP key is flagged",
			pks:      []ddl.IndexKey{{ColId: "c2", Order: 1}},
			expected: map[string][]internal.SchemaIssue{"c2": ts},
		},
		{
			name:     "composite key, TIMESTAMP first is flagged",
			pks:      []ddl.IndexKey{{ColId: "c2", Order: 1}, {ColId: "c1", Order: 2}},
			expected: map[string][]internal.SchemaIssue{"c2": ts},
		},
		{
			name:     "composite key, TIMESTAMP not first is not flagged",
			pks:      []ddl.IndexKey{{ColId: "c1", Order: 1}, {ColId: "c2", Order: 2}},
			expected: map[string][]internal.SchemaIssue{},
		},
		{
			name:     "keys stored out of Order: the Order=1 column decides (TIMESTAMP is second)",
			pks:      []ddl.IndexKey{{ColId: "c2", Order: 2}, {ColId: "c1", Order: 1}},
			expected: map[string][]internal.SchemaIssue{},
		},
		{
			name:     "keys stored out of Order: the Order=1 column decides (TIMESTAMP is first)",
			pks:      []ddl.IndexKey{{ColId: "c1", Order: 2}, {ColId: "c2", Order: 1}},
			expected: map[string][]internal.SchemaIssue{"c2": ts},
		},
		{
			name:     "Order not set (0 for all keys): the first key in the slice decides",
			pks:      []ddl.IndexKey{{ColId: "c1"}, {ColId: "c2"}},
			expected: map[string][]internal.SchemaIssue{},
		},
		{
			name:     "auto-increment column as the only key is flagged",
			pks:      []ddl.IndexKey{{ColId: "c3", Order: 1}},
			expected: map[string][]internal.SchemaIssue{"c3": ai},
		},
		{
			name:     "composite key, auto-increment column first is flagged",
			pks:      []ddl.IndexKey{{ColId: "c3", Order: 1}, {ColId: "c4", Order: 2}},
			expected: map[string][]internal.SchemaIssue{"c3": ai},
		},
		{
			name:     "composite key, auto-increment column not first is not flagged",
			pks:      []ddl.IndexKey{{ColId: "c4", Order: 1}, {ColId: "c3", Order: 2}},
			expected: map[string][]internal.SchemaIssue{},
		},
		{
			name:     "no primary key",
			pks:      nil,
			expected: map[string][]internal.SchemaIssue{},
		},
	}

	for _, tt := range tc {
		sessionState := session.GetSessionState()
		sessionState.Conv = hotspotConv(tt.pks)
		DetectHotspot()
		if got := hotspotIssues(sessionState.Conv); !reflect.DeepEqual(got, tt.expected) {
			t.Errorf("%s: expected %v, got %v", tt.name, tt.expected, got)
		}
		// Running the detection again must not add duplicates.
		DetectHotspot()
		if got := hotspotIssues(sessionState.Conv); !reflect.DeepEqual(got, tt.expected) {
			t.Errorf("%s (second run): expected %v, got %v", tt.name, tt.expected, got)
		}
	}
}

// A primary key edit (reorder, add or remove a column) recomputes the warnings
// on the final key, keeping the table's other issues.
func TestIsHotSpot_PrimaryKeyEdits(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Conv = hotspotConv([]ddl.IndexKey{{ColId: "c2", Order: 1}, {ColId: "c1", Order: 2}})
	sessionState.Conv.SchemaIssues["t1"].ColumnLevelIssues["c2"] = []internal.SchemaIssue{internal.Datetime}
	table := sessionState.Conv.SpSchema["t1"]

	isHotSpot(table.PrimaryKeys, table)
	if got := sessionState.Conv.SchemaIssues["t1"].ColumnLevelIssues["c2"]; !reflect.DeepEqual(got, []internal.SchemaIssue{internal.Datetime, internal.HotspotTimestamp}) {
		t.Errorf("initial: got %v", got)
	}

	// Reorder: id first. The timestamp warning goes, the other issue stays.
	isHotSpot([]ddl.IndexKey{{ColId: "c2", Order: 2}, {ColId: "c1", Order: 1}}, table)
	if got := sessionState.Conv.SchemaIssues["t1"].ColumnLevelIssues["c2"]; !reflect.DeepEqual(got, []internal.SchemaIssue{internal.Datetime}) {
		t.Errorf("after reorder: got %v", got)
	}

	// Add the auto-increment column as the new first key.
	isHotSpot([]ddl.IndexKey{{ColId: "c2", Order: 3}, {ColId: "c1", Order: 2}, {ColId: "c3", Order: 1}}, table)
	if got := hotspotIssues(sessionState.Conv); !reflect.DeepEqual(got, map[string][]internal.SchemaIssue{"c3": {internal.HotspotAutoIncrement}}) {
		t.Errorf("after add: got %v", got)
	}

	// Remove it again: the timestamp is first once more.
	isHotSpot([]ddl.IndexKey{{ColId: "c2", Order: 1}, {ColId: "c1", Order: 2}}, table)
	if got := hotspotIssues(sessionState.Conv); !reflect.DeepEqual(got, map[string][]internal.SchemaIssue{"c2": {internal.HotspotTimestamp}}) {
		t.Errorf("after remove: got %v", got)
	}
}
