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

package table

import (
	"fmt"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestIsInterleavingImpacted(t *testing.T) {
	t1 := ddl.CreateTable{
		Name:   "t1",
		Id:     "t1",
		ColIds: []string{"c1", "c2"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
			"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 10}, NotNull: false},
		},
		PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1}},
	}
	t2 := ddl.CreateTable{
		Name:   "t2",
		Id:     "t2",
		ColIds: []string{"c3", "c4"},
		ColDefs: map[string]ddl.ColumnDef{
			"c3": {Name: "a", Id: "c3", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
			"c4": {Name: "c", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
		},
		PrimaryKeys: []ddl.IndexKey{{ColId: "c3", Order: 1}, {ColId: "c4", Order: 2}},
		ParentTable: ddl.InterleavedParent{Id: "t1"},
	}

	conv := &internal.Conv{
		SpSchema: map[string]ddl.CreateTable{
			"t1": t1,
			"t2": t2,
		},
	}

	testCases := []struct {
		name          string
		tableId       string
		colId         string
		update        updateCol
		expectImpact  bool
		expectedError string
		customConv    *internal.Conv
	}{
		{
			name:         "Modify non-PK column in parent table",
			tableId:      "t1",
			colId:        "c2",
			update:       updateCol{Rename: "new_b"},
			expectImpact: false,
		},
		{
			name:         "Modify non-PK column in child table",
			tableId:      "t2",
			colId:        "c4",
			update:       updateCol{ToType: ddl.String},
			expectImpact: false,
		},
		{
			name:         "Modify PK of parent table (rename)",
			tableId:      "t1",
			colId:        "c1",
			update:       updateCol{Rename: "new_a"},
			expectImpact: true,
			expectedError: fmt.Sprintf("Modifying primary key column '%s' is not allowed because table '%s' is a parent in an interleave relationship. Please remove the interleave relationship first.",
				conv.SpSchema["t1"].ColDefs["c1"].Name, conv.SpSchema["t1"].Name),
		},
		{
			name:         "Modify PK of parent table (type change)",
			tableId:      "t1",
			colId:        "c1",
			update:       updateCol{ToType: ddl.String},
			expectImpact: true,
			customConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": t1, "t2": t2,
				},
			},
			expectedError: fmt.Sprintf("Modifying primary key column '%s' is not allowed because table '%s' is a parent in an interleave relationship. Please remove the interleave relationship first.",
				conv.SpSchema["t1"].ColDefs["c1"].Name, conv.SpSchema["t1"].Name),
		},
		{
			name:         "Modify PK of parent table (size change)",
			tableId:      "t1",
			colId:        "c1",
			update:       updateCol{MaxColLength: "20"},
			expectImpact: true,
			expectedError: fmt.Sprintf("Modifying primary key column '%s' is not allowed because table '%s' is a parent in an interleave relationship. Please remove the interleave relationship first.",
				conv.SpSchema["t1"].ColDefs["c1"].Name, conv.SpSchema["t1"].Name),
		},
		{
			name:         "Modify PK of parent table (not null change)",
			tableId:      "t1",
			colId:        "c1",
			update:       updateCol{NotNull: "REMOVED"},
			expectImpact: true,
			expectedError: fmt.Sprintf("Modifying primary key column '%s' is not allowed because table '%s' is a parent in an interleave relationship. Please remove the interleave relationship first.",
				conv.SpSchema["t1"].ColDefs["c1"].Name, conv.SpSchema["t1"].Name),
		},
		{
			name:         "Remove PK of parent table",
			tableId:      "t1",
			colId:        "c1",
			update:       updateCol{Removed: true},
			expectImpact: true,
			expectedError: fmt.Sprintf("Modifying primary key column '%s' is not allowed because table '%s' is a parent in an interleave relationship. Please remove the interleave relationship first.",
				conv.SpSchema["t1"].ColDefs["c1"].Name, conv.SpSchema["t1"].Name),
		},
		{
			name:         "Modify inherited PK of child table (rename)",
			tableId:      "t2",
			colId:        "c3",
			update:       updateCol{Rename: "new_a"},
			expectImpact: true,
			expectedError: fmt.Sprintf("Modifying column '%s' is not allowed because it is part of the interleaved primary key from parent table '%s'. Please remove the interleave relationship first.",
				conv.SpSchema["t2"].ColDefs["c3"].Name, conv.SpSchema["t1"].Name),
		},
		{
			name:         "Modify inherited PK of child table (type change)",
			tableId:      "t2",
			colId:        "c3",
			update:       updateCol{ToType: ddl.String},
			expectImpact: true,
			customConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": t1, "t2": t2,
				},
			},
			expectedError: fmt.Sprintf("Modifying column '%s' is not allowed because it is part of the interleaved primary key from parent table '%s'. Please remove the interleave relationship first.",
				conv.SpSchema["t2"].ColDefs["c3"].Name, conv.SpSchema["t1"].Name),
		},
		{
			name:         "Modify child-specific PK of child table (rename)",
			tableId:      "t2",
			colId:        "c4",
			update:       updateCol{Rename: "new_c"},
			expectImpact: false,
		},
		{
			name:         "Modify child-specific PK of child table (type change)",
			tableId:      "t2",
			colId:        "c4",
			update:       updateCol{ToType: ddl.String},
			expectImpact: false,
		},
		{
			name:         "Table is not interleaved, PK modification is allowed",
			tableId:      "t1",
			colId:        "c1",
			update:       updateCol{Rename: "new_a"},
			expectImpact: false,
			// Custom conv where t2 is not a child of t1
			customConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 10}, NotNull: false},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1}},
					},
					"t2": {
						Name:   "t2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c3", Order: 1}},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentConv := conv
			if tc.customConv != nil {
				currentConv = tc.customConv
			}


			sessionState := session.GetSessionState()
			sessionState.Conv = currentConv
			sessionState.Driver = constants.MYSQL
			errStr := IsInterleavingImpacted(tc.update, tc.tableId, tc.colId, currentConv)
			if tc.expectImpact {
				assert.Equal(t, tc.expectedError, errStr)
			} else {
				assert.Empty(t, errStr)
			}
		})
	}
}
