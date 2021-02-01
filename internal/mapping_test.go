// Copyright 2020 Google LLC
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

package internal

import (
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestGetSpannerTable(t *testing.T) {
	conv := MakeConv()
	basicTests := []struct {
		name     string // Name of test.
		srcTable string // Source DB table name to test.
		error    bool   // Whether an error is expected.
		spTable  string // Expected Spanner table name.
	}{
		{"Empty", "", true, ""},
		{"Good name", "table", false, "table"},
		{"Good name: setup collision", "tab_le_5", false, "tab_le_5"},
		{"Good name: setup collision (2)", "tab_le_6", false, "tab_le_6"},
		{"Illegal character", "tab\nle", false, "tab_le"},
		{"Illegal character with collision (1)", "tab\tle", false, "tab_le_4"},
		{"Illegal character with collision (2)", "tab?le", false, "tab_le_7"}, // Must skip tab_le_5 and tab_le_6.
		{"Collision with previous remapping", "tab_le_4", false, "tab_le_4_6"},
		{"Illegal start character", "2table", false, "Atable"},
		{"Illegal start character with collision (1)", "_table", false, "Atable_8"},
		{"Illegal start character with collision (2)", "\ntable", false, "Atable_9"},
	}
	for _, tc := range basicTests {
		spTable, err := GetSpannerTable(conv, tc.srcTable)
		if tc.error {
			assert.NotNil(t, err, tc.name)
			continue
		}
		assert.Equal(t, tc.spTable, spTable, tc.name)
		// Run again to check we get same result.
		s2, err := GetSpannerTable(conv, tc.srcTable)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, spTable, s2, tc.name)
	}
}

func TestGetSpannerCol(t *testing.T) {
	conv := MakeConv()
	basicTests := []struct {
		name     string // Name of test.
		srcTable string // Source DB table name to test.
		srcCol   string // Source DB col name to test.
		error    bool   // Whether an error is expected.
		spCol    string // Expected Spanner column name.
	}{
		{"Empty table", "", "col", true, ""},
		{"Empty col", "table", "", true, ""},
		{"Good name", "table", "col", false, "col"},
		{"Bad table", "ta.b\nle", "col", false, "col"},
		{"Bad col", "table", "c\nol", false, "c_ol"},
		{"Bad table and col", "t.able", "c\no\nl", false, "c_o_l"},
		{"table1 good name 1", "table1", "col", false, "col"},
		{"table1 good name 2", "table1", "c_ol", false, "c_ol"},
		{"table1 good name 3", "table1", "c_ol_5", false, "c_ol_5"},
		{"table1 good name 4", "table1", "c_ol_6", false, "c_ol_6"},
		{"table1 collision 1", "table1", "c\tol", false, "c_ol_4"},
		{"table1 collision 2", "table1", "c\nol", false, "c_ol_7"}, // Skip c_ol_5 and c_ol_6.
		{"table1 collision 3", "table1", "c?ol", false, "c_ol_8"},
	}
	for _, tc := range basicTests {
		_, err1 := GetSpannerTable(conv, tc.srcTable) // Ensure table is known.
		spCol, err2 := GetSpannerCol(conv, tc.srcTable, tc.srcCol, false)
		if tc.error {
			assert.True(t, err1 != nil || err2 != nil, tc.name)
			continue
		}
		assert.Equal(t, tc.spCol, spCol, tc.name)
		// Run again to check we get same result.
		spCol2, err := GetSpannerCol(conv, tc.srcTable, tc.srcCol, false)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, spCol, spCol2, tc.name)
	}
}

func TestToSpannerForeignKey(t *testing.T) {
	schemaForeignKeys := make(map[string]bool)

	basicTests := []struct {
		name       string // Name of test.
		srcKeyName string // Source foreign key name.
		spKeyName  string // Expected Spanner foreign key name.
	}{
		{"Good name", "fktest", "fktest"},
		{"Empty name", "", ""},
	}
	for _, tc := range basicTests {
		spKeyName := ToSpannerForeignKey(tc.srcKeyName, schemaForeignKeys)
		assert.Equal(t, tc.spKeyName, spKeyName, tc.name)
	}
}

func TestGetSpannerId(t *testing.T) {
	schemaIndexKeys := make(map[string]bool)

	basicTests := []struct {
		name       string // Name of test.
		srcKeyName string // Source key name.
		spKeyName  string // Expected Spanner key name.
	}{
		{"Good name", "index1", "index1"},
		{"Collision", "index1", "index1_1"},
		{"Collision 2", "index1", "index1_2"},
		{"Bad name", "in\ndex", "in_dex"},
		{"Bad name 2", "i\nn\ndex", "i_n_dex"},
		{"Collision 3", "index", "index"},
		{"Collision 4", "index", "index_6"},
		{"Good name", "in_dex", "in_dex_7"},
		{"Collision 5", "index_6", "index_6_8"},
		{"Bad name with collision", "in\tdex", "in_dex_9"},
		{"Bad name with collision 2", "in\ndex", "in_dex_10"},
		{"Bad name with collision 3", "in?dex", "in_dex_11"},
	}
	for _, tc := range basicTests {
		spKeyName := getSpannerId(tc.srcKeyName, schemaIndexKeys)
		assert.Equal(t, tc.spKeyName, spKeyName, tc.name)
	}
}

func TestResolveRefs(t *testing.T) {
	basicTests := []struct {
		name             string                     // Name of test.
		spSchema         map[string]ddl.CreateTable // Spanner schema.
		expectedSpSchema map[string]ddl.CreateTable // Expected Spanner schema.
		unexpecteds      int64                      // Expected unexpected conditions
	}{
		{
			name: "Table name case mismatch",
			spSchema: map[string]ddl.CreateTable{
				"a": ddl.CreateTable{
					Name:     "a",
					ColNames: []string{"acol1", "acol2"},
					ColDefs: map[string]ddl.ColumnDef{
						"acol1": ddl.ColumnDef{Name: "acol1", T: ddl.Type{Name: ddl.Int64}},
						"acol2": ddl.ColumnDef{Name: "acol2", T: ddl.Type{Name: ddl.Int64}},
					},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"acol1"}, ReferTable: "bB", ReferColumns: []string{"bcol1"}}},
				},
				"bb": ddl.CreateTable{
					Name:     "bb",
					ColNames: []string{"bcol1", "bcol2", "bcol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"bcol1": ddl.ColumnDef{Name: "bcol1", T: ddl.Type{Name: ddl.Int64}},
						"bcol2": ddl.ColumnDef{Name: "bcol2", T: ddl.Type{Name: ddl.Int64}},
						"bcol3": ddl.ColumnDef{Name: "bcol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			expectedSpSchema: map[string]ddl.CreateTable{
				"a": ddl.CreateTable{
					Name:     "a",
					ColNames: []string{"acol1", "acol2"},
					ColDefs: map[string]ddl.ColumnDef{
						"acol1": ddl.ColumnDef{Name: "acol1", T: ddl.Type{Name: ddl.Int64}},
						"acol2": ddl.ColumnDef{Name: "acol2", T: ddl.Type{Name: ddl.Int64}},
					},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"acol1"}, ReferTable: "bb", ReferColumns: []string{"bcol1"}}},
				},
				"bb": ddl.CreateTable{
					Name:     "bb",
					ColNames: []string{"bcol1", "bcol2", "bcol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"bcol1": ddl.ColumnDef{Name: "bcol1", T: ddl.Type{Name: ddl.Int64}},
						"bcol2": ddl.ColumnDef{Name: "bcol2", T: ddl.Type{Name: ddl.Int64}},
						"bcol3": ddl.ColumnDef{Name: "bcol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			unexpecteds: 0,
		},
		{
			name: "Column name case mismatch",
			spSchema: map[string]ddl.CreateTable{
				"bb": ddl.CreateTable{
					Name:     "bb",
					ColNames: []string{"bcol1", "bcol2", "bcol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"bcol1": ddl.ColumnDef{Name: "bcol1", T: ddl.Type{Name: ddl.Int64}},
						"bcol2": ddl.ColumnDef{Name: "bcol2", T: ddl.Type{Name: ddl.Int64}},
						"bcol3": ddl.ColumnDef{Name: "bcol3", T: ddl.Type{Name: ddl.Int64}},
					},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test2", Columns: []string{"bcol2", "bcol3"}, ReferTable: "cc", ReferColumns: []string{"cCol1", "ccol2"}}},
				},
				"cc": ddl.CreateTable{
					Name:     "cc",
					ColNames: []string{"ccol1", "ccol2", "ccol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"ccol1": ddl.ColumnDef{Name: "ccol1", T: ddl.Type{Name: ddl.Int64}},
						"ccol2": ddl.ColumnDef{Name: "ccol2", T: ddl.Type{Name: ddl.Int64}},
						"ccol3": ddl.ColumnDef{Name: "ccol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			expectedSpSchema: map[string]ddl.CreateTable{
				"bb": ddl.CreateTable{
					Name:     "bb",
					ColNames: []string{"bcol1", "bcol2", "bcol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"bcol1": ddl.ColumnDef{Name: "bcol1", T: ddl.Type{Name: ddl.Int64}},
						"bcol2": ddl.ColumnDef{Name: "bcol2", T: ddl.Type{Name: ddl.Int64}},
						"bcol3": ddl.ColumnDef{Name: "bcol3", T: ddl.Type{Name: ddl.Int64}},
					},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test2", Columns: []string{"bcol2", "bcol3"}, ReferTable: "cc", ReferColumns: []string{"ccol1", "ccol2"}}},
				},
				"cc": ddl.CreateTable{
					Name:     "cc",
					ColNames: []string{"ccol1", "ccol2", "ccol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"ccol1": ddl.ColumnDef{Name: "ccol1", T: ddl.Type{Name: ddl.Int64}},
						"ccol2": ddl.ColumnDef{Name: "ccol2", T: ddl.Type{Name: ddl.Int64}},
						"ccol3": ddl.ColumnDef{Name: "ccol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			unexpecteds: 0,
		},
		{
			name: "Column name not found after lower case check",
			spSchema: map[string]ddl.CreateTable{
				"cc": ddl.CreateTable{
					Name:     "cc",
					ColNames: []string{"ccol1", "ccol2", "ccol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"ccol1": ddl.ColumnDef{Name: "ccol1", T: ddl.Type{Name: ddl.Int64}},
						"ccol2": ddl.ColumnDef{Name: "ccol2", T: ddl.Type{Name: ddl.Int64}},
						"ccol3": ddl.ColumnDef{Name: "ccol3", T: ddl.Type{Name: ddl.Int64}},
					},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test3", Columns: []string{"ccol2", "ccol3"}, ReferTable: "dd", ReferColumns: []string{"dcol1", "dcol2"}}},
				},
				"dd": ddl.CreateTable{
					Name:     "dd",
					ColNames: []string{"dcol1", "ddcol2", "dcol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"dcol1":  ddl.ColumnDef{Name: "dcol1", T: ddl.Type{Name: ddl.Int64}},
						"ddcol2": ddl.ColumnDef{Name: "ddcol2", T: ddl.Type{Name: ddl.Int64}},
						"dcol3":  ddl.ColumnDef{Name: "dcol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			expectedSpSchema: map[string]ddl.CreateTable{
				"cc": ddl.CreateTable{
					Name:     "cc",
					ColNames: []string{"ccol1", "ccol2", "ccol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"ccol1": ddl.ColumnDef{Name: "ccol1", T: ddl.Type{Name: ddl.Int64}},
						"ccol2": ddl.ColumnDef{Name: "ccol2", T: ddl.Type{Name: ddl.Int64}},
						"ccol3": ddl.ColumnDef{Name: "ccol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
				"dd": ddl.CreateTable{
					Name:     "dd",
					ColNames: []string{"dcol1", "ddcol2", "dcol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"dcol1":  ddl.ColumnDef{Name: "dcol1", T: ddl.Type{Name: ddl.Int64}},
						"ddcol2": ddl.ColumnDef{Name: "ddcol2", T: ddl.Type{Name: ddl.Int64}},
						"dcol3":  ddl.ColumnDef{Name: "dcol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			unexpecteds: 1,
		},
		{
			name: "Table name not found after lower case check",
			spSchema: map[string]ddl.CreateTable{
				"dd": ddl.CreateTable{
					Name:     "dd",
					ColNames: []string{"dcol1", "ddcol2", "dcol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"dcol1":  ddl.ColumnDef{Name: "dcol1", T: ddl.Type{Name: ddl.Int64}},
						"ddcol2": ddl.ColumnDef{Name: "ddcol2", T: ddl.Type{Name: ddl.Int64}},
						"dcol3":  ddl.ColumnDef{Name: "dcol3", T: ddl.Type{Name: ddl.Int64}},
					},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test4", Columns: []string{"dcol3"}, ReferTable: "ee", ReferColumns: []string{"ecol1"}}},
				},
			},
			expectedSpSchema: map[string]ddl.CreateTable{
				"dd": ddl.CreateTable{
					Name:     "dd",
					ColNames: []string{"dcol1", "ddcol2", "dcol3"},
					ColDefs: map[string]ddl.ColumnDef{
						"dcol1":  ddl.ColumnDef{Name: "dcol1", T: ddl.Type{Name: ddl.Int64}},
						"ddcol2": ddl.ColumnDef{Name: "ddcol2", T: ddl.Type{Name: ddl.Int64}},
						"dcol3":  ddl.ColumnDef{Name: "dcol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			unexpecteds: 1,
		},
	}
	for _, tc := range basicTests {
		conv := MakeConv()
		conv.SpSchema = tc.spSchema
		ResolveRefs(conv)
		assert.Equal(t, tc.expectedSpSchema, conv.SpSchema, tc.name)
		assert.Equal(t, tc.unexpecteds, conv.Unexpecteds())
		conv = nil
	}
}
