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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestGetSpannerTable(t *testing.T) {
	conv := MakeConv()

	conv = &Conv{

		SrcSchema: map[string]schema.Table{
			"t2": {
				Name: "table",
				Id:   "t2",
			},
			"t3": {
				Name: "tab_le_5",
				Id:   "t3",
			},
			"t4": {
				Name: "tab\nle",
				Id:   "t4",
			},
			"t5": {
				Name: "tab\tle",
				Id:   "t5",
			},
			"t6": {
				Name: "tab?le",
				Id:   "t6",
			},
			"t7": {
				Name: "tab_le_4",
				Id:   "t7",
			},
			"t8": {
				Name: "2table",
				Id:   "t8",
			},
			"t9": {
				Name: "tab_le_6",
				Id:   "t9",
			},
			"t10": {
				Name: "_table",
				Id:   "t10",
			},
			"t11": {
				Name: "\ntable",
				Id:   "t11",
			},
			"t12": {
				Name: "TABLE",
				Id:   "t12",
			},
			"t13": {
				Name: "TAB\nLE_5",
				Id:   "t13",
			},
		},

		SpSchema: map[string]ddl.CreateTable{
			"t2": {
				Name: "table",
				Id:   "t2",
			}, "t3": {
				Name: "tab_le_5",
				Id:   "t3",
			}, "t4": {
				Name: "tab_le",
				Id:   "t4",
			},
			"t5": {
				Name: "tab_le_4",
				Id:   "t5",
			},
			"t6": {
				Name: "tab_le_7",
				Id:   "t6",
			},
			"t7": {
				Name: "tab_le_4_6",
				Id:   "t7",
			},
			"t8": {
				Name: "Atable",
				Id:   "t8",
			},
			"t9": {
				Name: "tab_le_6",
				Id:   "t9",
			},
			"t10": {
				Name: "Atable_8",
				Id:   "t10",
			},
			"t11": {
				Name: "Atable_9",
				Id:   "t11",
			},
			"t12": {
				Name: "TABLE_10",
				Id:   "t12",
			},
			"t13": {
				Name: "TAB_LE_5_11",
				Id:   "t13",
			},
		},
	}

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
		{"Name differing only in case", "TABLE", false, "TABLE_10"},
		{"Illegal differing only in case", "TAB\nLE_5", false, "TAB_LE_5_11"},
	}
	for _, tc := range basicTests {
		tableId, _ := GetTableIdFromSrcName(conv.SrcSchema, tc.srcTable)
		spTable, err := GetSpannerTable(conv, tableId)
		if tc.error {
			assert.NotNil(t, err, tc.name)
			continue
		}
		assert.Equal(t, tc.spTable, spTable, tc.name)
		// Run again to check we get same result.
		s2, err := GetSpannerTable(conv, tableId)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, spTable, s2, tc.name)
	}
}

func TestGetSpannerCol(t *testing.T) {
	conv := MakeConv()
	conv.SrcSchema = map[string]schema.Table{
		"t1": {
			Name:   "table",
			Id:     "t1",
			ColIds: []string{"c1", "c2"},
			ColDefs: map[string]schema.Column{
				"c1": {Name: "col", Id: "c1"},
				"c2": {Name: "c\nol", Id: "c2"},
			},
		},
		"t2": {
			Name:   "ta.b\nle",
			Id:     "t2",
			ColIds: []string{"c3"},
			ColDefs: map[string]schema.Column{
				"c3": {Name: "col", Id: "c3"},
			},
		},
		"t3": {
			Name:   "t.able",
			Id:     "t3",
			ColIds: []string{"c4"},
			ColDefs: map[string]schema.Column{
				"c4": {Name: "c\no\nl", Id: "c4"},
			},
		},
		"t4": {
			Name:   "table1",
			Id:     "t4",
			ColIds: []string{"c5", "c6", "c7"},
			ColDefs: map[string]schema.Column{
				"c1": {Name: "col", Id: "c1"},
				"c2": {Name: "c_ol", Id: "c2"},
				"c3": {Name: "c_ol_5", Id: "c3"},
				"c4": {Name: "c_ol_6", Id: "c4"},
				"c5": {Name: "c\tol", Id: "c5"},
				"c6": {Name: "c\nol", Id: "c6"},
				"c7": {Name: "c?ol", Id: "c7"},
			},
		},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": {
			Name: "table",
			Id:   "t1",
		},
		"t2": {
			Name: "ta_b_le",
			Id:   "t2",
		},
		"t3": {
			Name: "t_able",
			Id:   "t3",
		},
		"t4": {
			Name: "table1",
			Id:   "t4",
		},
	}
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
	}
	for _, tc := range basicTests {
		tableId, _ := GetTableIdFromSrcName(conv.SrcSchema, tc.srcTable)
		_, err1 := GetSpannerTable(conv, tableId) // Ensure table is known.
		colId, _ := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, tc.srcCol)
		spCol, err2 := GetSpannerCol(conv, tableId, colId, conv.SpSchema[tableId].ColDefs)
		if tc.error {
			assert.True(t, err1 != nil || err2 != nil, tc.name)
			continue
		}
		assert.Equal(t, tc.spCol, spCol, tc.name)
	}

	//Column name collision test
	conv.SpSchema["t4"] = ddl.CreateTable{
		Name:   "table1",
		Id:     "t4",
		ColIds: []string{"c5", "c6", "c7"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1": {Name: "col", Id: "c1"},
			"c2": {Name: "c_ol", Id: "c2"},
			"c3": {Name: "c_ol_5", Id: "c3"},
			"c4": {Name: "c_ol_6", Id: "c4"},
		},
	}

	collisionTests := []struct {
		name     string // Name of test.
		srcTable string // Source DB table name to test.
		srcCol   string // Source DB col name to test.
		error    bool   // Whether an error is expected.
		spCol    string // Expected Spanner column name.
	}{
		{"table1 collision 1", "table1", "c\tol", false, "c_ol_4"},
		{"table1 collision 2", "table1", "c\nol", false, "c_ol_7"}, // Skip c_ol_5 and c_ol_6.
		{"table1 collision 3", "table1", "c?ol", false, "c_ol_8"},
	}
	for _, tc := range collisionTests {
		tableId, _ := GetTableIdFromSrcName(conv.SrcSchema, tc.srcTable)
		_, err1 := GetSpannerTable(conv, tableId) // Ensure table is known.
		colId, _ := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, tc.srcCol)
		spCol, err2 := GetSpannerCol(conv, tableId, colId, conv.SpSchema[tableId].ColDefs)
		if tc.error {
			assert.True(t, err1 != nil || err2 != nil, tc.name)
			continue
		}
		assert.Equal(t, tc.spCol, spCol, tc.name)
		conv.SpSchema[tableId].ColDefs[colId] = ddl.ColumnDef{Name: spCol, Id: colId}
	}
}

func TestToSpannerForeignKey(t *testing.T) {
	conv := MakeConv()
	basicTests := []struct {
		name       string // Name of test.
		srcKeyName string // Source foreign key name.
		spKeyName  string // Expected Spanner foreign key name.
	}{
		{"Good name", "fktest", "fktest"},
		{"Empty name", "", ""},
	}
	for _, tc := range basicTests {
		spKeyName := ToSpannerForeignKey(conv, tc.srcKeyName)
		assert.Equal(t, tc.spKeyName, spKeyName, tc.name)
	}
}

func TestGetSpannerID(t *testing.T) {
	conv := MakeConv()
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
		{"Name with different case collision", "INdex1", "INdex1_12"},
		{"Bad name with different case collision", "IN\tDex", "IN_Dex_13"},
	}
	for _, tc := range basicTests {
		spKeyName := getSpannerValidName(conv, tc.srcKeyName)
		assert.Equal(t, tc.spKeyName, spKeyName, tc.name)
	}
}

func TestResolveRefs(t *testing.T) {
	basicTests := []struct {
		name             string     // Name of test.
		spSchema         ddl.Schema // Spanner schema.
		expectedSpSchema ddl.Schema // Expected Spanner schema.
		unexpecteds      int64      // Expected unexpected conditions
	}{
		{
			name: "Refer column Id not found",
			spSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:   "cc",
					Id:     "t1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "ccol1", T: ddl.Type{Name: ddl.Int64}},
						"c2": {Name: "ccol2", T: ddl.Type{Name: ddl.Int64}},
						"c3": {Name: "ccol3", T: ddl.Type{Name: ddl.Int64}},
					},
					ForeignKeys: []ddl.Foreignkey{{Name: "fk_test3", ColIds: []string{"c2", "c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c4", "c9"}}},
				},
				"t2": {
					Name:   "dd",
					Id:     "t2",
					ColIds: []string{"c4", "c5", "c6"},
					ColDefs: map[string]ddl.ColumnDef{
						"c4": {Name: "dcol1", T: ddl.Type{Name: ddl.Int64}},
						"c5": {Name: "ddcol2", T: ddl.Type{Name: ddl.Int64}},
						"c6": {Name: "dcol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			expectedSpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:   "cc",
					Id:     "t1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "ccol1", T: ddl.Type{Name: ddl.Int64}},
						"c2": {Name: "ccol2", T: ddl.Type{Name: ddl.Int64}},
						"c3": {Name: "ccol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
				"t2": {
					Name:   "dd",
					Id:     "t2",
					ColIds: []string{"c4", "c5", "c6"},
					ColDefs: map[string]ddl.ColumnDef{
						"c4": {Name: "dcol1", T: ddl.Type{Name: ddl.Int64}},
						"c5": {Name: "ddcol2", T: ddl.Type{Name: ddl.Int64}},
						"c6": {Name: "dcol3", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			unexpecteds: 1,
		},
		{
			name: "Refer table Id not found",
			spSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:   "dd",
					Id:     "t1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "dcol1", T: ddl.Type{Name: ddl.Int64}},
						"c2": {Name: "ddcol2", T: ddl.Type{Name: ddl.Int64}},
						"c3": {Name: "dcol3", T: ddl.Type{Name: ddl.Int64}},
					},
					ForeignKeys: []ddl.Foreignkey{{Name: "fk_test4", ColIds: []string{"c3"}, ReferTableId: "t3", ReferColumnIds: []string{"c6"}}},
				},
			},
			expectedSpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:   "dd",
					Id:     "t1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "dcol1", T: ddl.Type{Name: ddl.Int64}},
						"c2": {Name: "ddcol2", T: ddl.Type{Name: ddl.Int64}},
						"c3": {Name: "dcol3", T: ddl.Type{Name: ddl.Int64}},
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
