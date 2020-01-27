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

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSpannerTable(t *testing.T) {
	conv := MakeConv()
	basicTests := []struct {
		name    string // Name of test.
		pgTable string // PostgreSQL table name to test.
		error   bool   // Whether an error is expected.
		spTable string // Expected Spanner table name.
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
		spTable, err := GetSpannerTable(conv, tc.pgTable)
		if tc.error {
			assert.NotNil(t, err, tc.name)
			continue
		}
		assert.Equal(t, tc.spTable, spTable, tc.name)
		// Run again to check we get same result.
		s2, err := GetSpannerTable(conv, tc.pgTable)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, spTable, s2, tc.name)
	}
}

func TestGetSpannerCol(t *testing.T) {
	conv := MakeConv()
	basicTests := []struct {
		name    string // Name of test.
		pgTable string // PostgreSQL table name to test.
		pgCol   string // PostgreSQL col name to test.
		error   bool   // Whether an error is expected.
		spCol   string // Expected Spanner column name.
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
		_, err1 := GetSpannerTable(conv, tc.pgTable) // Ensure table is known.
		spCol, err2 := GetSpannerCol(conv, tc.pgTable, tc.pgCol, false)
		if tc.error {
			assert.True(t, err1 != nil || err2 != nil, tc.name)
			continue
		}
		assert.Equal(t, tc.spCol, spCol, tc.name)
		// Run again to check we get same result.
		spCol2, err := GetSpannerCol(conv, tc.pgTable, tc.pgCol, false)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, spCol, spCol2, tc.name)
	}
}
