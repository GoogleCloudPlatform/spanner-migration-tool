// Copyright 2019 Google LLC
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSpannerTable(t *testing.T) {
	conv := MakeConv()
	basicTests := []struct {
		name    string // Name of test.
		pgTable string // PostgreSQL table name to test.
		error   bool   // Whether an error is expected.
		changed bool   // Should table name to be changed.
	}{
		{"Empty", "", true, false},
		{"Good name", "table", false, false},
		{"Illegal character", "tab\nle", false, true},
		{"Illegal character with collision (1)", "tab\tle", false, true},
		{"Illegal character with collision (2)", "tab?le", false, true},
		{"Illegal start character", "2table", false, true},
		{"Illegal start character with collision (1)", "_table", false, true},
		{"Illegal start character with collision (2)", "\ntable", false, true},
	}
	for _, tc := range basicTests {
		spTable, err := GetSpannerTable(conv, tc.pgTable)
		if tc.error {
			assert.NotNil(t, err, tc.name)
			continue
		}
		assert.Equal(t, tc.changed, spTable != tc.pgTable, tc.name)
		// Run again to check we get same result.
		s2, err := GetSpannerTable(conv, tc.pgTable)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, spTable, s2, tc.name)
	}
	// Tests that unique PostgreSQL tables are mapped to unique Spanner tables.
	uniquenessTests := []string{"tab\nle", "tab\tle", "tab?le", "tab_le", "tab;le"}
	set := make(map[string]bool) // Set of Spanner tables generated.
	for _, tc := range uniquenessTests {
		spTable, err := GetSpannerTable(conv, tc)
		assert.Nil(t, err, fmt.Sprintf("table %s generated unexpected error", tc))
		assert.False(t, set[spTable], fmt.Sprintf("table %s mapped to existing Spanner table %s", tc, spTable))
		set[spTable] = true
	}
	// Tests the collision avoidance mechanism in GetSpannerTable.
	// This test depends on the specific avoidance algorithm used in GetSpannerTable.
	conv = MakeConv() // Fresh conv (clean state)
	collisionTests := []struct {
		in       string
		expected string
	}{
		{"ta_ble", "ta_ble"},
		{"ta_ble_2", "ta_ble_2"},
		{"ta?ble", "ta_ble_3"}, // First we try "ta_ble", then "ta_ble_2" and finally "ta_ble_3".
	}
	for _, tc := range collisionTests {
		spTable, err := GetSpannerTable(conv, tc.in)
		assert.Nil(t, err, fmt.Sprintf("table %s generated unexpected error", tc.in))
		assert.Equal(t, tc.expected, spTable, fmt.Sprintf("Table collision avoidance test failed: %s", tc.in))
	}
}

func TestGetSpannerCol(t *testing.T) {
	conv := MakeConv()
	basicTests := []struct {
		name    string // Name of test.
		pgTable string // PostgreSQL table name to test
		pgCol   string // PostgreSQL col name to test
		error   bool   // Whether an error is expected.
		changed bool   // Should col name to be changed.
	}{
		{"Empty table", "", "col", true, false},
		{"Empty col", "table", "", true, false},
		{"Acceptable name", "table", "col", false, false},
		{"Bad table", "ta.b\nle", "c\nol", false, true},
		{"Bad col", "table", "c\nol", false, true},
		{"Bad table and col", "t.able", "c\no\nl", false, true},
		{"Collision 1", "table", "c\tol", false, true},
		{"Collosion 2", "table", "c?ol", false, true},
		{"Acceptable name 2", "table1", "col", false, false},
		{"Collision 3", "table1", "c\nol", false, true},
	}
	for _, tc := range basicTests {
		_, err1 := GetSpannerTable(conv, tc.pgTable) // Ensure table is known.
		spCol, err2 := GetSpannerCol(conv, tc.pgTable, tc.pgCol, false)
		if tc.error {
			assert.True(t, err1 != nil || err2 != nil, tc.name)
			continue
		}
		assert.Equal(t, tc.changed, spCol != tc.pgCol, tc.name)
		// Run again to check we get same result.
		spCol2, err := GetSpannerCol(conv, tc.pgTable, tc.pgCol, false)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, spCol, spCol2, tc.name)
	}
	// Tests that unique PostgreSQL tables/cols are mapped to unique Spanner tables/cols.
	uniquenessTests := []struct {
		pgTable string // PostgreSQL table name to test
		pgCol   string // PostgreSQL col name to test
	}{
		{"table", "c\nol"},
		{"table", "c\tol"},
		{"table", "c?ol"},
		{"table1", "col"},
		{"table1", "c\nol"},
		{"table1", "c?ol"},
	}
	set := make(map[string]bool) // Tracks set of Spanner table/col pairs.
	for _, tc := range uniquenessTests {
		spTable, err := GetSpannerTable(conv, tc.pgTable) // Ensure table is known.
		assert.Nil(t, err, fmt.Sprintf("table %s generated unexpected error", tc.pgTable))
		spCol, err := GetSpannerCol(conv, tc.pgTable, tc.pgCol, false)
		assert.Nil(t, err, fmt.Sprintf("table %s col %s generated unexpected error", tc.pgTable, tc.pgCol))
		assert.False(t, set[spTable+spCol], fmt.Sprintf("table %s col %s mapped to existing Spanner col %s", tc.pgTable, tc.pgCol, spCol))
		set[spTable+spCol] = true
	}
	// Tests the collision avoidance mechanism in GetSpannerCol.
	// This test depends on the specific avoidance algorithm used in GetSpannerTable.
	conv = MakeConv() // Fresh conv (clean state)
	table := "table"
	_, err := GetSpannerTable(conv, table) // Ensure table is known.
	assert.Nil(t, err, fmt.Sprintf("table %s generated unexpected error", table))
	collisionTests := []struct {
		in       string
		expected string
	}{
		{"co_l", "co_l"},
		{"co_l_2", "co_l_2"},
		{"co?l", "co_l_3"}, // First we try "co_l", then "co_l_2" and finally "co_l_3".
	}
	for _, tc := range collisionTests {
		spCol, err := GetSpannerCol(conv, table, tc.in, false)
		assert.Nil(t, err, fmt.Sprintf("table %s generated unexpected error", tc.in))
		assert.Equal(t, tc.expected, spCol, fmt.Sprintf("Column collision avoidance test failed: %s", tc.in))
	}

}
