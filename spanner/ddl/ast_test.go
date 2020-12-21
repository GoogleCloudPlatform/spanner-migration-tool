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

package ddl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintScalarType(t *testing.T) {
	tests := []struct {
		in       Type
		expected string
	}{
		{Type{Name: Bool}, "BOOL"},
		{Type{Name: Int64}, "INT64"},
		{Type{Name: Float64}, "FLOAT64"},
		{Type{Name: String, Len: MaxLength}, "STRING(MAX)"},
		{Type{Name: String, Len: int64(42)}, "STRING(42)"},
		{Type{Name: Bytes, Len: MaxLength}, "BYTES(MAX)"},
		{Type{Name: Bytes, Len: int64(42)}, "BYTES(42)"},
		{Type{Name: Date}, "DATE"},
		{Type{Name: Timestamp}, "TIMESTAMP"},
	}
	for _, tc := range tests {
		assert.Equal(t, normalizeSpace(tc.expected), normalizeSpace(tc.in.PrintColumnDefType()))
	}
}

func TestPrintColumnDef(t *testing.T) {
	tests := []struct {
		in         ColumnDef
		protectIds bool
		expected   string
	}{
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}}, expected: "col1 INT64"},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64, IsArray: true}}, expected: "col1 ARRAY<INT64>"},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}, NotNull: true}, expected: "col1 INT64 NOT NULL"},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64, IsArray: true}, NotNull: true}, expected: "col1 ARRAY<INT64> NOT NULL"},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}}, protectIds: true, expected: "`col1` INT64"},
	}
	for _, tc := range tests {
		s, _ := tc.in.PrintColumnDef(Config{ProtectIds: tc.protectIds})
		assert.Equal(t, normalizeSpace(tc.expected), normalizeSpace(s))
	}
}

func TestPrintIndexKey(t *testing.T) {
	tests := []struct {
		in         IndexKey
		protectIds bool
		expected   string
	}{
		{in: IndexKey{Col: "col1"}, expected: "col1"},
		{in: IndexKey{Col: "col1", Desc: true}, expected: "col1 DESC"},
		{in: IndexKey{Col: "col1"}, protectIds: true, expected: "`col1`"},
	}
	for _, tc := range tests {
		assert.Equal(t, normalizeSpace(tc.expected), normalizeSpace(tc.in.PrintIndexKey(Config{ProtectIds: tc.protectIds})))
	}
}

func TestPrintCreateTable(t *testing.T) {
	cds := make(map[string]ColumnDef)
	cds["col1"] = ColumnDef{Name: "col1", T: Type{Name: Int64}, NotNull: true}
	cds["col2"] = ColumnDef{Name: "col2", T: Type{Name: String, Len: MaxLength}, NotNull: false}
	cds["col3"] = ColumnDef{Name: "col3", T: Type{Name: Bytes, Len: int64(42)}, NotNull: false}
	t1 := CreateTable{
		"mytable",
		[]string{"col1", "col2", "col3"},
		cds,
		[]IndexKey{IndexKey{Col: "col1", Desc: true}},
		nil,
		"",
		"",
	}
	t2 := CreateTable{
		"mytable",
		[]string{"col1", "col2", "col3"},
		cds,
		[]IndexKey{IndexKey{Col: "col1", Desc: true}},
		nil,
		"parent",
		"",
	}
	tests := []struct {
		name       string
		protectIds bool
		expected   string
		ct         CreateTable
	}{
		{"no quote", false, "CREATE TABLE mytable (col1 INT64 NOT NULL, col2 STRING(MAX), col3 BYTES(42)) PRIMARY KEY (col1 DESC)", t1},
		{"quote", true, "CREATE TABLE `mytable` (`col1` INT64 NOT NULL, `col2` STRING(MAX), `col3` BYTES(42)) PRIMARY KEY (`col1` DESC)", t1},
		{"interleaved", false, "CREATE TABLE mytable (col1 INT64 NOT NULL, col2 STRING(MAX), col3 BYTES(42)) PRIMARY KEY (col1 DESC),\nINTERLEAVE IN PARENT parent ON DELETE CASCADE", t2},
	}
	for _, tc := range tests {
		assert.Equal(t, normalizeSpace(tc.expected), normalizeSpace(tc.ct.PrintCreateTable(Config{ProtectIds: tc.protectIds})))
	}
}

func TestPrintCreateIndex(t *testing.T) {
	ci := CreateIndex{
		"myindex",
		"mytable",
		[]IndexKey{IndexKey{Col: "col1", Desc: true}, IndexKey{Col: "col2"}},
	}
	tests := []struct {
		name       string
		protectIds bool
		expected   string
	}{
		{"no quote", false, "CREATE INDEX myindex ON mytable (col1 DESC, col2)"},
		{"quote", true, "CREATE INDEX `myindex` ON `mytable` (`col1` DESC, `col2`)"},
	}
	for _, tc := range tests {
		assert.Equal(t, normalizeSpace(tc.expected), normalizeSpace(ci.PrintCreateIndex(Config{ProtectIds: tc.protectIds})))
	}
}

func TestPrintForeignKey(t *testing.T) {
	fk := []Foreignkey{
		Foreignkey{
			"fk_test",
			[]string{"c1", "c2"},
			"ref_table",
			[]string{"ref_c1", "ref_c2"},
		},
		Foreignkey{
			"",
			[]string{"c1"},
			"ref_table",
			[]string{"ref_c1"},
		},
	}
	tests := []struct {
		name       string
		protectIds bool
		expected   string
		fk         Foreignkey
	}{
		{"no quote", false, "CONSTRAINT fk_test FOREIGN KEY (c1,c2) REFERENCES ref_table (ref_c1,ref_c2)", fk[0]},
		{"quote", true, "CONSTRAINT `fk_test` FOREIGN KEY (`c1`,`c2`) REFERENCES `ref_table` (`ref_c1`,`ref_c2`)", fk[0]},
		{"no constraint name", false, "FOREIGN KEY (c1) REFERENCES ref_table (ref_c1)", fk[1]},
	}
	for _, tc := range tests {
		assert.Equal(t, normalizeSpace(tc.expected), normalizeSpace(tc.fk.PrintForeignKey(Config{ProtectIds: tc.protectIds})))
	}
}
func TestPrintForeignKeyAlterTable(t *testing.T) {
	fk := []Foreignkey{
		Foreignkey{
			"fk_test",
			[]string{"c1", "c2"},
			"ref_table",
			[]string{"ref_c1", "ref_c2"},
		},
		Foreignkey{
			"",
			[]string{"c1"},
			"ref_table",
			[]string{"ref_c1"},
		},
	}
	tests := []struct {
		name       string
		table      string
		protectIds bool
		expected   string
		fk         Foreignkey
	}{
		{"no quote", "table1", false, "ALTER TABLE table1 ADD CONSTRAINT fk_test FOREIGN KEY (c1,c2) REFERENCES ref_table (ref_c1,ref_c2)", fk[0]},
		{"quote", "table1", true, "ALTER TABLE `table1` ADD CONSTRAINT `fk_test` FOREIGN KEY (`c1`,`c2`) REFERENCES `ref_table` (`ref_c1`,`ref_c2`)", fk[0]},
		{"no constraint name", "table1", false, "ALTER TABLE table1 ADD FOREIGN KEY (c1) REFERENCES ref_table (ref_c1)", fk[1]},
	}
	for _, tc := range tests {
		assert.Equal(t, normalizeSpace(tc.expected), normalizeSpace(tc.fk.PrintForeignKeyAlterTable(Config{ProtectIds: tc.protectIds}, tc.table)))
	}
}

func normalizeSpace(s string) string {
	// Insert whitespace around parenthesis and commas.
	s = strings.ReplaceAll(s, ")", " ) ")
	s = strings.ReplaceAll(s, "(", " ( ")
	s = strings.ReplaceAll(s, ",", " , ")
	return strings.Join(strings.Fields(s), " ")
}
