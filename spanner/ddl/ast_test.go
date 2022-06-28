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
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
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
		assert.Equal(t, tc.expected, tc.in.PrintColumnDefType())
	}
}

func TestPrintScalarTypePG(t *testing.T) {
	tests := []struct {
		in       Type
		expected string
	}{
		{Type{Name: Bool}, "BOOL"},
		{Type{Name: Int64}, "INT8"},
		{Type{Name: Float64}, "FLOAT8"},
		{Type{Name: String, Len: MaxLength}, "VARCHAR(2621440)"},
		{Type{Name: String, Len: int64(42)}, "VARCHAR(42)"},
		{Type{Name: Bytes, Len: MaxLength}, "BYTEA"},
		{Type{Name: Bytes, Len: int64(42)}, "BYTEA"},
		{Type{Name: Timestamp}, "TIMESTAMPTZ"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.in.PGPrintColumnDefType())
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
		assert.Equal(t, tc.expected, s)
	}
}

func TestPrintColumnDefPG(t *testing.T) {
	tests := []struct {
		in         ColumnDef
		protectIds bool
		expected   string
	}{
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}}, expected: "col1 INT8"},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64, IsArray: true}}, expected: "col1 VARCHAR(2621440)"},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}, NotNull: true}, expected: "col1 INT8 NOT NULL"},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64, IsArray: true}, NotNull: true}, expected: "col1 VARCHAR(2621440) NOT NULL"},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}}, protectIds: true, expected: "\"col1\" INT8"},
	}
	for _, tc := range tests {
		s, _ := tc.in.PrintColumnDef(Config{ProtectIds: tc.protectIds, TargetDb: constants.TargetExperimentalPostgres})
		assert.Equal(t, tc.expected, s)
	}
}

func TestPrintIndexKey(t *testing.T) {
	tests := []struct {
		in         IndexKey
		protectIds bool
		targetDb   string
		expected   string
	}{
		{in: IndexKey{Col: "col1"}, expected: "col1"},
		{in: IndexKey{Col: "col1", Desc: true}, expected: "col1 DESC"},
		{in: IndexKey{Col: "col1"}, protectIds: true, expected: "`col1`"},
		{in: IndexKey{Col: "col1"}, protectIds: true, targetDb: constants.TargetExperimentalPostgres, expected: "\"col1\""},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.in.PrintIndexKey(Config{ProtectIds: tc.protectIds, TargetDb: tc.targetDb}))
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
		[]IndexKey{{Col: "col1", Desc: true}},
		nil,
		nil,
		"",
		"",
		"1",
	}
	t2 := CreateTable{
		"mytable",
		[]string{"col1", "col2", "col3"},
		cds,
		[]IndexKey{{Col: "col1", Desc: true}},
		nil,
		nil,
		"parent",
		"",
		"1",
	}
	tests := []struct {
		name       string
		protectIds bool
		ct         CreateTable
		expected   string
	}{
		{
			"no quote",
			false,
			t1,
			"CREATE TABLE mytable (\n" +
				"	col1 INT64 NOT NULL,\n" +
				"	col2 STRING(MAX),\n" +
				"	col3 BYTES(42),\n" +
				") PRIMARY KEY (col1 DESC)",
		},
		{
			"quote",
			true,
			t1,
			"CREATE TABLE `mytable` (\n" +
				"	`col1` INT64 NOT NULL,\n" +
				"	`col2` STRING(MAX),\n" +
				"	`col3` BYTES(42),\n" +
				") PRIMARY KEY (`col1` DESC)",
		},
		{
			"interleaved",
			false,
			t2,
			"CREATE TABLE mytable (\n" +
				"	col1 INT64 NOT NULL,\n" +
				"	col2 STRING(MAX),\n" +
				"	col3 BYTES(42),\n" +
				") PRIMARY KEY (col1 DESC),\n" +
				"INTERLEAVE IN PARENT parent",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.ct.PrintCreateTable(Config{ProtectIds: tc.protectIds}))
	}
}

func TestPrintCreateTablePG(t *testing.T) {
	cds := make(map[string]ColumnDef)
	cds["col1"] = ColumnDef{Name: "col1", T: Type{Name: Int64}, NotNull: true}
	cds["col2"] = ColumnDef{Name: "col2", T: Type{Name: String, Len: MaxLength}, NotNull: false}
	cds["col3"] = ColumnDef{Name: "col3", T: Type{Name: Bytes, Len: int64(42)}, NotNull: false}
	t1 := CreateTable{
		"mytable",
		[]string{"col1", "col2", "col3"},
		cds,
		[]IndexKey{{Col: "col1", Desc: true}},
		nil,
		nil,
		"",
		"",
		"1",
	}
	t2 := CreateTable{
		"mytable",
		[]string{"col1", "col2", "col3"},
		cds,
		[]IndexKey{{Col: "col1", Desc: true}},
		nil,
		nil,
		"parent",
		"",
		"1",
	}
	tests := []struct {
		name       string
		protectIds bool
		ct         CreateTable
		expected   string
	}{
		{
			"no quote",
			false,
			t1,
			"CREATE TABLE mytable (\n" +
				"	col1 INT8 NOT NULL,\n" +
				"	col2 VARCHAR(2621440),\n" +
				"	col3 BYTEA,\n" +
				"	PRIMARY KEY (col1 DESC)\n" +
				")",
		},
		{
			"quote",
			true,
			t1,
			"CREATE TABLE \"mytable\" (\n" +
				"	\"col1\" INT8 NOT NULL,\n" +
				"	\"col2\" VARCHAR(2621440),\n" +
				"	\"col3\" BYTEA,\n" +
				"	PRIMARY KEY (\"col1\" DESC)\n" +
				")",
		},
		{
			"interleaved",
			false,
			t2,
			"CREATE TABLE mytable (\n" +
				"	col1 INT8 NOT NULL,\n" +
				"	col2 VARCHAR(2621440),\n" +
				"	col3 BYTEA,\n" +
				"	PRIMARY KEY (col1 DESC)\n" +
				") INTERLEAVE IN PARENT parent",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.ct.PrintCreateTable(Config{ProtectIds: tc.protectIds, TargetDb: constants.TargetExperimentalPostgres}))
	}
}

func TestPrintCreateIndex(t *testing.T) {
	ci := []CreateIndex{
		{
			"myindex",
			"mytable",
			/*Unique =*/ false,
			[]IndexKey{{Col: "col1", Desc: true}, {Col: "col2"}},
			"1",
		},
		{
			"myindex2",
			"mytable",
			/*Unique =*/ true,
			[]IndexKey{{Col: "col1", Desc: true}, {Col: "col2"}},
			"1",
		}}
	tests := []struct {
		name       string
		protectIds bool
		targetDb   string
		index      CreateIndex
		expected   string
	}{
		{"no quote non unique", false, "", ci[0], "CREATE INDEX myindex ON mytable (col1 DESC, col2)"},
		{"quote non unique", true, "", ci[0], "CREATE INDEX `myindex` ON `mytable` (`col1` DESC, `col2`)"},
		{"unique key", true, "", ci[1], "CREATE UNIQUE INDEX `myindex2` ON `mytable` (`col1` DESC, `col2`)"},
		{"quote non unique PG", true, constants.TargetExperimentalPostgres, ci[0], "CREATE INDEX \"myindex\" ON \"mytable\" (\"col1\" DESC, \"col2\")"},
		{"unique key PG", true, constants.TargetExperimentalPostgres, ci[1], "CREATE UNIQUE INDEX \"myindex2\" ON \"mytable\" (\"col1\" DESC, \"col2\")"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.index.PrintCreateIndex(Config{ProtectIds: tc.protectIds, TargetDb: tc.targetDb}))
	}
}

func TestPrintForeignKey(t *testing.T) {
	fk := []Foreignkey{
		{
			"fk_test",
			[]string{"c1", "c2"},
			"ref_table",
			[]string{"ref_c1", "ref_c2"},
			"1",
		},
		{
			"",
			[]string{"c1"},
			"ref_table",
			[]string{"ref_c1"},
			"1",
		},
	}
	tests := []struct {
		name       string
		protectIds bool
		targetDb   string
		expected   string
		fk         Foreignkey
	}{
		{"no quote", false, "", "CONSTRAINT fk_test FOREIGN KEY (c1, c2) REFERENCES ref_table (ref_c1, ref_c2)", fk[0]},
		{"quote", true, "", "CONSTRAINT `fk_test` FOREIGN KEY (`c1`, `c2`) REFERENCES `ref_table` (`ref_c1`, `ref_c2`)", fk[0]},
		{"no constraint name", false, "", "FOREIGN KEY (c1) REFERENCES ref_table (ref_c1)", fk[1]},
		{"quote PG", true, constants.TargetExperimentalPostgres, "CONSTRAINT \"fk_test\" FOREIGN KEY (\"c1\", \"c2\") REFERENCES \"ref_table\" (\"ref_c1\", \"ref_c2\")", fk[0]},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.fk.PrintForeignKey(Config{ProtectIds: tc.protectIds, TargetDb: tc.targetDb}))
	}
}

func TestPrintForeignKeyAlterTable(t *testing.T) {
	fk := []Foreignkey{
		{
			"fk_test",
			[]string{"c1", "c2"},
			"ref_table",
			[]string{"ref_c1", "ref_c2"},
			"1",
		},
		{
			"",
			[]string{"c1"},
			"ref_table",
			[]string{"ref_c1"},
			"1",
		},
	}
	tests := []struct {
		name       string
		table      string
		protectIds bool
		targetDb   string
		expected   string
		fk         Foreignkey
	}{
		{"no quote", "table1", false, "", "ALTER TABLE table1 ADD CONSTRAINT fk_test FOREIGN KEY (c1, c2) REFERENCES ref_table (ref_c1, ref_c2)", fk[0]},
		{"quote", "table1", true, "", "ALTER TABLE `table1` ADD CONSTRAINT `fk_test` FOREIGN KEY (`c1`, `c2`) REFERENCES `ref_table` (`ref_c1`, `ref_c2`)", fk[0]},
		{"no constraint name", "table1", false, "", "ALTER TABLE table1 ADD FOREIGN KEY (c1) REFERENCES ref_table (ref_c1)", fk[1]},
		{"quote PG", "table1", true, constants.TargetExperimentalPostgres, "ALTER TABLE \"table1\" ADD CONSTRAINT \"fk_test\" FOREIGN KEY (\"c1\", \"c2\") REFERENCES \"ref_table\" (\"ref_c1\", \"ref_c2\")", fk[0]},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.fk.PrintForeignKeyAlterTable(Config{ProtectIds: tc.protectIds, TargetDb: tc.targetDb}, tc.table))
	}
}

func TestGetDDL(t *testing.T) {
	s := NewSchema()
	s["table1"] = CreateTable{
		Name:     "table1",
		ColNames: []string{"a", "b"},
		ColDefs: map[string]ColumnDef{
			"a": {Name: "a", T: Type{Name: Int64}},
			"b": {Name: "b", T: Type{Name: Int64}},
		},
		Pks:     []IndexKey{{Col: "a"}},
		Fks:     []Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "ref_table1", ReferColumns: []string{"ref_b"}}},
		Indexes: []CreateIndex{{Name: "index1", Table: "table1", Unique: false, Keys: []IndexKey{{Col: "b", Desc: false}}}},
	}
	s["table2"] = CreateTable{
		Name:     "table2",
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ColumnDef{
			"a": {Name: "a", T: Type{Name: Int64}},
			"b": {Name: "b", T: Type{Name: Int64}},
			"c": {Name: "c", T: Type{Name: Int64}},
		},
		Pks:     []IndexKey{{Col: "a"}},
		Fks:     []Foreignkey{{Name: "fk2", Columns: []string{"b", "c"}, ReferTable: "ref_table2", ReferColumns: []string{"ref_b", "ref_c"}}},
		Indexes: []CreateIndex{{Name: "index2", Table: "table2", Unique: true, Keys: []IndexKey{{Col: "b", Desc: true}, {Col: "c", Desc: false}}}},
	}
	s["table3"] = CreateTable{
		Name:     "table3",
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ColumnDef{
			"a": {Name: "a", T: Type{Name: Int64}},
			"b": {Name: "b", T: Type{Name: Int64}},
			"c": {Name: "c", T: Type{Name: Int64}},
		},
		Pks:    []IndexKey{{Col: "a"}, {Col: "b"}},
		Fks:    []Foreignkey{{Name: "fk3", Columns: []string{"c"}, ReferTable: "ref_table3", ReferColumns: []string{"ref_c"}}},
		Parent: "table1",
	}
	tablesOnly := s.GetDDL(Config{Tables: true, ForeignKeys: false})
	e := []string{
		"CREATE TABLE table1 (\n" +
			"	a INT64,\n" +
			"	b INT64,\n" +
			") PRIMARY KEY (a)",
		"CREATE INDEX index1 ON table1 (b)",
		"CREATE TABLE table2 (\n" +
			"	a INT64,\n" +
			"	b INT64,\n" +
			"	c INT64,\n" +
			") PRIMARY KEY (a)",
		"CREATE UNIQUE INDEX index2 ON table2 (b DESC, c)",
		"CREATE TABLE table3 (\n" +
			"	a INT64,\n" +
			"	b INT64,\n" +
			"	c INT64,\n" +
			") PRIMARY KEY (a, b),\n" +
			"INTERLEAVE IN PARENT table1",
	}
	assert.ElementsMatch(t, e, tablesOnly)

	fksOnly := s.GetDDL(Config{Tables: false, ForeignKeys: true})
	e2 := []string{
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES ref_table1 (ref_b)",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES ref_table2 (ref_b, ref_c)",
		"ALTER TABLE table3 ADD CONSTRAINT fk3 FOREIGN KEY (c) REFERENCES ref_table3 (ref_c)",
	}
	assert.ElementsMatch(t, e2, fksOnly)

	tablesAndFks := s.GetDDL(Config{Tables: true, ForeignKeys: true})
	e3 := []string{
		"CREATE TABLE table1 (\n" +
			"	a INT64,\n" +
			"	b INT64,\n" +
			") PRIMARY KEY (a)",
		"CREATE INDEX index1 ON table1 (b)",
		"CREATE TABLE table2 (\n" +
			"	a INT64,\n" +
			"	b INT64,\n" +
			"	c INT64,\n" +
			") PRIMARY KEY (a)",
		"CREATE UNIQUE INDEX index2 ON table2 (b DESC, c)",
		"CREATE TABLE table3 (\n" +
			"	a INT64,\n" +
			"	b INT64,\n" +
			"	c INT64,\n" +
			") PRIMARY KEY (a, b),\n" +
			"INTERLEAVE IN PARENT table1",
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES ref_table1 (ref_b)",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES ref_table2 (ref_b, ref_c)",
		"ALTER TABLE table3 ADD CONSTRAINT fk3 FOREIGN KEY (c) REFERENCES ref_table3 (ref_c)",
	}
	assert.ElementsMatch(t, e3, tablesAndFks)
}

func TestGetPGDDL(t *testing.T) {
	s := NewSchema()
	s["table1"] = CreateTable{
		Name:     "table1",
		ColNames: []string{"a", "b"},
		ColDefs: map[string]ColumnDef{
			"a": {Name: "a", T: Type{Name: Int64}},
			"b": {Name: "b", T: Type{Name: Int64}},
		},
		Pks:     []IndexKey{{Col: "a"}},
		Fks:     []Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "ref_table1", ReferColumns: []string{"ref_b"}}},
		Indexes: []CreateIndex{{Name: "index1", Table: "table1", Unique: false, Keys: []IndexKey{{Col: "b", Desc: false}}}},
	}
	s["table2"] = CreateTable{
		Name:     "table2",
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ColumnDef{
			"a": {Name: "a", T: Type{Name: Int64}},
			"b": {Name: "b", T: Type{Name: Int64}},
			"c": {Name: "c", T: Type{Name: Int64}},
		},
		Pks:     []IndexKey{{Col: "a"}},
		Fks:     []Foreignkey{{Name: "fk2", Columns: []string{"b", "c"}, ReferTable: "ref_table2", ReferColumns: []string{"ref_b", "ref_c"}}},
		Indexes: []CreateIndex{{Name: "index2", Table: "table2", Unique: true, Keys: []IndexKey{{Col: "b", Desc: true}, {Col: "c", Desc: false}}}},
	}
	s["table3"] = CreateTable{
		Name:     "table3",
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ColumnDef{
			"a": {Name: "a", T: Type{Name: Int64}},
			"b": {Name: "b", T: Type{Name: Int64}},
			"c": {Name: "c", T: Type{Name: Int64}},
		},
		Pks:    []IndexKey{{Col: "a"}, {Col: "b"}},
		Fks:    []Foreignkey{{Name: "fk3", Columns: []string{"c"}, ReferTable: "ref_table3", ReferColumns: []string{"ref_c"}}},
		Parent: "table1",
	}
	tablesOnly := s.GetDDL(Config{Tables: true, ForeignKeys: false, TargetDb: constants.TargetExperimentalPostgres})
	e := []string{
		"CREATE TABLE table1 (\n" +
			"	a INT8,\n" +
			"	b INT8,\n" +
			"	PRIMARY KEY (a)\n" +
			")",
		"CREATE INDEX index1 ON table1 (b)",
		"CREATE TABLE table2 (\n" +
			"	a INT8,\n" +
			"	b INT8,\n" +
			"	c INT8,\n" +
			"	PRIMARY KEY (a)\n" +
			")",
		"CREATE UNIQUE INDEX index2 ON table2 (b DESC, c)",
		"CREATE TABLE table3 (\n" +
			"	a INT8,\n" +
			"	b INT8,\n" +
			"	c INT8,\n" +
			"	PRIMARY KEY (a, b)\n" +
			") INTERLEAVE IN PARENT table1",
	}
	assert.ElementsMatch(t, e, tablesOnly)

	fksOnly := s.GetDDL(Config{Tables: false, ForeignKeys: true, TargetDb: constants.TargetExperimentalPostgres})
	e2 := []string{
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES ref_table1 (ref_b)",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES ref_table2 (ref_b, ref_c)",
		"ALTER TABLE table3 ADD CONSTRAINT fk3 FOREIGN KEY (c) REFERENCES ref_table3 (ref_c)",
	}
	assert.ElementsMatch(t, e2, fksOnly)

	tablesAndFks := s.GetDDL(Config{Tables: true, ForeignKeys: true, TargetDb: constants.TargetExperimentalPostgres})
	e3 := []string{
		"CREATE TABLE table1 (\n" +
			"	a INT8,\n" +
			"	b INT8,\n" +
			"	PRIMARY KEY (a)\n" +
			")",
		"CREATE INDEX index1 ON table1 (b)",
		"CREATE TABLE table2 (\n" +
			"	a INT8,\n" +
			"	b INT8,\n" +
			"	c INT8,\n" +
			"	PRIMARY KEY (a)\n" +
			")",
		"CREATE UNIQUE INDEX index2 ON table2 (b DESC, c)",
		"CREATE TABLE table3 (\n" +
			"	a INT8,\n" +
			"	b INT8,\n" +
			"	c INT8,\n" +
			"	PRIMARY KEY (a, b)\n" +
			") INTERLEAVE IN PARENT table1",
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES ref_table1 (ref_b)",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES ref_table2 (ref_b, ref_c)",
		"ALTER TABLE table3 ADD CONSTRAINT fk3 FOREIGN KEY (c) REFERENCES ref_table3 (ref_c)",
	}
	assert.ElementsMatch(t, e3, tablesAndFks)
}
