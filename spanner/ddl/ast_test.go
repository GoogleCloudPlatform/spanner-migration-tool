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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

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
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}}, protectIds: true, expected: "col1 INT8"},
	}
	for _, tc := range tests {
		s, _ := tc.in.PrintColumnDef(Config{ProtectIds: tc.protectIds, SpDialect: constants.DIALECT_POSTGRESQL})
		assert.Equal(t, tc.expected, s)
	}
}

func TestPrintPkOrIndexKey(t *testing.T) {
	ct := CreateTable{
		Name:   "table1",
		Id:     "t1",
		ColIds: []string{"c1", "c2"},
		ColDefs: map[string]ColumnDef{
			"c1": {Name: "col1", Id: "c1"},
			"c2": {Name: "col2", Id: "c2"},
		},
	}
	tests := []struct {
		in         IndexKey
		protectIds bool
		spDialect  string
		expected   string
	}{
		{in: IndexKey{ColId: "c1"}, expected: "col1"},
		{in: IndexKey{ColId: "c1", Desc: true}, expected: "col1 DESC"},
		{in: IndexKey{ColId: "c1"}, protectIds: true, expected: "`col1`"},
		{in: IndexKey{ColId: "c1"}, protectIds: true, spDialect: constants.DIALECT_POSTGRESQL, expected: "col1"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.in.PrintPkOrIndexKey(ct, Config{ProtectIds: tc.protectIds, SpDialect: tc.spDialect}))
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
		"",
		cds,
		[]IndexKey{{ColId: "col1", Desc: true}},
		nil,
		nil,
		"",
		"",
		"1",
	}
	t2 := CreateTable{
		"mytable",
		[]string{"col1", "col2", "col3"},
		"",
		cds,
		[]IndexKey{{ColId: "col1", Desc: true}},
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
				"INTERLEAVE IN PARENT ",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.ct.PrintCreateTable(Schema{}, Config{ProtectIds: tc.protectIds}))
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
		"",
		cds,
		[]IndexKey{{ColId: "col1", Desc: true}},
		nil,
		nil,
		"",
		"",
		"1",
	}
	t2 := CreateTable{
		"mytable",
		[]string{"col1", "col2", "col3"},
		"",
		cds,
		[]IndexKey{{ColId: "col1", Desc: true}},
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
			"CREATE TABLE mytable (\n" +
				"	col1 INT8 NOT NULL,\n" +
				"	col2 VARCHAR(2621440),\n" +
				"	col3 BYTEA,\n" +
				"	PRIMARY KEY (col1 DESC)\n" +
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
				") INTERLEAVE IN PARENT ",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.ct.PrintCreateTable(Schema{}, Config{ProtectIds: tc.protectIds, SpDialect: constants.DIALECT_POSTGRESQL}))
	}
}

func TestPrintCreateIndex(t *testing.T) {
	ct := CreateTable{
		Name:   "mytable",
		Id:     "t1",
		ColIds: []string{"c1", "c2"},
		ColDefs: map[string]ColumnDef{
			"c1": {Name: "col1", Id: "c1"},
			"c2": {Name: "col2", Id: "c2"},
		},
	}
	ci := []CreateIndex{
		{
			"myindex",
			"t1",
			/*Unique =*/ false,
			[]IndexKey{{ColId: "c1", Desc: true}, {ColId: "c2"}},
			"i1",
			nil,
		},
		{
			"myindex2",
			"t1",
			/*Unique =*/ true,
			[]IndexKey{{ColId: "c1", Desc: true}, {ColId: "c2"}},
			"i2",
			nil,
		}}
	tests := []struct {
		name       string
		protectIds bool
		spDialect  string
		index      CreateIndex
		expected   string
	}{
		{"no quote non unique", false, "", ci[0], "CREATE INDEX myindex ON mytable (col1 DESC, col2)"},
		{"quote non unique", true, "", ci[0], "CREATE INDEX `myindex` ON `mytable` (`col1` DESC, `col2`)"},
		{"unique key", true, "", ci[1], "CREATE UNIQUE INDEX `myindex2` ON `mytable` (`col1` DESC, `col2`)"},
		{"quote non unique PG", true, constants.DIALECT_POSTGRESQL, ci[0], "CREATE INDEX myindex ON mytable (col1 DESC, col2)"},
		{"unique key PG", true, constants.DIALECT_POSTGRESQL, ci[1], "CREATE UNIQUE INDEX myindex2 ON mytable (col1 DESC, col2)"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.index.PrintCreateIndex(ct, Config{ProtectIds: tc.protectIds, SpDialect: tc.spDialect}))
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
		spDialect  string
		expected   string
		fk         Foreignkey
	}{
		{"no quote", false, "", "CONSTRAINT fk_test FOREIGN KEY (c1, c2) REFERENCES ref_table (ref_c1, ref_c2)", fk[0]},
		{"quote", true, "", "CONSTRAINT `fk_test` FOREIGN KEY (`c1`, `c2`) REFERENCES `ref_table` (`ref_c1`, `ref_c2`)", fk[0]},
		{"no constraint name", false, "", "FOREIGN KEY (c1) REFERENCES ref_table (ref_c1)", fk[1]},
		{"quote PG", true, constants.DIALECT_POSTGRESQL, "CONSTRAINT fk_test FOREIGN KEY (c1, c2) REFERENCES ref_table (ref_c1, ref_c2)", fk[0]},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.fk.PrintForeignKey(Config{ProtectIds: tc.protectIds, SpDialect: tc.spDialect}))
	}
}

func TestPrintForeignKeyAlterTable(t *testing.T) {
	spannerSchema := map[string]CreateTable{
		"t1": CreateTable{
			Name:   "table1",
			ColIds: []string{"c1", "c2", "c3"},
			ColDefs: map[string]ColumnDef{
				"c1": ColumnDef{Name: "productid", T: Type{Name: String, Len: MaxLength}},
				"c2": ColumnDef{Name: "userid", T: Type{Name: String, Len: MaxLength}},
				"c3": ColumnDef{Name: "quantity", T: Type{Name: Int64}},
			},
			ForeignKeys: []Foreignkey{
				{
					"fk_test",
					[]string{"c1", "c2"},
					"t2",
					[]string{"c4", "c5"},
					"f1",
				},
				{
					"",
					[]string{"c1"},
					"t2",
					[]string{"c4"},
					"f2",
				},
			},
		},

		"t2": CreateTable{
			Name:   "table2",
			ColIds: []string{"c4", "c5"},
			ColDefs: map[string]ColumnDef{
				"c4": ColumnDef{Name: "productid", T: Type{Name: String, Len: MaxLength}},
				"c5": ColumnDef{Name: "userid", T: Type{Name: String, Len: MaxLength}},
			},
		}}

	tests := []struct {
		name       string
		table      string
		protectIds bool
		spDialect  string
		expected   string
		fk         Foreignkey
	}{
		{"no quote", "t1", false, "", "ALTER TABLE table1 ADD CONSTRAINT fk_test FOREIGN KEY (productid, userid) REFERENCES table2 (productid, userid)", spannerSchema["t1"].ForeignKeys[0]},
		{"quote", "t1", true, "", "ALTER TABLE `table1` ADD CONSTRAINT `fk_test` FOREIGN KEY (productid, userid) REFERENCES `table2` (productid, userid)", spannerSchema["t1"].ForeignKeys[0]},
		{"no constraint name", "t1", false, "", "ALTER TABLE table1 ADD FOREIGN KEY (productid) REFERENCES table2 (productid)", spannerSchema["t1"].ForeignKeys[1]},
		{"quote PG", "t1", true, constants.DIALECT_POSTGRESQL, "ALTER TABLE table1 ADD CONSTRAINT fk_test FOREIGN KEY (productid, userid) REFERENCES table2 (productid, userid)", spannerSchema["t1"].ForeignKeys[0]},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.fk.PrintForeignKeyAlterTable(spannerSchema, Config{ProtectIds: tc.protectIds, SpDialect: tc.spDialect}, tc.table))
	}
}

func TestPrintAutoGenCol(t *testing.T) {
	tests := []struct {
		agc      AutoGenCol
		expected string
	}{
<<<<<<< HEAD
		{AutoGenCol{Name: constants.UUID, Type: "Pre-defined"}, " DEFAULT (GENERATE_UUID())"},
		{AutoGenCol{Type: "None", Name: "None"}, ""},
=======
		{AutoGenCol{Name: constants.UUID, GenerationType: "Pre-defined"}, " DEFAULT (GENERATE_UUID())"},
		{AutoGenCol{GenerationType: "", Name: ""}, ""},
>>>>>>> master
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.agc.PrintAutoGenCol())
	}
}

func TestPGPrintAutoGenCol(t *testing.T) {
	tests := []struct {
		agc      AutoGenCol
		expected string
	}{
<<<<<<< HEAD
		{AutoGenCol{Name: constants.UUID, Type: "Pre-defined"}, " DEFAULT (spanner.generate_uuid())"},
		{AutoGenCol{Type: "None", Name: "None"}, ""},
=======
		{AutoGenCol{Name: constants.UUID, GenerationType: "Pre-defined"}, " DEFAULT (spanner.generate_uuid())"},
		{AutoGenCol{GenerationType: "", Name: ""}, ""},
>>>>>>> master
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.agc.PGPrintAutoGenCol())
	}
}

func TestGetDDL(t *testing.T) {
	s := Schema{
		"t1": CreateTable{
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2"},
			ColDefs: map[string]ColumnDef{
				"c1": {Name: "a", Id: "c1", T: Type{Name: Int64}},
				"c2": {Name: "b", Id: "c2", T: Type{Name: Int64}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c1"}},
			ForeignKeys: []Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c5"}}},
			Indexes:     []CreateIndex{{Name: "index1", TableId: "t1", Unique: false, Keys: []IndexKey{{ColId: "c2", Desc: false}}}},
		},
		"t2": CreateTable{
			Name:   "table2",
			Id:     "t2",
			ColIds: []string{"c4", "c5", "c6"},
			ColDefs: map[string]ColumnDef{
				"c4": {Name: "a", Id: "c4", T: Type{Name: Int64}},
				"c5": {Name: "b", Id: "c5", T: Type{Name: Int64}},
				"c6": {Name: "c", Id: "c6", T: Type{Name: Int64}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c4"}},
			ForeignKeys: []Foreignkey{{Name: "fk2", ColIds: []string{"c5", "c6"}, ReferTableId: "t3", ReferColumnIds: []string{"c8", "c9"}}},
			Indexes:     []CreateIndex{{Name: "index2", TableId: "t2", Unique: true, Keys: []IndexKey{{ColId: "c5", Desc: true}, {ColId: "c6", Desc: false}}}},
		},
		"t3": CreateTable{
			Name:   "table3",
			Id:     "t3",
			ColIds: []string{"c7", "c8", "c9"},
			ColDefs: map[string]ColumnDef{
				"c7": {Name: "a", Id: "c7", T: Type{Name: Int64}},
				"c8": {Name: "b", Id: "c8", T: Type{Name: Int64}},
				"c9": {Name: "c", Id: "c9", T: Type{Name: Int64}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c7"}, {ColId: "c8"}},
			ParentId:    "t1",
		},
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
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES table2 (b)",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES table3 (b, c)",
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
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES table2 (b)",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES table3 (b, c)",
	}
	assert.ElementsMatch(t, e3, tablesAndFks)
}

func TestGetPGDDL(t *testing.T) {
	s := Schema{
		"t1": CreateTable{
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2"},
			ColDefs: map[string]ColumnDef{
				"c1": {Name: "a", Id: "c1", T: Type{Name: Int64}},
				"c2": {Name: "b", Id: "c2", T: Type{Name: Int64}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c1"}},
			ForeignKeys: []Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
			Indexes:     []CreateIndex{{Name: "index1", TableId: "t1", Unique: false, Keys: []IndexKey{{ColId: "c2", Desc: false}}}},
		},
		"t2": CreateTable{
			Name:   "table2",
			Id:     "t2",
			ColIds: []string{"c3", "c4", "c5"},
			ColDefs: map[string]ColumnDef{
				"c3": {Name: "a", Id: "c3", T: Type{Name: Int64}},
				"c4": {Name: "b", Id: "c4", T: Type{Name: Int64}},
				"c5": {Name: "c", Id: "c5", T: Type{Name: Int64}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c3"}},
			ForeignKeys: []Foreignkey{{Name: "fk2", ColIds: []string{"c4", "c5"}, ReferTableId: "t3", ReferColumnIds: []string{"c7", "c8"}}},
			Indexes:     []CreateIndex{{Name: "index2", TableId: "t2", Unique: true, Keys: []IndexKey{{ColId: "c4", Desc: true}, {ColId: "c5", Desc: false}}}},
		},
		"t3": CreateTable{
			Name:   "table3",
			Id:     "t3",
			ColIds: []string{"c6", "c7", "c8"},
			ColDefs: map[string]ColumnDef{
				"c6": {Name: "a", Id: "c6", T: Type{Name: Int64}},
				"c7": {Name: "b", Id: "c7", T: Type{Name: Int64}},
				"c8": {Name: "c", Id: "c8", T: Type{Name: Int64}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c6"}, {ColId: "c7"}},
			ParentId:    "t1",
		},
	}
	tablesOnly := s.GetDDL(Config{Tables: true, ForeignKeys: false, SpDialect: constants.DIALECT_POSTGRESQL})
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

	fksOnly := s.GetDDL(Config{Tables: false, ForeignKeys: true, SpDialect: constants.DIALECT_POSTGRESQL})
	e2 := []string{
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES table2 (b)",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES table3 (b, c)",
	}
	assert.ElementsMatch(t, e2, fksOnly)

	tablesAndFks := s.GetDDL(Config{Tables: true, ForeignKeys: true, SpDialect: constants.DIALECT_POSTGRESQL})
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
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES table2 (b)",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES table3 (b, c)",
	}
	assert.ElementsMatch(t, e3, tablesAndFks)
}

func TestGetSortedTableIdsBySpName(t *testing.T) {
	testCases := []struct {
		description string
		schema      Schema
		expected    []string
	}{
		// Test Case 1: Empty schema
		{
			description: "Empty schema",
			schema:      Schema{},
			expected:    []string{},
		},
		// Test Case 2: Schema with one table
		{
			description: "Schema with one table",
			schema: Schema{
				"table_id_1": CreateTable{
					Name: "Table1",
					Id:   "table_id_1",
				},
			},
			expected: []string{"table_id_1"},
		},
		// Test Case 3: Schema with interleaved tables
		{
			description: "Schema with interleaved tables",
			schema: Schema{
				"table_id_1": CreateTable{
					Name: "Table1",
					Id:   "table_id_1",
				},
				"table_id_2": CreateTable{
					Name:     "Table2",
					Id:       "table_id_2",
					ParentId: "table_id_1",
				},
				"table_id_3": CreateTable{
					Name:     "Table3",
					Id:       "table_id_3",
					ParentId: "table_id_2",
				},
			},
			expected: []string{"table_id_1", "table_id_2", "table_id_3"},
		},
		// Test Case 4: Schema with tables having no parent
		{
			description: "Schema with tables having no parent",
			schema: Schema{
				"table_id_1": CreateTable{
					Name: "Table1",
					Id:   "table_id_1",
				},
				"table_id_2": CreateTable{
					Name: "Table2",
					Id:   "table_id_2",
				},
			},
			expected: []string{"table_id_1", "table_id_2"},
		},
		// Test Case 5: Schema with a table having a non-existent parent
		{
			description: "Schema with a table having a non-existent parent",
			schema: Schema{
				"table_id_1": CreateTable{
					Name:     "Table1",
					Id:       "table_id_1",
					ParentId: "table_id_2",
				},
			},
			expected: []string{"table_id_1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result := GetSortedTableIdsBySpName(tc.schema)
			assert.ElementsMatch(t, tc.expected, result)
		})
	}
}
