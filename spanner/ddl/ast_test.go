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
		{Type{Name: Float32}, "FLOAT32"},
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
		{Type{Name: Float32}, "FLOAT4"},
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
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}, NotNull: true}, expected: "col1 INT64 NOT NULL "},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64, IsArray: true}, NotNull: true}, expected: "col1 ARRAY<INT64> NOT NULL "},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}}, protectIds: true, expected: "`col1` INT64"},
		{
			in: ColumnDef{
				Name: "col1",
				T:    Type{Name: Int64},
				DefaultValue: DefaultValue{
					IsPresent: true,
					Value:     Expression{Statement: "(`col2` + 1)"},
				},
			},
			expected: "col1 INT64 DEFAULT ((`col2` + 1))",
		},
		{
			in: ColumnDef{
				Name: "col1",
				T:    Type{Name: Int64},
				Opts: map[string]string{"cassandra_type": "bigint"},
			},
			expected: "col1 INT64 OPTIONS (cassandra_type = 'bigint')",
		},
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
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}, NotNull: true}, expected: "col1 INT8 NOT NULL "},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64, IsArray: true}, NotNull: true}, expected: "col1 VARCHAR(2621440) NOT NULL "},
		{in: ColumnDef{Name: "col1", T: Type{Name: Int64}}, protectIds: true, expected: "col1 INT8"},
		{
			in: ColumnDef{
				Name: "col1",
				T:    Type{Name: Int64},
				DefaultValue: DefaultValue{
					IsPresent: true,
					Value:     Expression{Statement: "(`col2` + 1)"},
				},
			},
			expected: "col1 INT8 DEFAULT ((`col2` + 1))",
		},
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
	s := Schema{
		"t1": CreateTable{
			Name:          "table1",
			ColIds:        []string{"col1", "col2", "col3"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col1": {Name: "col1", T: Type{Name: Int64}, NotNull: true},
				"col2": {Name: "col2", T: Type{Name: String, Len: MaxLength}, NotNull: false},
				"col3": {Name: "col3", T: Type{Name: Bytes, Len: int64(42)}, NotNull: false},
			},
			PrimaryKeys: []IndexKey{{ColId: "col1", Desc: true}},
			ForeignKeys: nil,
			CheckConstraints: []CheckConstraint{
				{Id: "ck1", Name: "check_1", Expr: "(age > 18)"},
				{Id: "ck2", Name: "check_2", Expr: "(age < 99)"},
			},
			Indexes:     nil,
			ParentTable: InterleavedParent{},
			Comment:     "",
			Id:          "t1",
		},
		"t2": CreateTable{
			Name:          "table2",
			ColIds:        []string{"col4", "col5"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col4": {Name: "col4", T: Type{Name: Int64}, NotNull: true},
				"col5": {Name: "col5", T: Type{Name: String, Len: MaxLength}, NotNull: false},
			},
			PrimaryKeys:      []IndexKey{{ColId: "col4", Desc: true}},
			ForeignKeys:      nil,
			Indexes:          nil,
			CheckConstraints: nil,
			ParentTable:      InterleavedParent{Id: "t1", OnDelete: constants.FK_CASCADE, InterleaveType: "IN PARENT"},
			Comment:          "",
			Id:               "t2",
		},
		"t3": CreateTable{
			Name:          "table3",
			ColIds:        []string{"col6"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col6": {Name: "col6", T: Type{Name: Int64}, NotNull: true},
			},
			PrimaryKeys:      []IndexKey{{ColId: "col6", Desc: true}},
			ForeignKeys:      nil,
			Indexes:          nil,
			CheckConstraints: nil,
			ParentTable:      InterleavedParent{Id: "t1", OnDelete: "", InterleaveType: "IN PARENT"},
			Comment:          "",
			Id:               "t3",
		},
		"t4": CreateTable{
			Name:          "table1",
			ColIds:        []string{"col1", "col2", "col3"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col1": {Name: "col1", T: Type{Name: Int64}, NotNull: true},
				"col2": {Name: "col2", T: Type{Name: String, Len: MaxLength}, NotNull: false},
				"col3": {Name: "col3", T: Type{Name: Bytes, Len: int64(42)}, NotNull: false},
			},
			PrimaryKeys: nil,
			ForeignKeys: nil,
			CheckConstraints: []CheckConstraint{
				{Id: "ck1", Name: "check_1", Expr: "(age > 18)"},
				{Id: "ck2", Name: "check_2", Expr: "(age < 99)"},
			},
			Indexes:     nil,
			ParentTable: InterleavedParent{},
			Comment:     "",
			Id:          "t1",
		},
		"t5": CreateTable{
			Name:          "table5",
			ColIds:        []string{"col7", "col8"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col7": {Name: "col7", T: Type{Name: Int64}, NotNull: true},
				"col8": {Name: "col8", T: Type{Name: String, Len: MaxLength}, NotNull: false},
			},
			PrimaryKeys:      []IndexKey{{ColId: "col7", Desc: true}},
			ForeignKeys:      nil,
			Indexes:          nil,
			CheckConstraints: nil,
			ParentTable:      InterleavedParent{Id: "t1", OnDelete: constants.FK_NO_ACTION, InterleaveType: "IN"},
			Comment:          "",
			Id:               "t5",
		},
		"t6": CreateTable{
			Name:          "table6",
			ColIds:        []string{"col9"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col9": {Name: "col9", T: Type{Name: Int64}, NotNull: true},
			},
			PrimaryKeys:      []IndexKey{{ColId: "col9", Desc: true}},
			ForeignKeys:      nil,
			Indexes:          nil,
			CheckConstraints: nil,
			ParentTable:      InterleavedParent{Id: "t1", OnDelete: "", InterleaveType: "IN"},
			Comment:          "",
			Id:               "t6",
		},
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
			s["t1"],
			"CREATE TABLE table1 (\n" +
				"	col1 INT64 NOT NULL ,\n" +
				"	col2 STRING(MAX),\n" +
				"	col3 BYTES(42),\n" +
				"\tCONSTRAINT check_1 CHECK (age > 18),\n\tCONSTRAINT check_2 CHECK (age < 99)\n" +
				") PRIMARY KEY (col1 DESC)",
		},
		{
			"quote",
			true,
			s["t1"],
			"CREATE TABLE `table1` (\n" +
				"	`col1` INT64 NOT NULL ,\n" +
				"	`col2` STRING(MAX),\n" +
				"	`col3` BYTES(42),\n" +
				"\tCONSTRAINT check_1 CHECK (age > 18),\n\tCONSTRAINT check_2 CHECK (age < 99)\n" +
				") PRIMARY KEY (`col1` DESC)",
		},
		{
			"interleaved in parent",
			false,
			s["t2"],
			"CREATE TABLE table2 (\n" +
				"	col4 INT64 NOT NULL ,\n" +
				"	col5 STRING(MAX),\n" +
				") PRIMARY KEY (col4 DESC),\n" +
				"INTERLEAVE IN PARENT table1 ON DELETE CASCADE",
		},
		{
			"interleaved in parent without on delete support",
			false,
			s["t3"],
			"CREATE TABLE table3 (\n" +
				"	col6 INT64 NOT NULL ,\n" +
				") PRIMARY KEY (col6 DESC),\n" +
				"INTERLEAVE IN PARENT table1",
		},
		{
			"no quote",
			false,
			s["t4"],
			"CREATE TABLE table1 (\n" +
				"	col1 INT64 NOT NULL ,\n" +
				"	col2 STRING(MAX),\n" +
				"	col3 BYTES(42),\n" +
				"\tCONSTRAINT check_1 CHECK (age > 18),\n\tCONSTRAINT check_2 CHECK (age < 99)\n" +
				") ",
		},
		{
			"interleaved in",
			false,
			s["t5"],
			"CREATE TABLE table5 (\n" +
				"	col7 INT64 NOT NULL ,\n" +
				"	col8 STRING(MAX),\n" +
				") PRIMARY KEY (col7 DESC),\n" +
				"INTERLEAVE IN table1",
		},
		{
			"interleaved in without on delete support set",
			false,
			s["t6"],
			"CREATE TABLE table6 (\n" +
				"	col9 INT64 NOT NULL ,\n" +
				") PRIMARY KEY (col9 DESC),\n" +
				"INTERLEAVE IN table1",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.ct.PrintCreateTable(s, Config{ProtectIds: tc.protectIds, SpDialect: constants.DIALECT_GOOGLESQL}))
	}
}

func TestPrintCreateTablePG(t *testing.T) {
	s := Schema{
		"t1": CreateTable{
			Name:          "table1",
			ColIds:        []string{"col1", "col2", "col3"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col1": {Name: "col1", T: Type{Name: Int64}, NotNull: true},
				"col2": {Name: "col2", T: Type{Name: String, Len: MaxLength}, NotNull: false},
				"col3": {Name: "col3", T: Type{Name: Bytes, Len: int64(42)}, NotNull: false},
			},
			PrimaryKeys: []IndexKey{{ColId: "col1", Desc: true}},
			ForeignKeys: nil,
			CheckConstraints: []CheckConstraint{
				{Id: "ck1", Name: "check_1", Expr: "(age > 18)"},
				{Id: "ck2", Name: "check_2", Expr: "(age < 99)"},
			},
			Indexes:     nil,
			ParentTable: InterleavedParent{},
			Comment:     "",
			Id:          "t1",
		},
		"t2": CreateTable{
			Name:          "table2",
			ColIds:        []string{"col4", "col5"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col4": {Name: "col4", T: Type{Name: Int64}, NotNull: true},
				"col5": {Name: "col5", T: Type{Name: String, Len: MaxLength}, NotNull: false},
			},
			PrimaryKeys:      []IndexKey{{ColId: "col4", Desc: true}},
			ForeignKeys:      nil,
			Indexes:          nil,
			CheckConstraints: nil,
			ParentTable:      InterleavedParent{Id: "t1", OnDelete: constants.FK_CASCADE, InterleaveType: "IN PARENT"},
			Comment:          "",
			Id:               "t2",
		},
		"t3": CreateTable{
			Name:          "table3",
			ColIds:        []string{"col6"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col6": {Name: "col6", T: Type{Name: Int64}, NotNull: true},
			},
			PrimaryKeys:      []IndexKey{{ColId: "col6", Desc: true}},
			ForeignKeys:      nil,
			Indexes:          nil,
			CheckConstraints: nil,
			ParentTable:      InterleavedParent{Id: "t1", OnDelete: "", InterleaveType: "IN PARENT"},
			Comment:          "",
			Id:               "t3",
		},
		"t4": CreateTable{
			Name:          "table4",
			ColIds:        []string{"col7", "col8"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col7": {Name: "col7", T: Type{Name: Int64}, NotNull: true},
				"col8": {Name: "col8", T: Type{Name: String, Len: MaxLength}, NotNull: false},
			},
			PrimaryKeys:      []IndexKey{{ColId: "col7", Desc: true}},
			ForeignKeys:      nil,
			Indexes:          nil,
			CheckConstraints: nil,
			ParentTable:      InterleavedParent{Id: "t1", OnDelete: constants.FK_NO_ACTION, InterleaveType: "IN"},
			Comment:          "",
			Id:               "t4",
		},
		"t5": CreateTable{
			Name:          "table5",
			ColIds:        []string{"col9"},
			ShardIdColumn: "",
			ColDefs: map[string]ColumnDef{
				"col9": {Name: "col9", T: Type{Name: Int64}, NotNull: true},
			},
			PrimaryKeys:      []IndexKey{{ColId: "col9", Desc: true}},
			ForeignKeys:      nil,
			Indexes:          nil,
			CheckConstraints: nil,
			ParentTable:      InterleavedParent{Id: "t1", OnDelete: "", InterleaveType: "IN"},
			Comment:          "",
			Id:               "t5",
		},
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
			s["t1"],
			"CREATE TABLE table1 (\n" +
				"	col1 INT8 NOT NULL ,\n" +
				"	col2 VARCHAR(2621440),\n" +
				"	col3 BYTEA,\n" +
				"\tCONSTRAINT check_1 CHECK (age > 18),\n\tCONSTRAINT check_2 CHECK (age < 99),\n" +
				"	PRIMARY KEY (col1 DESC)\n" +
				")",
		},
		{
			"quote",
			true,
			s["t1"],
			"CREATE TABLE table1 (\n" +
				"	col1 INT8 NOT NULL ,\n" +
				"	col2 VARCHAR(2621440),\n" +
				"	col3 BYTEA,\n" +
				"\tCONSTRAINT check_1 CHECK (age > 18),\n\tCONSTRAINT check_2 CHECK (age < 99),\n" +
				"	PRIMARY KEY (col1 DESC)\n" +
				")",
		},
		{
			"interleaved in parent",
			false,
			s["t2"],
			"CREATE TABLE table2 (\n" +
				"	col4 INT8 NOT NULL ,\n" +
				"	col5 VARCHAR(2621440),\n" +
				"	PRIMARY KEY (col4 DESC)\n" +
				") INTERLEAVE IN PARENT table1 ON DELETE CASCADE",
		},
		{
			"interleaved in parent without on delete support",
			false,
			s["t3"],
			"CREATE TABLE table3 (\n" +
				"	col6 INT8 NOT NULL ,\n" +
				"	PRIMARY KEY (col6 DESC)\n" +
				") INTERLEAVE IN PARENT table1",
		},
		{
			"interleaved in",
			false,
			s["t4"],
			"CREATE TABLE table4 (\n" +
				"	col7 INT8 NOT NULL ,\n" +
				"	col8 VARCHAR(2621440),\n" +
				"	PRIMARY KEY (col7 DESC)\n" +
				") INTERLEAVE IN table1",
		},
		{
			"interleaved in without on delete support set",
			false,
			s["t5"],
			"CREATE TABLE table5 (\n" +
				"	col9 INT8 NOT NULL ,\n" +
				"	PRIMARY KEY (col9 DESC)\n" +
				") INTERLEAVE IN table1",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.ct.PrintCreateTable(s, Config{ProtectIds: tc.protectIds, SpDialect: constants.DIALECT_POSTGRESQL}))
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
		},
	}
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
			constants.FK_NO_ACTION,
			constants.FK_NO_ACTION,
		},
		{
			"",
			[]string{"c1"},
			"ref_table",
			[]string{"ref_c1"},
			"1",
			constants.FK_CASCADE,
			constants.FK_NO_ACTION,
		},
		{
			"fk_test",
			[]string{"c1", "c2"},
			"ref_table",
			[]string{"ref_c1", "ref_c2"},
			"1",
			"",
			"",
		},
	}
	tests := []struct {
		name       string
		protectIds bool
		spDialect  string
		expected   string
		fk         Foreignkey
	}{
		{"no quote", false, "", "CONSTRAINT fk_test FOREIGN KEY (c1, c2) REFERENCES ref_table (ref_c1, ref_c2) ON DELETE NO ACTION", fk[0]},
		{"quote", true, "", "CONSTRAINT `fk_test` FOREIGN KEY (`c1`, `c2`) REFERENCES `ref_table` (`ref_c1`, `ref_c2`) ON DELETE NO ACTION", fk[0]},
		{"no constraint name", false, "", "FOREIGN KEY (c1) REFERENCES ref_table (ref_c1) ON DELETE CASCADE", fk[1]},
		{"quote PG", true, constants.DIALECT_POSTGRESQL, "CONSTRAINT fk_test FOREIGN KEY (c1, c2) REFERENCES ref_table (ref_c1, ref_c2) ON DELETE NO ACTION", fk[0]},
		{"foreign key constraints not supported i.e. dont print ON DELETE", false, "", "CONSTRAINT fk_test FOREIGN KEY (c1, c2) REFERENCES ref_table (ref_c1, ref_c2)", fk[2]},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.fk.PrintForeignKey(Config{ProtectIds: tc.protectIds, SpDialect: tc.spDialect}))
		})
	}
}

func TestPrintForeignKeyAlterTable(t *testing.T) {
	spannerSchema := map[string]CreateTable{
		"t1": {
			Name:   "table1",
			ColIds: []string{"c1", "c2", "c3", "c4"},
			ColDefs: map[string]ColumnDef{
				"c1": {Name: "productid", T: Type{Name: String, Len: MaxLength}},
				"c2": {Name: "userid", T: Type{Name: String, Len: MaxLength}},
				"c3": {Name: "quantity", T: Type{Name: Int64}},
				"c4": {Name: "from", T: Type{Name: String, Len: MaxLength}},
			},
			ForeignKeys: []Foreignkey{
				{
					"fk_test",
					[]string{"c1", "c2", "c4"},
					"t2",
					[]string{"c5", "c6", "c7"},
					"f1",
					constants.FK_CASCADE,
					constants.FK_NO_ACTION,
				},
				{
					"",
					[]string{"c1"},
					"t2",
					[]string{"c5"},
					"f2",
					constants.FK_NO_ACTION,
					constants.FK_NO_ACTION,
				},
				{
					"fk_test2",
					[]string{"c1", "c2"},
					"t2",
					[]string{"c5", "c6"},
					"f1",
					"",
					"",
				},
			},
		},

		"t2": {
			Name:   "table2",
			ColIds: []string{"c4", "c5"},
			ColDefs: map[string]ColumnDef{
				"c5": {Name: "productid", T: Type{Name: String, Len: MaxLength}},
				"c6": {Name: "userid", T: Type{Name: String, Len: MaxLength}},
				"c7": {Name: "from", T: Type{Name: String, Len: MaxLength}},
			},
		},
	}

	tests := []struct {
		name       string
		table      string
		protectIds bool
		spDialect  string
		expected   string
		fk         Foreignkey
	}{
		{"no quote", "t1", false, "", "ALTER TABLE table1 ADD CONSTRAINT fk_test FOREIGN KEY (productid, userid, from) REFERENCES table2 (productid, userid, from) ON DELETE CASCADE", spannerSchema["t1"].ForeignKeys[0]},
		{"quote", "t1", true, "", "ALTER TABLE `table1` ADD CONSTRAINT `fk_test` FOREIGN KEY (`productid`, `userid`, `from`) REFERENCES `table2` (`productid`, `userid`, `from`) ON DELETE CASCADE", spannerSchema["t1"].ForeignKeys[0]},
		{"no constraint name", "t1", false, "", "ALTER TABLE table1 ADD FOREIGN KEY (productid) REFERENCES table2 (productid) ON DELETE NO ACTION", spannerSchema["t1"].ForeignKeys[1]},
		{"quote PG", "t1", true, constants.DIALECT_POSTGRESQL, "ALTER TABLE table1 ADD CONSTRAINT fk_test FOREIGN KEY (productid, userid, \"from\") REFERENCES table2 (productid, userid, \"from\") ON DELETE CASCADE", spannerSchema["t1"].ForeignKeys[0]},
		{"foreign key constraints not supported i.e. dont print ON DELETE", "t1", false, "", "ALTER TABLE table1 ADD CONSTRAINT fk_test2 FOREIGN KEY (productid, userid) REFERENCES table2 (productid, userid)", spannerSchema["t1"].ForeignKeys[2]},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.fk.PrintForeignKeyAlterTable(spannerSchema, Config{ProtectIds: tc.protectIds, SpDialect: tc.spDialect}, tc.table))
		})
	}
}

func TestPrintDefaultValue(t *testing.T) {
	tests := []struct {
		name     string
		dv       DefaultValue
		ty       Type
		expected string
	}{
		{
			name: "default value present",
			dv: DefaultValue{
				IsPresent: true,
				Value:     Expression{Statement: "(`col1` + 1)"},
			},
			ty: Type{
				Name: "INT64",
			},
			expected: " DEFAULT ((`col1` + 1))",
		},
		{
			name: "default value present",
			dv: DefaultValue{
				IsPresent: true,
				Value:     Expression{Statement: "(`col1` + 1)"},
			},
			ty: Type{
				Name: "NUMERIC",
			},
			expected: " DEFAULT (CAST((`col1` + 1) AS NUMERIC))",
		},
		{
			name: "empty default value",
			dv:   DefaultValue{},
			ty: Type{
				Name: "INT64",
			},
			expected: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.dv.PrintDefaultValue(tc.ty))
		})
	}
}

func TestPGPrintDefaultValue(t *testing.T) {
	tests := []struct {
		name     string
		dv       DefaultValue
		ty       Type
		expected string
	}{
		{
			name: "default value present",
			dv: DefaultValue{
				IsPresent: true,
				Value:     Expression{Statement: "(`col1` + 1)"},
			},
			ty: Type{
				Name: "INT64",
			},
			expected: " DEFAULT ((`col1` + 1))",
		},
		{
			name: "default value present",
			dv: DefaultValue{
				IsPresent: true,
				Value:     Expression{Statement: "(`col1` + 1)"},
			},
			ty: Type{
				Name: "NUMERIC",
			},
			expected: " DEFAULT (CAST((`col1` + 1) AS NUMERIC))",
		},
		{
			name: "empty default value",
			dv:   DefaultValue{},
			ty: Type{
				Name: "INT64",
			},
			expected: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.dv.PGPrintDefaultValue(tc.ty))
		})
	}
}

func TestPrintAutoGenCol(t *testing.T) {
	tests := []struct {
		agc      AutoGenCol
		expected string
	}{
		{AutoGenCol{Name: constants.UUID, GenerationType: "Pre-defined"}, " DEFAULT (GENERATE_UUID())"},
		{AutoGenCol{GenerationType: "", Name: ""}, ""},
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
		{AutoGenCol{Name: constants.UUID, GenerationType: "Pre-defined"}, " DEFAULT (spanner.generate_uuid())"},
		{AutoGenCol{GenerationType: "", Name: ""}, ""},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.agc.PGPrintAutoGenCol())
	}
}

func TestPrintSequence(t *testing.T) {
	s1 := Sequence{
		Id:               "s1",
		Name:             "sequence1",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "",
		SkipRangeMax:     "",
		StartWithCounter: "",
	}
	s2 := Sequence{
		Id:               "s2",
		Name:             "sequence2",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "0",
		SkipRangeMax:     "1",
		StartWithCounter: "",
	}
	s3 := Sequence{
		Id:               "s3",
		Name:             "sequence3",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "",
		SkipRangeMax:     "",
		StartWithCounter: "7",
	}
	s4 := Sequence{
		Id:               "s3",
		Name:             "sequence4",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "0",
		SkipRangeMax:     "1",
		StartWithCounter: "7",
	}
	tests := []struct {
		name       string
		sequence   Sequence
		protectIds bool
		spDialect  string
		expected   string
	}{
		{
			name:       "no optional values set",
			sequence:   s1,
			protectIds: true,
			spDialect:  constants.DIALECT_GOOGLESQL,
			expected:   "CREATE SEQUENCE `sequence1` OPTIONS (sequence_kind='bit_reversed_positive') ",
		},
		{
			name:       "min and max skip range set",
			sequence:   s2,
			protectIds: false,
			spDialect:  constants.DIALECT_GOOGLESQL,
			expected:   "CREATE SEQUENCE sequence2 OPTIONS (sequence_kind='bit_reversed_positive', skip_range_min = 0, skip_range_max = 1) ",
		},
		{
			name:       "start with counter set",
			sequence:   s3,
			protectIds: false,
			spDialect:  constants.DIALECT_GOOGLESQL,
			expected:   "CREATE SEQUENCE sequence3 OPTIONS (sequence_kind='bit_reversed_positive', start_with_counter = 7) ",
		},
		{
			name:       "all optional values set",
			sequence:   s4,
			protectIds: false,
			spDialect:  constants.DIALECT_GOOGLESQL,
			expected:   "CREATE SEQUENCE sequence4 OPTIONS (sequence_kind='bit_reversed_positive', skip_range_min = 0, skip_range_max = 1, start_with_counter = 7) ",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.sequence.PrintSequence(
			Config{ProtectIds: tc.protectIds, SpDialect: tc.spDialect}))
	}
}

func TestPGPrintSequence(t *testing.T) {
	s1 := Sequence{
		Id:               "s1",
		Name:             "sequence1",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "",
		SkipRangeMax:     "",
		StartWithCounter: "",
	}
	s2 := Sequence{
		Id:               "s2",
		Name:             "sequence2",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "0",
		SkipRangeMax:     "1",
		StartWithCounter: "",
	}
	s3 := Sequence{
		Id:               "s3",
		Name:             "sequence3",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "",
		SkipRangeMax:     "",
		StartWithCounter: "7",
	}
	s4 := Sequence{
		Id:               "s3",
		Name:             "sequence4",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "0",
		SkipRangeMax:     "1",
		StartWithCounter: "7",
	}
	s5 := Sequence{
		Id:           "s5",
		Name:         "from",
		SequenceKind: "BIT REVERSED POSITIVE",
	}
	tests := []struct {
		name       string
		sequence   Sequence
		protectIds bool
		spDialect  string
		expected   string
	}{
		{
			name:       "no optional values set",
			sequence:   s1,
			protectIds: true,
			spDialect:  constants.DIALECT_POSTGRESQL,
			expected:   "CREATE SEQUENCE sequence1 BIT_REVERSED_POSITIVE",
		},
		{
			name:       "min and max skip range set",
			sequence:   s2,
			protectIds: false,
			spDialect:  constants.DIALECT_POSTGRESQL,
			expected:   "CREATE SEQUENCE sequence2 BIT_REVERSED_POSITIVE SKIP RANGE 0 1",
		},
		{
			name:       "start with counter set",
			sequence:   s3,
			protectIds: false,
			spDialect:  constants.DIALECT_POSTGRESQL,
			expected:   "CREATE SEQUENCE sequence3 BIT_REVERSED_POSITIVE START COUNTER WITH 7",
		},
		{
			name:       "all optional values set",
			sequence:   s4,
			protectIds: false,
			spDialect:  constants.DIALECT_POSTGRESQL,
			expected:   "CREATE SEQUENCE sequence4 BIT_REVERSED_POSITIVE SKIP RANGE 0 1 START COUNTER WITH 7",
		},
		{
			name:       "no optional values set with protected squence name",
			sequence:   s5,
			protectIds: true,
			spDialect:  constants.DIALECT_POSTGRESQL,
			expected:   "CREATE SEQUENCE \"from\" BIT_REVERSED_POSITIVE",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.sequence.PGPrintSequence(Config{ProtectIds: tc.protectIds, SpDialect: tc.spDialect}))
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
			ForeignKeys: []Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c5"}, OnDelete: constants.FK_CASCADE, OnUpdate: constants.FK_NO_ACTION}},
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
			ForeignKeys: []Foreignkey{{Name: "fk2", ColIds: []string{"c5", "c6"}, ReferTableId: "t3", ReferColumnIds: []string{"c8", "c9"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION}},
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
			ParentTable: InterleavedParent{Id: "t1", OnDelete: constants.FK_CASCADE, InterleaveType: "IN PARENT"},
		},
		"t4": CreateTable{
			Name:   "table4",
			Id:     "t4",
			ColIds: []string{"c10", "c11", "c12"},
			ColDefs: map[string]ColumnDef{
				"c10": {Name: "a", Id: "c7", T: Type{Name: Int64}},
				"c11": {Name: "b", Id: "c8", T: Type{Name: Int64}},
				"c12": {Name: "c", Id: "c9", T: Type{Name: Int64}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c10"}, {ColId: "c11"}},
			ParentTable: InterleavedParent{Id: "t1", OnDelete: constants.FK_NO_ACTION, InterleaveType: "IN"},
		},
	}
	tablesOnly := GetDDL(Config{Tables: true, ForeignKeys: false}, s, make(map[string]Sequence), DatabaseOptions{})
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
			"INTERLEAVE IN PARENT table1 ON DELETE CASCADE",
		"CREATE TABLE table4 (\n" +
			"	a INT64,\n" +
			"	b INT64,\n" +
			"	c INT64,\n" +
			") PRIMARY KEY (a, b),\n" +
			"INTERLEAVE IN table1",
	}
	assert.ElementsMatch(t, e, tablesOnly)

	fksOnly := GetDDL(Config{Tables: false, ForeignKeys: true}, s, make(map[string]Sequence), DatabaseOptions{})
	e2 := []string{
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES table2 (b) ON DELETE CASCADE",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES table3 (b, c) ON DELETE NO ACTION",
	}
	assert.ElementsMatch(t, e2, fksOnly)

	tablesAndFks := GetDDL(Config{Tables: true, ForeignKeys: true}, s, make(map[string]Sequence), DatabaseOptions{})
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
			"INTERLEAVE IN PARENT table1 ON DELETE CASCADE",
		"CREATE TABLE table4 (\n" +
			"	a INT64,\n" +
			"	b INT64,\n" +
			"	c INT64,\n" +
			") PRIMARY KEY (a, b),\n" +
			"INTERLEAVE IN table1",
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES table2 (b) ON DELETE CASCADE",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES table3 (b, c) ON DELETE NO ACTION",
	}
	assert.ElementsMatch(t, e3, tablesAndFks)

	sequences := make(map[string]Sequence)
	sequences["s1"] = Sequence{
		Id:               "s1",
		Name:             "sequence1",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "0",
		SkipRangeMax:     "5",
		StartWithCounter: "7",
	}
	e4 := []string{
		"CREATE SEQUENCE sequence1 OPTIONS (sequence_kind='bit_reversed_positive', skip_range_min = 0, skip_range_max = 5, start_with_counter = 7) ",
	}
	sequencesOnly := GetDDL(Config{}, Schema{}, sequences, DatabaseOptions{})
	assert.ElementsMatch(t, e4, sequencesOnly)

	databaseOptions := DatabaseOptions{
		DbName: "test-db",
		DefaultTimezone: "America/New_York",
	}
	e5 := []string{
		"ALTER DATABASE `test-db` SET OPTIONS (default_time_zone = 'America/New_York')",
	}
	dbOptionsOnly := GetDDL(Config{}, Schema{}, make(map[string]Sequence), databaseOptions)
	assert.ElementsMatch(t, e5, dbOptionsOnly)
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
			ForeignKeys: []Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}, OnDelete: constants.FK_CASCADE, OnUpdate: constants.FK_NO_ACTION}},
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
			ForeignKeys: []Foreignkey{{Name: "fk2", ColIds: []string{"c4", "c5"}, ReferTableId: "t3", ReferColumnIds: []string{"c7", "c8"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION}},
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
			ParentTable: InterleavedParent{Id: "t1", OnDelete: constants.FK_CASCADE, InterleaveType: "IN PARENT"},
		},
		"t4": CreateTable{
			Name:   "table4",
			Id:     "t4",
			ColIds: []string{"c9", "c10", "c11"},
			ColDefs: map[string]ColumnDef{
				"c9":  {Name: "a", Id: "c9", T: Type{Name: Int64}},
				"c10": {Name: "b", Id: "c10", T: Type{Name: Int64}},
				"c11": {Name: "c", Id: "c11", T: Type{Name: Int64}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c9"}, {ColId: "c10"}},
			ParentTable: InterleavedParent{Id: "t1", OnDelete: constants.FK_NO_ACTION, InterleaveType: "IN"},
		},
	}
	tablesOnly := GetDDL(Config{Tables: true, ForeignKeys: false, SpDialect: constants.DIALECT_POSTGRESQL}, s, make(map[string]Sequence), DatabaseOptions{})
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
			") INTERLEAVE IN PARENT table1 ON DELETE CASCADE",
		"CREATE TABLE table4 (\n" +
			"	a INT8,\n" +
			"	b INT8,\n" +
			"	c INT8,\n" +
			"	PRIMARY KEY (a, b)\n" +
			") INTERLEAVE IN table1",
	}
	assert.ElementsMatch(t, e, tablesOnly)

	fksOnly := GetDDL(Config{Tables: false, ForeignKeys: true, SpDialect: constants.DIALECT_POSTGRESQL}, s, make(map[string]Sequence), DatabaseOptions{})
	e2 := []string{
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES table2 (b) ON DELETE CASCADE",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES table3 (b, c) ON DELETE NO ACTION",
	}
	assert.ElementsMatch(t, e2, fksOnly)

	tablesAndFks := GetDDL(Config{Tables: true, ForeignKeys: true, SpDialect: constants.DIALECT_POSTGRESQL}, s, make(map[string]Sequence), DatabaseOptions{})
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
			") INTERLEAVE IN PARENT table1 ON DELETE CASCADE",
		"CREATE TABLE table4 (\n" +
			"	a INT8,\n" +
			"	b INT8,\n" +
			"	c INT8,\n" +
			"	PRIMARY KEY (a, b)\n" +
			") INTERLEAVE IN table1",
		"ALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES table2 (b) ON DELETE CASCADE",
		"ALTER TABLE table2 ADD CONSTRAINT fk2 FOREIGN KEY (b, c) REFERENCES table3 (b, c) ON DELETE NO ACTION",
	}
	assert.ElementsMatch(t, e3, tablesAndFks)

	sequences := make(map[string]Sequence)
	sequences["s1"] = Sequence{
		Id:               "s1",
		Name:             "sequence1",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "0",
		SkipRangeMax:     "5",
		StartWithCounter: "7",
	}
	e4 := []string{
		"CREATE SEQUENCE sequence1 BIT_REVERSED_POSITIVE SKIP RANGE 0 5 START COUNTER WITH 7",
	}
	sequencesOnly := GetDDL(Config{SpDialect: constants.DIALECT_POSTGRESQL}, Schema{}, sequences, DatabaseOptions{})
	assert.ElementsMatch(t, e4, sequencesOnly)

	databaseOptions := DatabaseOptions{
		DbName: "test-db",
		DefaultTimezone: "America/New_York",
	}
	e5 := []string{
		"ALTER DATABASE \"test-db\" SET spanner.default_time_zone = 'America/New_York'",
	}
	dbOptionsOnly := GetDDL(Config{SpDialect: constants.DIALECT_POSTGRESQL}, Schema{}, make(map[string]Sequence), databaseOptions)
	assert.ElementsMatch(t, e5, dbOptionsOnly)
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
					Name:        "Table2",
					Id:          "table_id_2",
					ParentTable: InterleavedParent{Id: "table_id_1", OnDelete: constants.FK_CASCADE, InterleaveType: "IN PARENT"},
				},
				"table_id_3": CreateTable{
					Name:        "Table3",
					Id:          "table_id_3",
					ParentTable: InterleavedParent{Id: "table_id_2", OnDelete: constants.FK_NO_ACTION, InterleaveType: "IN PARENT"},
				},
				"table_id_4": CreateTable{
					Name:        "Table4",
					Id:          "table_id_4",
					ParentTable: InterleavedParent{Id: "table_id_2", OnDelete: constants.FK_NO_ACTION, InterleaveType: "IN"},
				},
			},
			expected: []string{"table_id_1", "table_id_2", "table_id_3", "table_id_4"},
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
					Name:        "Table1",
					Id:          "table_id_1",
					ParentTable: InterleavedParent{Id: "table_id_2", OnDelete: constants.FK_NO_ACTION, InterleaveType: "IN PARENT"},
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

func TestFormatCheckConstraints(t *testing.T) {
	tests := []struct {
		description string
		cks         []CheckConstraint
		expected    string
	}{
		{
			description: "Empty constraints list",
			cks:         []CheckConstraint{},
			expected:    "",
		},
		{
			description: "Single constraint",
			cks: []CheckConstraint{
				{Name: "ck1", Expr: "(id > 0)"},
			},
			expected: "\tCONSTRAINT ck1 CHECK (id > 0)\n",
		},
		{
			description: "Constraint without name",
			cks: []CheckConstraint{
				{Name: "", Expr: "(id > 0)"},
			},
			expected: "\tCHECK (id > 0)\n",
		},
		{
			description: "Multiple constraints",
			cks: []CheckConstraint{
				{Name: "ck1", Expr: "(id > 0)"},
				{Name: "ck2", Expr: "(name IS NOT NULL)"},
			},
			expected: "\tCONSTRAINT ck1 CHECK (id > 0),\n\tCONSTRAINT ck2 CHECK (name IS NOT NULL)\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			actual := FormatCheckConstraints(tc.cks, constants.DIALECT_GOOGLESQL)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestFormatCheckConstraintsPG(t *testing.T) {
	tests := []struct {
		description string
		cks         []CheckConstraint
		expected    string
	}{
		{
			description: "Empty constraints list",
			cks:         []CheckConstraint{},
			expected:    "",
		},
		{
			description: "Single constraint",
			cks: []CheckConstraint{
				{Name: "ck1", Expr: "(id > 0)"},
			},
			expected: "\tCONSTRAINT ck1 CHECK (id > 0),\n",
		},
		{
			description: "Constraint without name",
			cks: []CheckConstraint{
				{Name: "", Expr: "(id > 0)"},
			},
			expected: "\tCHECK (id > 0),\n",
		},
		{
			description: "Multiple constraints",
			cks: []CheckConstraint{
				{Name: "ck1", Expr: "(id > 0)"},
				{Name: "ck2", Expr: "(name IS NOT NULL)"},
			},
			expected: "\tCONSTRAINT ck1 CHECK (id > 0),\n\tCONSTRAINT ck2 CHECK (name IS NOT NULL),\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			actual := FormatCheckConstraints(tc.cks, constants.DIALECT_POSTGRESQL)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestPrintDatabaseOptions(t *testing.T) {
	tests := []struct {
		dbOptions DatabaseOptions
		expected  string
	}{
		{
			dbOptions: DatabaseOptions{},
			expected:  "",
		},
		{
			dbOptions: DatabaseOptions{
				DbName: "test-db",
				DefaultTimezone: "",
			},
			expected:  "",
		},
		{
			dbOptions: DatabaseOptions{
				DbName: "",
				DefaultTimezone: "America/New_York",
			},
			expected:  "ALTER DATABASE db SET OPTIONS (default_time_zone = 'America/New_York')",
		},
		{
			dbOptions: DatabaseOptions{
				DbName: "test-db",
				DefaultTimezone: "America/New_York",
			},
			expected:  "ALTER DATABASE `test-db` SET OPTIONS (default_time_zone = 'America/New_York')",
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.dbOptions.PrintDatabaseOptions())
	}
}

func TestPGPrintDatabaseOptions(t *testing.T) {
	tests := []struct {
		dbOptions DatabaseOptions
		expected  []string
	}{
		{
			dbOptions: DatabaseOptions{},
			expected:  nil,
		},
		{
			dbOptions: DatabaseOptions{
				DbName: "test-db",
				DefaultTimezone: "",
			},
			expected:  nil,
		},
		{
			dbOptions: DatabaseOptions{
				DbName: "",
				DefaultTimezone: "America/New_York",
			},
			expected:  []string{"ALTER DATABASE db SET spanner.default_time_zone = 'America/New_York'"},
		},
		{
			dbOptions: DatabaseOptions{
				DbName: "test-db",
				DefaultTimezone: "America/New_York",
			},
			expected:  []string{"ALTER DATABASE \"test-db\" SET spanner.default_time_zone = 'America/New_York'"},
		},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.dbOptions.PGPrintDatabaseOptions())
	}
}
