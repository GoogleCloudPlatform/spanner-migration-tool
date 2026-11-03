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

package mysql

import (
	"bufio"
	"context"
	"fmt"
	"math/big"
	"math/bits"
	"strings"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/mocks"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestProcessMySQLDump_Scalar(t *testing.T) {
	// First, test scalar types.
	scalarTests := []struct {
		ty       string
		expected ddl.Type
	}{
		{"varbinary(100)", ddl.Type{Name: ddl.Bytes, Len: int64(100)}},
		{"bigint", ddl.Type{Name: ddl.Int64}},
		{"bool", ddl.Type{Name: ddl.Bool}},
		{"boolean", ddl.Type{Name: ddl.Bool}},
		{"tinyint(1)", ddl.Type{Name: ddl.Bool}},
		{"tinyint(4)", ddl.Type{Name: ddl.Int64}},
		{"json", ddl.Type{Name: ddl.JSON}},
		{"blob", ddl.Type{Name: ddl.Bytes, Len: int64(65535)}},
		{"mediumblob", ddl.Type{Name: ddl.Bytes, Len: int64(10_485_760)}},
		{"tinyblob", ddl.Type{Name: ddl.Bytes, Len: int64(255)}},
		{"longblob", ddl.Type{Name: ddl.Bytes, Len: int64(10_485_760)}},
		{"char(42)", ddl.Type{Name: ddl.String, Len: int64(42)}},
		{"date", ddl.Type{Name: ddl.Date}},
		{"decimal(4,10)", ddl.Type{Name: ddl.Numeric}},
		{"numeric(4,10)", ddl.Type{Name: ddl.Numeric}},
		{"double(4,10)", ddl.Type{Name: ddl.Float64}},
		{"float(4,10)", ddl.Type{Name: ddl.Float32}},
		{"integer", ddl.Type{Name: ddl.Int64}},
		{"mediumint", ddl.Type{Name: ddl.Int64}},
		{"int", ddl.Type{Name: ddl.Int64}},
		{"bit(1)", ddl.Type{Name: ddl.Bool}},
		{"bit(5)", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		{"smallint", ddl.Type{Name: ddl.Int64}},
		{"bigint unsigned", ddl.Type{Name: ddl.Int64}},
		{"text", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"tinytext", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"mediumtext", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"longtext", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"enum('a','b')", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}},
		{"datetime", ddl.Type{Name: ddl.Timestamp}},
		{"binary", ddl.Type{Name: ddl.Bytes, Len: int64(1)}},
		{"varchar(42)", ddl.Type{Name: ddl.String, Len: int64(42)}},
	}
	for _, tc := range scalarTests {
		t.Run(tc.ty, func(t *testing.T) {
			conv, _ := runProcessMySQLDump(fmt.Sprintf("CREATE TABLE t (a %s);", tc.ty))
			tableId, _ := internal.GetTableIdFromSrcName(conv.SrcSchema, "t")
			columnId, _ := internal.GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, "a")
			noIssues(conv, t, "Scalar type: "+tc.ty)
			assert.Equal(t, tc.expected, conv.SpSchema[tableId].ColDefs[columnId].T, "Scalar type: "+tc.ty)
		})
	}
}

func TestProcessMySQLDump_SingleCol(t *testing.T) {
	// Test array types and not null.
	singleColTests := []struct {
		ty       string
		expected ddl.ColumnDef
	}{
		{"set('a','b','c')", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: ""}},
		{"text NOT NULL", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
	}

	for _, tc := range singleColTests {
		t.Run(tc.ty, func(t *testing.T) {
			v := fmt.Sprintf("CREATE TABLE t (a %s);", tc.ty)
			conv, _ := runProcessMySQLDump(v)
			noIssues(conv, t, "Not null: "+tc.ty)
			tableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "t")
			assert.Equal(t, nil, err)
			colId, err := internal.GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, "a")
			assert.Equal(t, nil, err)
			cd := conv.SpSchema[tableId].ColDefs[colId]
			cd.Comment = ""
			cd.Id = ""
			assert.Equal(t, tc.expected, cd, "Not null: "+tc.ty)
		})
	}
}

func TestProcessMySQLDump_MultiCol(t *testing.T) {
	// Next test more general cases: multi-column schemas and data conversion.
	multiColTests := []struct {
		name           string
		input          string
		expectedSchema map[string]ddl.CreateTable
		expectedData   []spannerData
		expectIssues   bool // True if we expect to encounter issues during conversion.
	}{
		{
			name: "Shopping cart",
			input: "CREATE TABLE cart (productid text, userid text, quantity bigint, price bigint unsigned);\n" +
				"ALTER TABLE cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": {
					Name:   "cart",
					ColIds: []string{"productid", "userid", "quantity", "price"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": {Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"userid":    {Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"quantity":  {Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
						"price":     {Name: "price", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "productid", Order: 1}, {ColId: "userid", Order: 2}}}},
		},
		{
			name:  "Shopping cart with no primary key",
			input: "CREATE TABLE cart (productid text, userid text NOT NULL, quantity bigint);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": {
					Name:   "cart",
					ColIds: []string{"productid", "userid", "quantity", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": {Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"userid":    {Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"quantity":  {Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
						"synth_id":  {Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Order: 1}}}},
		},
		{
			name:  "Create table with single primary key",
			input: "CREATE TABLE test (a text PRIMARY KEY, b text);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": {
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "a", Order: 1}}}},
		},
		{
			name:  "Create table with multiple primary keys",
			input: "CREATE TABLE test (a text, b text, n bigint, PRIMARY KEY (a, b) );\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": {
					Name:   "test",
					ColIds: []string{"a", "b", "n"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"n": {Name: "n", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "a", Order: 1}, {ColId: "b", Order: 2}}}},
		},
		{
			name: "Create table with single foreign key",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d SMALLINT, CONSTRAINT `fk_test` FOREIGN KEY (d) REFERENCES test (a) ON DELETE RESTRICT ON UPDATE CASCADE);",
			expectedSchema: map[string]ddl.CreateTable{
				"test": {
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "a", Order: 1}}},
				"test2": {
					Name:   "test2",
					ColIds: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        {Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        {Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Order: 1}},
					ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"d"}, ReferTableId: "test", ReferColumnIds: []string{"a"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION}}}},
		},
		{
			name: "Create table with multiple foreign key test constraint name",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test3 (e SMALLINT, f text, PRIMARY KEY (e) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d SMALLINT, CONSTRAINT `1_fk_test_2` FOREIGN KEY (d) REFERENCES test (a) ON DELETE RESTRICT ON UPDATE CASCADE );\n" +
				"ALTER TABLE test2 ADD CONSTRAINT __fk_test_2 FOREIGN KEY (c) REFERENCES test3(e);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}},
				"test3": ddl.CreateTable{
					Name:   "test3",
					ColIds: []string{"e", "f"},
					ColDefs: map[string]ddl.ColumnDef{
						"e": ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"f": ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "e", Order: 1}}},
				"test2": ddl.CreateTable{
					Name:   "test2",
					ColIds: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "A_fk_test_2", ColIds: []string{"d"}, ReferTableId: "test", ReferColumnIds: []string{"a"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION},
						ddl.Foreignkey{Name: "A_fk_test_2_3", ColIds: []string{"c"}, ReferTableId: "test3", ReferColumnIds: []string{"e"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION}}}},
		},
		{
			name: "Alter table add foreign key",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d SMALLINT );\n" +
				"ALTER TABLE test2 ADD FOREIGN KEY (d) REFERENCES test(a);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}},
				"test2": ddl.CreateTable{
					Name:   "test2",
					ColIds: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{ColIds: []string{"d"}, ReferTableId: "test", ReferColumnIds: []string{"a"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION}}}},
		},
		{
			name: "Alter table add constraint foreign key",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d SMALLINT );\n" +
				"ALTER TABLE test2 ADD CONSTRAINT fk_test FOREIGN KEY (d) REFERENCES test(a);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}},
				"test2": ddl.CreateTable{
					Name:   "test2",
					ColIds: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"d"}, ReferTableId: "test", ReferColumnIds: []string{"a"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION}}}},
		},
		{
			name: "Create table with multiple foreign keys",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d text, PRIMARY KEY (c) );\n" +
				"CREATE TABLE test3 (e SMALLINT, f SMALLINT, g text, CONSTRAINT `fk_test` FOREIGN KEY (e) REFERENCES test (a) ON DELETE CASCADE ON UPDATE CASCADE,CONSTRAINT `fk_test2` FOREIGN KEY (f) REFERENCES test2 (c) ON DELETE RESTRICT);",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}},
				"test2": ddl.CreateTable{
					Name:   "test2",
					ColIds: []string{"c", "d"},
					ColDefs: map[string]ddl.ColumnDef{
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"d": ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c", Order: 1}}},
				"test3": ddl.CreateTable{
					Name:   "test3",
					ColIds: []string{"e", "f", "g", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"e":        ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Int64}},
						"f":        ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Int64}},
						"g":        ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"e"}, ReferTableId: "test", ReferColumnIds: []string{"a"}, OnDelete: constants.FK_CASCADE, OnUpdate: constants.FK_NO_ACTION},
						ddl.Foreignkey{Name: "fk_test2", ColIds: []string{"f"}, ReferTableId: "test2", ReferColumnIds: []string{"c"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION}}}},
		},
		{
			name: "Create table with single foreign key multiple column",
			input: "CREATE TABLE test (a SMALLINT, b SMALLINT, c text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (e SMALLINT, f SMALLINT, g text, CONSTRAINT `fk_test` FOREIGN KEY (e,f) REFERENCES test (a,b) ON UPDATE CASCADE );",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}},

				"test2": ddl.CreateTable{
					Name:   "test2",
					ColIds: []string{"e", "f", "g", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"e":        ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Int64}},
						"f":        ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Int64}},
						"g":        ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"e", "f"}, ReferTableId: "test", ReferColumnIds: []string{"a", "b"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION}}}},
		},
		{
			name: "Create table with index keys",
			input: "CREATE TABLE test (" +
				"a smallint DEFAULT NULL," +
				"b text DEFAULT NULL," +
				"c text DEFAULT NULL," +
				"KEY custom_index (b, c)" +
				");\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					Indexes:     []ddl.CreateIndex{ddl.CreateIndex{Name: "custom_index", TableId: "test", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Desc: false, Order: 1}, ddl.IndexKey{ColId: "c", Desc: false, Order: 2}}}}}},
		},
		{
			name: "Create table with unique index keys",
			input: "CREATE TABLE test (" +
				"a smallint DEFAULT NULL," +
				"b text DEFAULT NULL," +
				"c text DEFAULT NULL," +
				"UNIQUE KEY custom_index (b, c)" +
				");\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Order: 1}, ddl.IndexKey{ColId: "c", Order: 2}},
					Indexes:     []ddl.CreateIndex{}}},
		},
		{
			name: "Create table with multiple index keys with different order",
			input: "CREATE TABLE test (" +
				"a smallint DEFAULT NULL," +
				"b text DEFAULT NULL," +
				"c text DEFAULT NULL," +
				"UNIQUE KEY custom_index (b, c)," +
				"KEY custom_index2 (c, a)" +
				");\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Order: 1}, ddl.IndexKey{ColId: "c", Order: 2}},
					Indexes:     []ddl.CreateIndex{ddl.CreateIndex{Name: "custom_index2", TableId: "test", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c", Desc: false, Order: 1}, ddl.IndexKey{ColId: "a", Desc: false, Order: 2}}}}}},
		},
		{
			name: "Alter table add index keys",
			input: "CREATE TABLE test (" +
				"a smallint DEFAULT NULL," +
				"b text DEFAULT NULL," +
				"c text DEFAULT NULL" +
				");\n" +
				"ALTER TABLE test ADD INDEX custom_index (b, c);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					Indexes:     []ddl.CreateIndex{ddl.CreateIndex{Name: "custom_index", TableId: "test", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Desc: false, Order: 1}, ddl.IndexKey{ColId: "c", Desc: false, Order: 2}}}}}},
		},
		{
			name: "Alter table add unique index keys",
			input: "CREATE TABLE test (" +
				"a smallint DEFAULT NULL," +
				"b text DEFAULT NULL," +
				"c text DEFAULT NULL" +
				");\n" +
				"ALTER TABLE test ADD UNIQUE INDEX custom_index (b, c);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Order: 1}, ddl.IndexKey{ColId: "c", Order: 2}},
					Indexes:     []ddl.CreateIndex{},
				}},
		},
		{
			name: "Alter table add index keys",
			input: "CREATE TABLE test (" +
				"a smallint DEFAULT NULL," +
				"b text DEFAULT NULL," +
				"c text DEFAULT NULL" +
				");\n" +
				"ALTER TABLE test ADD CONSTRAINT custom_index UNIQUE (b, c);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Order: 1}, ddl.IndexKey{ColId: "c", Order: 2}},
					Indexes:     []ddl.CreateIndex{},
				}},
		},
		{
			name: "Create index statement",
			input: "CREATE TABLE test (" +
				"a smallint DEFAULT NULL," +
				"b text DEFAULT NULL," +
				"c text DEFAULT NULL" +
				");\n" +
				"CREATE INDEX custom_index ON test (b, c);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					Indexes:     []ddl.CreateIndex{ddl.CreateIndex{Name: "custom_index", TableId: "test", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Desc: false, Order: 1}, ddl.IndexKey{ColId: "c", Desc: false, Order: 2}}}}}},
		},
		{
			name: "Create unique index statement",
			input: "CREATE TABLE test (" +
				"a smallint DEFAULT NULL," +
				"b text DEFAULT NULL," +
				"c text DEFAULT NULL" +
				");\n" +
				"CREATE UNIQUE INDEX custom_index ON test (b, c);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Order: 1}, ddl.IndexKey{ColId: "c", Order: 2}},
					Indexes:     []ddl.CreateIndex{},
				}},
		},
		{
			name:  "Create table with mysql schema",
			input: "CREATE TABLE myschema.test (a text PRIMARY KEY, b text);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"myschema_test": ddl.CreateTable{
					Name:   "myschema_test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}}},
		},
		{
			name: "Create table with function, trigger and procedure",
			input: `
		DELIMITER ;;
		CREATE PROCEDURE test_procedure( x INT )
		    DETERMINISTIC
		BEGIN
		  SELECT concat(x, ' is a nice number');
		END ;;
		DELIMITER ;

		DELIMITER ;;
		CREATE FUNCTION test_function( x INT ) RETURNS int(11)
		    DETERMINISTIC
		BEGIN
		  RETURN x + 42;
		END ;;
		DELIMITER ;

		DELIMITER ;;
		/*!50003 CREATE TRIGGER test_trigger BEFORE INSERT ON MyTable FOR EACH ROW If NEW.id < 0 THEN SET NEW.id = -NEW.id; END IF */;;
		DELIMITER ;

		CREATE TABLE test (a text PRIMARY KEY, b text);`,
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}}},
			expectIssues: true, // conv.Stats.Reparsed != 0
		},
		{
			name: "ALTER TABLE SET NOT NULL",
			input: "CREATE TABLE test (a text PRIMARY KEY, b text);\n" +
				"ALTER TABLE test MODIFY b text NOT NULL;\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}}},
		},
		{
			name: "Multiple statements on one line",
			input: "CREATE TABLE t1 (a text, b text); CREATE TABLE t2 (c text);\n" +
				"ALTER TABLE t1 ADD CONSTRAINT t1_pkey PRIMARY KEY (a);\n" +
				"ALTER TABLE t2 ADD CONSTRAINT t2_pkey PRIMARY KEY (c);",
			expectedSchema: map[string]ddl.CreateTable{
				"t1": ddl.CreateTable{
					Name:   "t1",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}},
				"t2": ddl.CreateTable{
					Name:   "t2",
					ColIds: []string{"c"},
					ColDefs: map[string]ddl.ColumnDef{
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c", Order: 1}}}},
		},
		{
			name: "INSERT statement",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				"INSERT INTO test (a, b, n) VALUES ('a1','b1',42),\n" +
				"('a22','b99', 6);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "INSERT INTO with renamed table/cols",
			input: "CREATE TABLE _test (_a text, b text, n bigint);\n" +
				"ALTER TABLE _test ADD CONSTRAINT test_pkey PRIMARY KEY (_a, b);\n" +
				"INSERT INTO _test (_a, b, n) VALUES ('a1','b1',42),\n" +
				"('a22','b99', 6);",
			expectedData: []spannerData{
				spannerData{table: "Atest", cols: []string{"Aa", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "Atest", cols: []string{"Aa", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "INSERT INTO with CRLF",
			input: "CREATE TABLE test (a text, b text, n bigint);\r\n" +
				"INSERT INTO test (a, b, n) VALUES \r\n ('a1','b1',42),\r\n" +
				"('a22','b99', 6);\r\n" +
				"ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\r\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "INSERT INTO with spaces",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				"INSERT INTO test (a, b, n) VALUES ('a1 ',' b1',42),\n" +
				"('a22','b 99 ', 6);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1 ", " b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b 99 ", int64(6)}}},
		},
		{
			name: "INSERT INTO with no primary key",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"INSERT INTO test (a, b, n) VALUES\n" +
				"('a1','b1',42),\n" +
				"('a22','b99', 6),\n" +
				"('a33','b',9),\n" +
				"('a3','b',7);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a1", "b1", int64(42), fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a22", "b99", int64(6), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a33", "b", int64(9), fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a3", "b", int64(7), fmt.Sprintf("%d", bitReverse(3))}}},
		},
		{
			name: "INSERT INTO with empty cols",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"INSERT INTO test (a, b, n) VALUES\n" +
				"(NULL,'b1',42),\n" +
				"('a22',NULL,6),\n" +
				"('a33','b',NULL),\n" +
				"('a3','b',7);\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"b", "n", "synth_id"}, vals: []interface{}{"b1", int64(42), fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"a", "n", "synth_id"}, vals: []interface{}{"a22", int64(6), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"a", "b", "synth_id"}, vals: []interface{}{"a33", "b", fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a3", "b", int64(7), fmt.Sprintf("%d", bitReverse(3))}}},
		},
		{
			name: "INSERT",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);" +
				"ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				"INSERT INTO test (a, b, n) VALUES ('a42', 'b6', 2);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a42", "b6", int64(2)}}},
		},
		{
			name: "INSERT with no primary key",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"INSERT INTO test (a, b, n) VALUES ('a42', 'b6', 2);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a42", "b6", int64(2), fmt.Sprintf("%d", bitReverse(0))}}},
		},
		{
			name: "INSERT with spaces",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				"INSERT INTO test (a, b, n) VALUES (' a42 ', '\nb6\n', 2);\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{" a42 ", "\nb6\n", int64(2)}}},
		},
		{
			name: "Statements with embedded semicolons",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				`INSERT INTO test (a, b, n) VALUES ('a;\n2', 'b;6\n', 2);` + "\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a;\n2", "b;6\n", int64(2)}}},
		},
		{
			name: "Tables and columns with illegal characters",
			input: "CREATE TABLE `te.s t` (`a?^` text PRIMARY KEY, `b.b` text, `n*n` bigint);\n" +
				"INSERT INTO `te.s t` (`a?^`, `b.b`, `n*n`) VALUES ('a', 'b', 2);",
			expectedData: []spannerData{
				spannerData{table: "te_s_t", cols: []string{"a__", "b_b", "n_n"}, vals: []interface{}{"a", "b", int64(2)}}},
			expectedSchema: map[string]ddl.CreateTable{
				"te_s_t": ddl.CreateTable{
					Name:   "te_s_t",
					ColIds: []string{"a__", "b_b", "n_n"},
					ColDefs: map[string]ddl.ColumnDef{
						"a__": ddl.ColumnDef{Name: "a__", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b_b": ddl.ColumnDef{Name: "b_b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"n_n": ddl.ColumnDef{Name: "n_n", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a__", Order: 1}}}},
		},
		{
			name: "Tables and columns with illegal characters: CREATE-ALTER",
			input: "CREATE TABLE `te.s t` (`a?^` text, `b.b` text, `n*n` bigint);\n" +
				"ALTER TABLE `te.s t` ADD CONSTRAINT test_pkey PRIMARY KEY (`a?^`);\n" +
				"INSERT INTO `te.s t` (`a?^`, `b.b`, `n*n`) VALUES ('a', 'b', 2);",
			expectedData: []spannerData{
				spannerData{table: "te_s_t", cols: []string{"a__", "b_b", "n_n"}, vals: []interface{}{"a", "b", int64(2)}}},
			expectedSchema: map[string]ddl.CreateTable{
				"te_s_t": ddl.CreateTable{
					Name:   "te_s_t",
					ColIds: []string{"a__", "b_b", "n_n"},
					ColDefs: map[string]ddl.ColumnDef{
						"a__": ddl.ColumnDef{Name: "a__", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b_b": ddl.ColumnDef{Name: "b_b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"n_n": ddl.ColumnDef{Name: "n_n", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a__", Order: 1}}}},
		},
		// The "Data conversion: ..." cases check data conversion for each type.
		{
			name: "Data conversion: bool, bigint, char, blob",
			input: `
		CREATE TABLE test (id integer PRIMARY KEY, a bool, b bigint, c char(1),d blob);
		INSERT INTO test (id, a, b, c, d) VALUES (1, 1, 42, 'x',_binary '` + string([]byte{137, 80}) + `');`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(1), true, int64(42), "x", []byte{0x89, 0x50}}}},
		},
		{
			name: "Data conversion: date, float, decimal, mediumint",
			input: `
		CREATE TABLE test (id integer PRIMARY KEY, a date, b float, c decimal(3,5));
		INSERT INTO test (id, a, b, c) VALUES (1,'2019-10-29',4.444,5.44444);
		`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c"}, vals: []interface{}{int64(1), getDate("2019-10-29"), float32(4.444), big.NewRat(136111, 25000)}}},
		},
		{
			name: "Data conversion: smallint, mediumint, bigint, double",
			input: `
		CREATE TABLE test (id integer PRIMARY KEY, a smallint, b mediumint, c bigint, d double);
		INSERT INTO test (id, a, b, c, d) VALUES (1, 88, 44, 22, 444.9876);
		`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(1), int64(88), int64(44), int64(22), float64(444.9876)}}},
		},
		{
			name: "Data conversion: negative values for smallint, mediumint, bigint, double",
			input: `
		CREATE TABLE test (id integer PRIMARY KEY, a smallint, b mediumint, c bigint, d double);
		INSERT INTO test (id, a, b, c, d) VALUES (-1, -88, -44, -22, -444.9876);
		`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(-1), int64(-88), int64(-44), int64(-22), float64(-444.9876)}}},
		},
		{
			name: "create table check constraint",
			input: "CREATE TABLE `Reviews` (" +
				"`review_id` int NOT NULL AUTO_INCREMENT, " +
				"`rating` tinyint unsigned DEFAULT NULL, " +
				"`restaurant_id` bigint unsigned DEFAULT NULL, " +
				"PRIMARY KEY (`review_id`), " +
				"UNIQUE KEY `restaurant_id` (`restaurant_id`), " +
				"CONSTRAINT `reviews_chk_1` CHECK (((`rating` >= 1) and (`rating` <= 5)))" +
				");",
			expectedSchema: map[string]ddl.CreateTable{
				"Reviews": {
					Name:   "Reviews",
					ColIds: []string{"review_id", "rating"},
					ColDefs: map[string]ddl.ColumnDef{
						"review_id":     {Name: "review_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true, AutoGen: ddl.AutoGenCol{Name: constants.IDENTITY, GenerationType: constants.IDENTITY}},
						"rating":        {Name: "rating", T: ddl.Type{Name: ddl.Int64}},
						"restaurant_id": {Name: "restaurant_id", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "review_id", Order: 1}},
					Indexes: []ddl.CreateIndex{
						{
							Name:   "restaurant_id",
							Unique: true,
							Keys:   []ddl.IndexKey{{ColId: "restaurant_id", Order: 1}},
						},
					},
					CheckConstraints: []ddl.CheckConstraint{
						{
							Name:   "reviews_chk_1",
							Expr:   "((rating>=1) AND (rating<=5))",
							ExprId: "e2",
							Id:     "cc3",
						},
					},
				},
			},
			expectIssues: false,
		},
		// test with different timezone
		{
			name: "Data conversion:  text, timestamp, datetime, varchar",
			input: `
		SET TIME_ZONE='+02:30';
		CREATE TABLE test (id integer PRIMARY KEY, a text, b timestamp, c datetime, d varchar(15));
		INSERT INTO test (id, a, b, c, d) VALUES (1, 'my text', '2019-10-29 05:30:00', '2019-10-29 05:30:00', 'my varchar');
		`,
			expectedData: []spannerData{
				{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(1), "my text", getTime(t, "2019-10-29T05:30:00+02:30"), getTimeWithoutTimezone(t, "2019-10-29 05:30:00"), "my varchar"}}},
		},
	}
	for _, tc := range multiColTests {
		t.Run(tc.name, func(t *testing.T) {
			conv, rows := runProcessMySQLDump(tc.input)
			if !tc.expectIssues {
				noIssues(conv, t, tc.name)
			}
			if tc.expectedSchema != nil {
				internal.AssertSpSchema(conv, t, tc.expectedSchema, stripSchemaComments(conv.SpSchema))
			}
			if tc.expectedData != nil {
				assert.Equal(t, tc.expectedData, rows, tc.name+": Data rows did not match")
			}
		})
	}
}

func TestProcessMySQLDump_TimeZone(t *testing.T) {
	// Test set timezone statement.
	conv, _ := runProcessMySQLDump("SET TIME_ZONE='+02:30';")
	assert.Equal(t, conv.TimezoneOffset, "+02:30", "Set timezone")
}

func TestProcessMySQLDump_DataError(t *testing.T) {
	// Finally test data conversion errors.
	dataErrorTests := []struct {
		name         string
		input        string
		expectedData []spannerData
	}{
		{
			// Test bad data for each scalar type (except text, which accepts all values) and an array type.
			name: "Data conversion errors",
			input: "CREATE TABLE test (a int, b double, c bool, d date, e blob, f set('42','6'), g bit, h float);\n" +
				`INSERT INTO test (a, b, c, d, e, f, g, h) VALUES (7,42.1,1,'2019-10-29',_binary '` + string([]byte{137, 80}) + `','42,6', 0, 3.14);` + // Baseline (good)
				"INSERT INTO test (a, b, c, d, e, f, g) VALUES (7,NULL,NULL,NULL,NULL,NULL, NULL);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (7.1,NULL,NULL,NULL,NULL,NULL);\n" + // Error
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,42.1,NULL,NULL,NULL,NULL);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,'42-1',NULL,NULL,NULL,NULL);\n" + // Error
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,true,NULL,NULL,NULL);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,'truefalse',NULL,NULL,NULL);\n" + // Error
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,'2019-10-29',NULL,NULL);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,'2019-10-42',NULL,NULL);\n" + // Error
				`INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,NULL,_binary '` + string([]byte{137, 80}) + `',NULL);` + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,NULL,NULL,'42,6');\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,NULL,NULL,42,6);\n" + // Error
				"INSERT INTO test (a, b, c, d, e, f, h) VALUES (NULL,NULL,NULL,NULL,NULL,NULL, 3.14);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f, h) VALUES (NULL,NULL,NULL,NULL,NULL,NULL, '3-14');\n", // Error
			expectedData: []spannerData{
				spannerData{
					table: "test", cols: []string{"a", "b", "c", "d", "e", "f", "g", "h", "synth_id"},
					vals: []interface{}{int64(7), float64(42.1), true,
						getDate("2019-10-29"), []byte{0x89, 0x50},
						"42,6", false, float32(3.14),
						fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"a", "synth_id"}, vals: []interface{}{int64(7), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"b", "synth_id"}, vals: []interface{}{float64(42.1), fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"c", "synth_id"}, vals: []interface{}{true, fmt.Sprintf("%d", bitReverse(3))}},
				spannerData{table: "test", cols: []string{"d", "synth_id"}, vals: []interface{}{getDate("2019-10-29"), fmt.Sprintf("%d", bitReverse(4))}},
				spannerData{table: "test", cols: []string{"e", "synth_id"}, vals: []interface{}{[]byte{0x89, 0x50}, fmt.Sprintf("%d", bitReverse(5))}},
				spannerData{table: "test", cols: []string{"f", "synth_id"},
					vals: []interface{}{"42,6", fmt.Sprintf("%d", bitReverse(6))}},
				spannerData{table: "test", cols: []string{"h", "synth_id"}, vals: []interface{}{float32(3.14), fmt.Sprintf("%d", bitReverse(7))}},
			},
		},
	}
	for _, tc := range dataErrorTests {
		conv, rows := runProcessMySQLDump(tc.input)
		assert.Equal(t, tc.expectedData, rows, tc.name+": Data rows did not match")
		assert.Equal(t, conv.BadRows(), int64(6), tc.name+": Error count did not match")
	}
}

// The following test Conv API calls based on data generated by ProcessMySQLDump.
func TestProcessMySQLDump_GetDDL(t *testing.T) {
	conv, _ := runProcessMySQLDump("CREATE TABLE cart (productid text, userid text, quantity bigint);\n" +
		"ALTER TABLE cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);")
	expected :=
		"CREATE TABLE cart (\n" +
			"	productid STRING(MAX) NOT NULL ,\n" +
			"	userid STRING(MAX) NOT NULL ,\n" +
			"	quantity INT64,\n" +
			") PRIMARY KEY (productid, userid)"
	c := ddl.Config{Tables: true}
	assert.Equal(t, expected, strings.Join(ddl.GetDDL(c, conv.SpSchema, conv.SpSequences, conv.DatabaseOptions), " "))
}

func TestProcessMySQLDump_Rows(t *testing.T) {
	conv, _ := runProcessMySQLDump("CREATE TABLE cart (a text, n bigint);\n" +
		"INSERT INTO cart (a, n) VALUES ('a42', 2);")
	assert.Equal(t, int64(1), conv.Rows())
}

func TestProcessMySQLDump_BadRows(t *testing.T) {
	conv, _ := runProcessMySQLDump("CREATE TABLE cart (a text, n bigint);\n" +
		"INSERT INTO cart (a, n) VALUES ('a42', 'not_a_number');")
	assert.Equal(t, int64(1), conv.BadRows())
}

func TestProcessMySQLDump_GetBadRows(t *testing.T) {
	conv, _ := runProcessMySQLDump("CREATE TABLE cart (a text, n bigint);\n" +
		"INSERT INTO cart (a, n) VALUES ('a42', 'not_a_number');")
	assert.Equal(t, 1, len(conv.SampleBadRows(100)))
}

func TestProcessMySQLDump_AddPrimaryKeys(t *testing.T) {
	cases := []struct {
		name           string
		input          string
		expectedSchema map[string]ddl.CreateTable
	}{
		{
			name:  "Shopping cart",
			input: "CREATE TABLE cart (productid text, userid text, quantity bigint);",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": ddl.CreateTable{
					Name:   "cart",
					ColIds: []string{"productid", "userid", "quantity", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
						"synth_id":  ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}}}},
		},
		{
			name:  "synth_id clash",
			input: "CREATE TABLE test (synth_id text, synth_id0 text, synth_id1 bigint);",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"synth_id", "synth_id0", "synth_id1", "synth_id2"},
					ColDefs: map[string]ddl.ColumnDef{
						"synth_id":  ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id0": ddl.ColumnDef{Name: "synth_id0", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id1": ddl.ColumnDef{Name: "synth_id1", T: ddl.Type{Name: ddl.Int64}},
						"synth_id2": ddl.ColumnDef{Name: "synth_id2", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id2", Order: 1}}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conv, _ := runProcessMySQLDump(tc.input)
			conv.AddPrimaryKeys()
			if tc.expectedSchema != nil {
				internal.AssertSpSchema(conv, t, tc.expectedSchema, stripSchemaComments(conv.SpSchema))
			}
		})
	}
}

func runProcessMySQLDump(s string) (*internal.Conv, []spannerData) {
	conv := internal.MakeConv()
	conv.SetLocation(time.UTC)
	conv.SetSchemaMode()
	mockAccessor := new(mocks.MockExpressionVerificationAccessor)
	ctx := context.Background()
	mockAccessor.On("VerifyExpressions", ctx, mock.Anything).Return(internal.VerifyExpressionsOutput{
		ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
			{Result: true, Err: nil, ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 0)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1", "colId": "c1", "checkConstraintName": "check1"}, ExpressionId: "expr1"}},
		},
	})
	mysqlDbDump := DbDumpImpl{}
	common.ProcessDbDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil), mysqlDbDump, &expressions_api.MockDDLVerifier{}, mockAccessor)
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(func(table string, cols []string, vals []interface{}) {
		rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
	})
	common.ProcessDbDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil), mysqlDbDump, &expressions_api.MockDDLVerifier{}, mockAccessor)
	return conv, rows
}

// noIssues verifies that conversion was issue-free by checking that conv
// contains no unexpected conditions, statement errors, etc. Note that
// many tests are issue-free, but several explicitly test handling of
// various issues (so don't call nonIssue for them!).
func noIssues(conv *internal.Conv, t *testing.T, name string) {
	assert.Zero(t, conv.Unexpecteds(), fmt.Sprintf("'%s' generated unexpected conditions: %v", name, conv.Stats.Unexpected))
	for s, stat := range conv.Stats.Statement {
		assert.Zero(t, stat.Error, fmt.Sprintf("'%s' generated %d errors for %s statements", name, stat.Error, s))
		if stat.Error > 0 {
		}
	}
	assert.Zero(t, len(conv.Stats.BadRows), fmt.Sprintf("'%s' generated bad rows: %v", name, conv.Stats.BadRows))
	assert.Zero(t, conv.Stats.Reparsed, fmt.Sprintf("'%s' generated %d reparse events", name, conv.Stats.Reparsed))
}

// stripSchemaComments returns a schema with all comments removed.
// We mostly ignore schema comments in testing since schema comments
// are often changed and are not a core part of conversion functionality.
func stripSchemaComments(spSchema map[string]ddl.CreateTable) map[string]ddl.CreateTable {
	for t, ct := range spSchema {
		for c, cd := range ct.ColDefs {
			cd.Comment = ""
			ct.ColDefs[c] = cd
		}
		ct.Comment = ""
		spSchema[t] = ct
	}
	return spSchema
}

func bitReverse(i int64) int64 {
	return int64(bits.Reverse64(uint64(i)))
}

func mustRat(t *testing.T, s string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		t.Fatalf("failed to parse rational %q", s)
	}
	return r
}

func TestParseMySQLDecimal_EdgeCases(t *testing.T) {
	t.Run("valid decimals within 9 words", func(t *testing.T) {
		cases := []struct {
			in   string
			want string
		}{
			{"0", "0"},
			{"0.00", "0.00"},
			{"-0.00", "0.00"},
			{".5", "0.5"},
			{"12345.67890", "12345.67890"},
			{strings.Repeat("9", 35) + "." + strings.Repeat("9", 30), strings.Repeat("9", 35) + "." + strings.Repeat("9", 30)},
			{strings.Repeat("9", 81), strings.Repeat("9", 81)},
		}
		for _, tc := range cases {
			dec, err := parseMySQLDecimal(tc.in)
			assert.NoError(t, err, tc.in)
			assert.Equal(t, tc.want, fmt.Sprintf("%v", dec), tc.in)
		}
	})

	t.Run("fractional precision exceeding 9 words truncates safely without panic", func(t *testing.T) {
		// 1 integer digit (1 word) + 85 fractional digits (10 words) -> truncated to (9-1)*9 = 72 fractional digits.
		in := "1." + strings.Repeat("2", 85)
		dec, err := parseMySQLDecimal(in)
		assert.NoError(t, err)
		assert.Equal(t, "1."+strings.Repeat("2", 72), fmt.Sprintf("%v", dec))

		// 40 integer digits (5 words) + 40 fractional digits (5 words) -> truncated to (9-5)*9 = 36 fractional digits.
		in80 := strings.Repeat("3", 40) + "." + strings.Repeat("4", 40)
		dec80, err := parseMySQLDecimal(in80)
		assert.NoError(t, err)
		assert.Equal(t, strings.Repeat("3", 40)+"."+strings.Repeat("4", 36), fmt.Sprintf("%v", dec80))

		// 81 integer digits (9 words) + 5 fractional digits (1 word) -> truncated to 0 fractional digits.
		in81Frac := strings.Repeat("7", 81) + ".12345"
		dec81Frac, err := parseMySQLDecimal(in81Frac)
		assert.NoError(t, err)
		assert.Equal(t, strings.Repeat("7", 81), fmt.Sprintf("%v", dec81Frac))
	})

	t.Run("integer precision exceeding 81 digits returns error without panic", func(t *testing.T) {
		in := strings.Repeat("9", 82)
		dec, err := parseMySQLDecimal(in)
		assert.Error(t, err)
		assert.NotNil(t, dec)
	})

	t.Run("malformed inputs return error without panic", func(t *testing.T) {
		for _, bad := range []string{"", "   ", "-", "+", "."} {
			dec, err := parseMySQLDecimal(bad)
			assert.Error(t, err, "input=%q", bad)
			assert.NotNil(t, dec, "input=%q", bad)
		}
	})
}

func TestProcessMySQLDump_DecimalAndNumericEdgeCases(t *testing.T) {
	pos38 := "12345678901234567890123456789.123456789"
	neg38 := "-12345678901234567890123456789.123456789"
	pos65 := strings.Repeat("1", 35) + "." + strings.Repeat("2", 30)
	neg65 := "-" + pos65
	over81Frac := "1." + strings.Repeat("5", 85)
	over81FracTruncated := "1." + strings.Repeat("5", 72)

	sqlDump := fmt.Sprintf(`
CREATE TABLE dec_edge (
	id BIGINT PRIMARY KEY,
	d_zero DECIMAL(10, 2),
	d_neg_zero DECIMAL(10, 2),
	d_hp DECIMAL(38, 9),
	d_max DECIMAL(65, 30),
	f_sci DOUBLE
);
INSERT INTO dec_edge (id, d_zero, d_neg_zero, d_hp, d_max, f_sci) VALUES
	(1, 0.00, -0.00, %s, %s, 1.25e4),
	(-9223372036854775808, 0.0000, -.5, %s, %s, -1.25e4),
	(2, .25, -0.25, 0.000000001, %s, 0.0);
`, pos38, pos65, neg38, neg65, over81Frac)

	conv, rows := runProcessMySQLDump(sqlDump)
	noIssues(conv, t, "Decimal and numeric edge cases")

	expected := []spannerData{
		{
			table: "dec_edge",
			cols:  []string{"id", "d_zero", "d_neg_zero", "d_hp", "d_max", "f_sci"},
			vals: []interface{}{
				int64(1),
				mustRat(t, "0"),
				mustRat(t, "0"),
				mustRat(t, pos38),
				mustRat(t, pos65),
				float64(12500),
			},
		},
		{
			table: "dec_edge",
			cols:  []string{"id", "d_zero", "d_neg_zero", "d_hp", "d_max", "f_sci"},
			vals: []interface{}{
				int64(-9223372036854775808),
				mustRat(t, "0"),
				mustRat(t, "-0.5"),
				mustRat(t, neg38),
				mustRat(t, neg65),
				float64(-12500),
			},
		},
		{
			table: "dec_edge",
			cols:  []string{"id", "d_zero", "d_neg_zero", "d_hp", "d_max", "f_sci"},
			vals: []interface{}{
				int64(2),
				mustRat(t, "0.25"),
				mustRat(t, "-0.25"),
				mustRat(t, "0.000000001"),
				mustRat(t, over81FracTruncated),
				float64(0),
			},
		},
	}
	assert.Equal(t, expected, rows)
}

func TestProcessMySQLDump_Over81DigitIntegerLiteralDoesNotPanic(t *testing.T) {
	hugeInt := strings.Repeat("9", 85)
	sqlDump := fmt.Sprintf("CREATE TABLE t_overflow (id INT PRIMARY KEY, d DECIMAL(65, 0));\n"+
		"INSERT INTO t_overflow (id, d) VALUES (1, 42),(2, %s),(3, 99);\n", hugeInt)

	assert.NotPanics(t, func() {
		_, rows := runProcessMySQLDump(sqlDump)
		expected := []spannerData{
			{table: "t_overflow", cols: []string{"id", "d"}, vals: []interface{}{int64(1), mustRat(t, "42")}},
			{table: "t_overflow", cols: []string{"id", "d"}, vals: []interface{}{int64(3), mustRat(t, "99")}},
		}
		assert.Equal(t, expected, rows)
	})
}

func TestProcessMySQLDump_DefaultValuesAndEscapedExpressions(t *testing.T) {
	sqlDump := "CREATE TABLE expr_edge (\n" +
		"  id INT NOT NULL PRIMARY KEY,\n" +
		"  c_null VARCHAR(20) DEFAULT NULL,\n" +
		"  c_str VARCHAR(20) DEFAULT 'hello',\n" +
		"  c_dec DECIMAL(10, 2) DEFAULT 0.00,\n" +
		"  c_int INT DEFAULT 42,\n" +
		"  path VARCHAR(255),\n" +
		"  label VARCHAR(255),\n" +
		"  gen_virtual VARCHAR(512) GENERATED ALWAYS AS (CONCAT(`path`, 'C:\\\\Users\\\\docs', 'O''Brien')) VIRTUAL,\n" +
		"  gen_stored DECIMAL(12, 2) GENERATED ALWAYS AS ((`c_dec` + 1.25)) STORED,\n" +
		"  CONSTRAINT `chk_escaped_backslash` CHECK ((`path` LIKE 'C:\\\\Users\\\\%')),\n" +
		"  CONSTRAINT `chk_escaped_quotes` CHECK ((`label` <> _utf8mb4'it''s_invalid')),\n" +
		"  CONSTRAINT `chk_decimal` CHECK (((`c_dec` >= 0.00) AND (`c_dec` <= 9999.99)))\n" +
		");\n"

	conv, _ := runProcessMySQLDump(sqlDump)
	tableID, err := internal.GetTableIdFromSrcName(conv.SrcSchema, "expr_edge")
	assert.NoError(t, err)
	srcTbl := conv.SrcSchema[tableID]

	cNullID, err := internal.GetColIdFromSrcName(srcTbl.ColDefs, "c_null")
	assert.NoError(t, err)
	assert.False(t, srcTbl.ColDefs[cNullID].Ignored.Default, "DEFAULT NULL should not mark Ignored.Default")

	for _, colName := range []string{"c_str", "c_dec", "c_int"} {
		colID, err := internal.GetColIdFromSrcName(srcTbl.ColDefs, colName)
		assert.NoError(t, err, colName)
		assert.True(t, srcTbl.ColDefs[colID].Ignored.Default, "%s with explicit DEFAULT should mark Ignored.Default", colName)
	}

	genVirtID, err := internal.GetColIdFromSrcName(srcTbl.ColDefs, "gen_virtual")
	assert.NoError(t, err)
	assert.True(t, srcTbl.ColDefs[genVirtID].GeneratedColumn.IsPresent)
	assert.Equal(t, ddl.GeneratedColVirtual, srcTbl.ColDefs[genVirtID].GeneratedColumn.Type)
	assert.Equal(t, "CONCAT(path, _UTF8MB4'C:\\Users\\docs', _UTF8MB4'O''Brien')", srcTbl.ColDefs[genVirtID].GeneratedColumn.Value.Statement)

	genStoredID, err := internal.GetColIdFromSrcName(srcTbl.ColDefs, "gen_stored")
	assert.NoError(t, err)
	assert.True(t, srcTbl.ColDefs[genStoredID].GeneratedColumn.IsPresent)
	assert.Equal(t, ddl.GeneratedColStored, srcTbl.ColDefs[genStoredID].GeneratedColumn.Type)
	assert.Equal(t, "(c_dec+1.25)", srcTbl.ColDefs[genStoredID].GeneratedColumn.Value.Statement)

	gotChecks := map[string]string{}
	for _, cc := range srcTbl.CheckConstraints {
		gotChecks[cc.Name] = cc.Expr
	}
	assert.Equal(t, "(path LIKE 'C:\\\\Users\\\\%')", gotChecks["chk_escaped_backslash"])
	assert.Equal(t, "(label!='it''s_invalid')", gotChecks["chk_escaped_quotes"])
	assert.Equal(t, "((c_dec>=0.00) AND (c_dec<=9999.99))", gotChecks["chk_decimal"])
}

func TestExpressionToString_ValueExprRestoreParity(t *testing.T) {
	stmtSQL := "CREATE TABLE t (id INT, CONSTRAINT c1 CHECK (" +
		"a = 'C:\\\\dir\\\\file' AND b = 'O''Reilly' AND d = 0.00 AND e IS NOT NULL AND f = TRUE AND g = b'101' AND h = x'ff'" +
		"));"
	stmts, _, err := parser.New().Parse(stmtSQL, "", "")
	assert.NoError(t, err)
	createStmt := stmts[0].(*ast.CreateTableStmt)
	restored := expressionToString(createStmt.Constraints[0].Expr)
	assert.Equal(t,
		"a=_UTF8MB4'C:\\\\dir\\\\file' AND b=_UTF8MB4'O''Reilly' AND d=0.00 AND e IS NOT NULL AND f=TRUE AND g=b'101' AND h=x'ff'",
		restored,
	)
}
