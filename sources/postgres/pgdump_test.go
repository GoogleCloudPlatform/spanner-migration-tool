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
	"bufio"
	"fmt"
	"math/big"
	"math/bits"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"github.com/stretchr/testify/assert"
)

type spannerData struct {
	table string
	cols  []string
	vals  []interface{}
}

func TestProcessPgDump(t *testing.T) {
	// First, test scalar types.
	scalarTests := []struct {
		ty       string
		expected ddl.Type
	}{
		{"bigint", ddl.Type{Name: ddl.Int64}},
		{"bool", ddl.Type{Name: ddl.Bool}},
		{"boolean", ddl.Type{Name: ddl.Bool}},
		{"bytea", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		{"char(42)", ddl.Type{Name: ddl.String, Len: int64(42)}},
		{"date", ddl.Type{Name: ddl.Date}},
		{"decimal", ddl.Type{Name: ddl.Numeric}}, // pg parser maps this to numeric.
		{"double precision", ddl.Type{Name: ddl.Float64}},
		{"float8", ddl.Type{Name: ddl.Float64}},
		{"float4", ddl.Type{Name: ddl.Float64}},
		{"integer", ddl.Type{Name: ddl.Int64}},
		{"numeric", ddl.Type{Name: ddl.Numeric}},
		{"numeric(4)", ddl.Type{Name: ddl.Numeric}},
		{"numeric(6, 4)", ddl.Type{Name: ddl.Numeric}},
		{"real", ddl.Type{Name: ddl.Float64}},
		{"smallint", ddl.Type{Name: ddl.Int64}},
		{"text", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}},
		{"timestamp without time zone", ddl.Type{Name: ddl.Timestamp}},
		{"timestamp(5)", ddl.Type{Name: ddl.Timestamp}},
		{"timestamptz", ddl.Type{Name: ddl.Timestamp}},
		{"timestamp with time zone", ddl.Type{Name: ddl.Timestamp}},
		{"timestamptz(5)", ddl.Type{Name: ddl.Timestamp}},
		{"varchar", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"varchar(42)", ddl.Type{Name: ddl.String, Len: int64(42)}},
		{"json", ddl.Type{Name: ddl.JSON}},
		{"jsonb", ddl.Type{Name: ddl.JSON}},
	}
	for _, tc := range scalarTests {
		conv, _ := runProcessPgDump(fmt.Sprintf("CREATE TABLE t (a %s);", tc.ty))
		noIssues(conv, t, "Scalar type: "+tc.ty)
		tableId := internal.GetTableIdFromSpName(conv.SpSchema, "t")
		colId := internal.GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, "a")
		assert.Equal(t, conv.SpSchema[tableId].ColDefs[colId].T, tc.expected, "Scalar type: "+tc.ty)
	}
	// Next test array types and not null.
	singleColTests := []struct {
		ty       string
		expected ddl.ColumnDef
	}{
		{"text", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}},
		{"text NOT NULL", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
		{"text array[4]", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}}},
		{"text[4]", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}}},
		{"text[]", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}}},
		{"text[][]", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}}, // Unrecognized array type mapped to string.
	}
	for _, tc := range singleColTests {
		conv, _ := runProcessPgDump(fmt.Sprintf("CREATE TABLE t (a %s);", tc.ty))
		noIssues(conv, t, "Not null: "+tc.ty)
		tableId := internal.GetTableIdFromSpName(conv.SpSchema, "t")
		colId := internal.GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, "a")
		cd := conv.SpSchema[tableId].ColDefs[colId]
		cd.Comment = ""
		cd.Id = ""
		assert.Equal(t, tc.expected, cd, "Not null: "+tc.ty)
	}
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
			input: "CREATE TABLE cart (productid text, userid text, quantity bigint);\n" +
				"ALTER TABLE ONLY cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": ddl.CreateTable{
					Name:   "cart",
					ColIds: []string{"productid", "userid", "quantity"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "productid", Order: 1}, ddl.IndexKey{ColId: "userid", Order: 2}}}},
		},
		{
			name:  "Shopping cart with no primary key",
			input: "CREATE TABLE cart (productid text, userid text NOT NULL, quantity bigint);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": ddl.CreateTable{
					Name:   "cart",
					ColIds: []string{"productid", "userid", "quantity", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
						"synth_id":  ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}}}},
		},
		{
			name:  "Create table with single primary key",
			input: "CREATE TABLE test (a text PRIMARY KEY, b text);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}}},
		},
		{
			name:  "Create table with multiple primary keys",
			input: "CREATE TABLE test (a text, b text, n bigint, PRIMARY KEY (a, b) );\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "n"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"n": ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}, ddl.IndexKey{ColId: "b", Order: 2}}}},
		},
		{
			name: "Create table with single foreign key",
			input: "CREATE TABLE test (a bigint PRIMARY KEY, b text );\n" +
				"CREATE TABLE test2 (c bigint, d bigint, CONSTRAINT fk_test FOREIGN KEY(d) REFERENCES test(a));\n",
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
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"d"}, ReferTableId: "test", ReferColumnIds: []string{"a"}}},
				}},
		},
		{
			name: "Alter table with single foreign key",
			input: "CREATE TABLE test (a bigint PRIMARY KEY, b text );\n" +
				"CREATE TABLE test2 (c bigint, d bigint);\n" +
				"ALTER TABLE ONLY test2 ADD CONSTRAINT fk_test FOREIGN KEY (d) REFERENCES test(a) ON UPDATE CASCADE ON DELETE RESTRICT;\n",
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
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"d"}, ReferTableId: "test", ReferColumnIds: []string{"a"}}},
				}},
		},
		{
			name: "Alter table with single foreign key multiple column",
			input: "CREATE TABLE test (a bigint PRIMARY KEY, b bigint, c text );\n" +
				"CREATE TABLE test2 (c bigint, d bigint);\n" +
				"ALTER TABLE ONLY test2 ADD CONSTRAINT fk_test FOREIGN KEY (c,d) REFERENCES test(a,b) ON UPDATE CASCADE ON DELETE RESTRICT;\n",
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
					ColIds: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"c", "d"}, ReferTableId: "test", ReferColumnIds: []string{"a", "b"}}},
				}},
		},
		{
			name: "Alter table with multiple foreign keys",
			input: "CREATE TABLE test (a bigint PRIMARY KEY, b text );\n" +
				"CREATE TABLE test2 (c bigint PRIMARY KEY, d text );\n" +
				"CREATE TABLE test3 (e bigint, f bigint, g text );\n" +
				"ALTER TABLE ONLY test3 ADD CONSTRAINT fk_test FOREIGN KEY (e) REFERENCES test (a);\n" +
				"ALTER TABLE ONLY test3 ADD CONSTRAINT fk_test2 FOREIGN KEY (f) REFERENCES test2 (c);",
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
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"e"}, ReferTableId: "test", ReferColumnIds: []string{"a"}},
						ddl.Foreignkey{Name: "fk_test2", ColIds: []string{"f"}, ReferTableId: "test2", ReferColumnIds: []string{"c"}}}}},
		},
		{
			name: "Create index statement",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text" +
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
			name: "Create index statement with order",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text" +
				");\n" +
				"CREATE INDEX custom_index ON test (b DESC, c ASC);\n",
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
					Indexes:     []ddl.CreateIndex{ddl.CreateIndex{Name: "custom_index", TableId: "test", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Desc: true, Order: 1}, ddl.IndexKey{ColId: "c", Desc: false, Order: 2}}}}}},
		},
		{
			name: "Create index statement with different sequence order",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text" +
				");\n" +
				"CREATE UNIQUE INDEX custom_index ON test (c DESC, b ASC);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c", Desc: true, Order: 1}, ddl.IndexKey{ColId: "b", Desc: false, Order: 2}},
					Indexes:     []ddl.CreateIndex{},
				}},
		},
		{
			name: "Create table with unique constraint",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text," +
				"CONSTRAINT custom_index UNIQUE(b)" +
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
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Order: 1}},
					Indexes:     []ddl.CreateIndex{},
				}},
		},
		{
			name: "Alter table add unique constraint",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text" +
				");\n" +
				"ALTER TABLE test ADD CONSTRAINT custom_index UNIQUE (b,c);\n",
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
			name:  "Create table with pg schema",
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
			name: "ALTER TABLE SET NOT NULL",
			input: "CREATE TABLE test (a text PRIMARY KEY, b text);\n" +
				"ALTER TABLE test ALTER COLUMN b SET NOT NULL;\n",
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
			input: "CREATE TABLE t1 (a text, b text); CREATE TABLE t2 (c text);" +
				"ALTER TABLE ONLY t1 ADD CONSTRAINT t1_pkey PRIMARY KEY (a);" +
				"ALTER TABLE ONLY t2 ADD CONSTRAINT t2_pkey PRIMARY KEY (c);",
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
			name: "COPY FROM",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"COPY public.test (a, b, n) FROM stdin;\n" +
				"a1	b1	42\n" +
				"a22	b99	6\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "COPY FROM with renamed table/cols",
			input: "CREATE TABLE _test (_a text, b text, n bigint);\n" +
				"ALTER TABLE ONLY _test ADD CONSTRAINT test_pkey PRIMARY KEY (_a, b);" +
				"COPY public._test (_a, b, n) FROM stdin;\n" +
				"a1	b1	42\n" +
				"a22	b99	6\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "Atest", cols: []string{"Aa", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "Atest", cols: []string{"Aa", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "COPY FROM with CRLF",
			input: "CREATE TABLE test (a text, b text, n bigint);\r\n" +
				"COPY public.test (a, b, n) FROM stdin;\r\n" +
				"a1	b1	42\r\n" +
				"a22	b99	6\r\n" +
				"\\.\r\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\r\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "COPY FROM with spaces",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"COPY public.test (a, b, n) FROM stdin;\n" +
				"a1 	 b1	42\n" +
				"a22	b 99 	6\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1 ", " b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b 99 ", int64(6)}}},
		},
		{
			name: "COPY FROM with no primary key",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"COPY public.test (a, b, n) FROM stdin;\n" +
				"a1	b1	42\n" +
				"a22	b99	6\n" +
				"a33	b	9\n" +
				"a3	b	7\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a1", "b1", int64(42), fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a22", "b99", int64(6), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a33", "b", int64(9), fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a3", "b", int64(7), fmt.Sprintf("%d", bitReverse(3))}}},
		},
		{
			name: "COPY FROM with empty cols",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"COPY public.test (a, b, n) FROM stdin;\n" +
				"\\N	b1	42\n" +
				"a22	\\N	6\n" +
				"a33	b	\\N\n" +
				"a3	b	7\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"b", "n", "synth_id"}, vals: []interface{}{"b1", int64(42), fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"a", "n", "synth_id"}, vals: []interface{}{"a22", int64(6), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"a", "b", "synth_id"}, vals: []interface{}{"a33", "b", fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a3", "b", int64(7), fmt.Sprintf("%d", bitReverse(3))}}},
		},
		{
			name: "INSERT",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"INSERT INTO test (a, b, n) VALUES ('a42', 'b6', 2);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a42", "b6", int64(2)}}},
		},
		{
			name: "INSERT with multiple rows",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"INSERT INTO test (a, b, n) VALUES ('a42', 'b6', 2), ('a43', 'b7', 3);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a42", "b6", int64(2)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a43", "b7", int64(3)}},
			},
		},
		{
			name: "INSERT with no cols",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"INSERT INTO test VALUES ('a42', 'b6', 2);",
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
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				"INSERT INTO test (a, b, n) VALUES (' a42 ', '\nb6\n', 2);\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{" a42 ", "\nb6\n", int64(2)}}},
		},
		{
			name: "Statements with embedded semicolons",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				"INSERT INTO test (a, b, n) VALUES ('a;\n2', 'b;6\n', 2);\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a;\n2", "b;6\n", int64(2)}}},
			expectIssues: true,
		},
		{
			name: "Tables and columns with illegal characters",
			input: `CREATE TABLE "te.s t" ("a?^" text PRIMARY KEY, "b.b" text, "n*n" bigint);
		                                INSERT INTO "te.s t" ("a?^", "b.b", "n*n") VALUES ('a', 'b', 2);`,
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
			input: `CREATE TABLE "te.s t" ("a?^" text, "b.b" text, "n*n" bigint);
						ALTER TABLE ONLY "te.s t" ADD CONSTRAINT test_pkey PRIMARY KEY ("a?^");
		                                INSERT INTO "te.s t" ("a?^", "b.b", "n*n") VALUES ('a', 'b', 2);`,
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
			name: "Data conversion: bool, bigserial, char, bytea",
			input: `
CREATE TABLE test (id integer PRIMARY KEY, a bool, b bigserial, c char, d bytea);
COPY test (id, a, b, c, d) FROM stdin;
1	true	42	x	\\x0001beef
\.
`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(1), true, int64(42), "x", []byte{0x0, 0x1, 0xbe, 0xef}}}},
		},
		{
			name: "Data conversion: date, float8, float4, int8",
			input: `
CREATE TABLE test (id integer PRIMARY KEY, a date, b float8, c float4);
COPY test (id, a, b, c) FROM stdin;
1	2019-10-29	4.444	5.44444
\.
`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c"}, vals: []interface{}{int64(1), getDate("2019-10-29"), float64(4.444), float64(5.44444)}}},
		},
		{
			name: "Data conversion: int8, int4, int2, numeric",
			input: `
CREATE TABLE test (id integer PRIMARY KEY, a int8, b int4, c int2, d numeric);
COPY test (id, a, b, c, d) FROM stdin;
1	88	44	22	444.9876
\.
`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(1), int64(88), int64(44), int64(22), big.NewRat(1112469, 2500)}}},
		},
		{
			name: "Data conversion: serial, text, timestamp, timestamptz, varchar, json",
			input: `
CREATE TABLE test (id integer PRIMARY KEY, a serial, b text, c timestamp, d timestamptz, e varchar, f json, g jsonb);
COPY test (id, a, b, c, d, e, f, g) FROM stdin;
1	2	my text	2019-10-29 05:30:00	2019-10-29 05:30:00+10:30	my varchar	{"k":"k1", "v":"v1"}	{"k":"k2", "v":"v2"}
\.
`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d", "e", "f", "g"}, vals: []interface{}{int64(1), int64(2), "my text", getTime(t, "2019-10-29T05:30:00Z"), getTime(t, "2019-10-29T05:30:00+10:30"), "my varchar", "{\"k\":\"k1\", \"v\":\"v1\"}", "{\"k\":\"k2\", \"v\":\"v2\"}"}}},
		},
	}
	for _, tc := range multiColTests {
		conv, rows := runProcessPgDump(tc.input)
		assert.ElementsMatch(t, [][]string{{"a"}, {"b"}}, [][]string{{"b"}, {"a"}}, "Array good")
		if !tc.expectIssues {
			noIssues(conv, t, tc.name)
		}
		if tc.expectedSchema != nil {
			internal.AssertSpSchema(conv, t, tc.expectedSchema, stripSchemaComments(conv.SpSchema))
		}
		if tc.expectedData != nil {
			assert.Equal(t, tc.expectedData, rows, tc.name+": Data rows did not match")
		}
	}

	{ // Test set timezone statement.
		conv, _ := runProcessPgDump("set timezone='US/Eastern';")
		loc, _ := time.LoadLocation("US/Eastern")
		assert.Equal(t, conv.Location, loc, "Set timezone")
	}

	// Finally test data conversion errors.
	dataErrorTests := []struct {
		name         string
		input        string
		expectedData []spannerData
	}{
		{
			// Test bad data for each scalar type (except text, which accepts all values) and an array type.
			name: "Data conversion errors",
			input: "CREATE TABLE test (int8 int8, float8 float8, bool bool, timestamp timestamp, date date, bytea bytea, arr integer array);\n" +
				"COPY public.test (int8, float8, bool, timestamp, date, bytea, arr) FROM stdin;\n" +
				"7	42.1	true	2019-10-29 05:30:00	2019-10-29	\\\\x0001beef	{42,6}\n" + // Baseline (good)
				"7	\\N	\\N	\\N	\\N	\\N	\\N\n" + // Good
				"7-	\\N	\\N	\\N	\\N	\\N	\\N\n" + // Error
				"\\N	42.1	\\N	\\N	\\N	\\N	\\N\n" + // Good
				"\\N	4.2.1	\\N	\\N	\\N	\\N	\\N\n" + // Error
				"\\N	\\N	true	\\N	\\N	\\N	\\N\n" + // Good
				"\\N	\\N	truefalse	\\N	\\N	\\N	\\N\n" + // Error
				"\\N	\\N	\\N	2019-10-29 05:30:00	\\N	\\N	\\N\n" + // Good
				"\\N	\\N	\\N	2019-100-29 05:30:00	\\N	\\N	\\N\n" + // Error
				"\\N	\\N	\\N	\\N	2019-10-29	\\N	\\N\n" + // Good
				"\\N	\\N	\\N	\\N	2019-10-42	\\N	\\N\n" + // Error
				"\\N	\\N	\\N	\\N	\\N	\\\\x0001beef	\\N\n" + // Good
				"\\N	\\N	\\N	\\N	\\N	\\ \\x0001beef	\\N\n" + // Error
				"\\N	\\N	\\N	\\N	\\N	\\N	{42,6}\n" + // Good
				"\\N	\\N	\\N	\\N	\\N	\\N	{42, 6}\n" + // Error
				"\\.\n",
			expectedData: []spannerData{
				spannerData{
					table: "test", cols: []string{"int8", "float8", "bool", "timestamp", "date", "bytea", "arr", "synth_id"},
					vals: []interface{}{int64(7), float64(42.1), true, getTime(t, "2019-10-29T05:30:00Z"),
						getDate("2019-10-29"), []byte{0x0, 0x1, 0xbe, 0xef},
						[]spanner.NullInt64{{Int64: 42, Valid: true}, {Int64: 6, Valid: true}},
						fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"int8", "synth_id"}, vals: []interface{}{int64(7), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"float8", "synth_id"}, vals: []interface{}{float64(42.1), fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"bool", "synth_id"}, vals: []interface{}{true, fmt.Sprintf("%d", bitReverse(3))}},
				spannerData{table: "test", cols: []string{"timestamp", "synth_id"}, vals: []interface{}{getTime(t, "2019-10-29T05:30:00Z"), fmt.Sprintf("%d", bitReverse(4))}},
				spannerData{table: "test", cols: []string{"date", "synth_id"}, vals: []interface{}{getDate("2019-10-29"), fmt.Sprintf("%d", bitReverse(5))}},
				spannerData{table: "test", cols: []string{"bytea", "synth_id"}, vals: []interface{}{[]byte{0x0, 0x1, 0xbe, 0xef}, fmt.Sprintf("%d", bitReverse(6))}},
				spannerData{table: "test", cols: []string{"arr", "synth_id"},
					vals: []interface{}{[]spanner.NullInt64{{Int64: 42, Valid: true}, {Int64: 6, Valid: true}}, fmt.Sprintf("%d", bitReverse(7))}},
			},
		},
	}
	for _, tc := range dataErrorTests {
		conv, rows := runProcessPgDump(tc.input)
		assert.Equal(t, tc.expectedData, rows, tc.name+": Data rows did not match")
		assert.Equal(t, conv.BadRows(), int64(7), tc.name+": Error count did not match")
	}
}

func TestProcessPgDumpPGTarget(t *testing.T) {
	// First, test scalar types.
	scalarTests := []struct {
		ty       string
		expected ddl.Type
	}{
		{"bigint", ddl.Type{Name: ddl.Int64}},
		{"bool", ddl.Type{Name: ddl.Bool}},
		{"boolean", ddl.Type{Name: ddl.Bool}},
		{"bytea", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		{"char(42)", ddl.Type{Name: ddl.String, Len: int64(42)}},
		{"date", ddl.Type{Name: ddl.Date}},
		{"decimal", ddl.Type{Name: ddl.Numeric}}, // pg parser maps this to numeric.
		{"double precision", ddl.Type{Name: ddl.Float64}},
		{"float8", ddl.Type{Name: ddl.Float64}},
		{"float4", ddl.Type{Name: ddl.Float64}},
		{"integer", ddl.Type{Name: ddl.Int64}},
		{"numeric", ddl.Type{Name: ddl.Numeric}},
		{"numeric(4)", ddl.Type{Name: ddl.Numeric}},
		{"numeric(6, 4)", ddl.Type{Name: ddl.Numeric}},
		{"real", ddl.Type{Name: ddl.Float64}},
		{"smallint", ddl.Type{Name: ddl.Int64}},
		{"text", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}},
		{"timestamp without time zone", ddl.Type{Name: ddl.Timestamp}},
		{"timestamp(5)", ddl.Type{Name: ddl.Timestamp}},
		{"timestamptz", ddl.Type{Name: ddl.Timestamp}},
		{"timestamp with time zone", ddl.Type{Name: ddl.Timestamp}},
		{"timestamptz(5)", ddl.Type{Name: ddl.Timestamp}},
		{"varchar", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"varchar(42)", ddl.Type{Name: ddl.String, Len: int64(42)}},
	}
	for _, tc := range scalarTests {
		conv, _ := runProcessPgDumpPGTarget(fmt.Sprintf("CREATE TABLE t (a %s);", tc.ty))
		noIssues(conv, t, "Scalar type: "+tc.ty)
		tableId, _ := internal.GetTableIdFromSrcName(conv.SrcSchema, "t")
		columnId, _ := internal.GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, "a")
		assert.Equal(t, conv.SpSchema[tableId].ColDefs[columnId].T, tc.expected, "Scalar type: "+tc.ty)
	}
	// Next test array types and not null. For PG Spanner, all array types mapped to string.
	singleColTests := []struct {
		ty       string
		expected ddl.ColumnDef
	}{
		{"text", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}},
		{"text NOT NULL", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
		{"text array[4]", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}},
		{"text[4]", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}},
		{"text[]", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}},
		{"text[][]", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}},
	}
	for _, tc := range singleColTests {
		conv, _ := runProcessPgDumpPGTarget(fmt.Sprintf("CREATE TABLE t (a %s);", tc.ty))
		noIssues(conv, t, "Not null: "+tc.ty)
		tableId, _ := internal.GetTableIdFromSrcName(conv.SrcSchema, "t")
		columnId, _ := internal.GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, "a")
		cd := conv.SpSchema[tableId].ColDefs[columnId]
		cd.Comment = ""
		cd.Id = ""
		assert.Equal(t, tc.expected, cd, "Not null: "+tc.ty)
	}
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
			input: "CREATE TABLE cart (productid text, userid text, quantity bigint);\n" +
				"ALTER TABLE ONLY cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": ddl.CreateTable{
					Name:   "cart",
					ColIds: []string{"productid", "userid", "quantity"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "productid", Order: 1}, ddl.IndexKey{ColId: "userid", Order: 2}}}},
		},
		{
			name:  "Shopping cart with no primary key",
			input: "CREATE TABLE cart (productid text, userid text NOT NULL, quantity bigint);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": ddl.CreateTable{
					Name:   "cart",
					ColIds: []string{"productid", "userid", "quantity", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
						"synth_id":  ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}}}},
		},
		{
			name:  "Create table with single primary key",
			input: "CREATE TABLE test (a text PRIMARY KEY, b text);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}}}},
		},
		{
			name:  "Create table with multiple primary keys",
			input: "CREATE TABLE test (a text, b text, n bigint, PRIMARY KEY (a, b) );\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "n"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"n": ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.Int64}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "a", Order: 1}, ddl.IndexKey{ColId: "b", Order: 2}}}},
		},
		{
			name: "Create table with single foreign key",
			input: "CREATE TABLE test (a bigint PRIMARY KEY, b text );\n" +
				"CREATE TABLE test2 (c bigint, d bigint, CONSTRAINT fk_test FOREIGN KEY(d) REFERENCES test(a));\n",
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
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"d"}, ReferTableId: "test", ReferColumnIds: []string{"a"}}},
				}},
		},
		{
			name: "Alter table with single foreign key",
			input: "CREATE TABLE test (a bigint PRIMARY KEY, b text );\n" +
				"CREATE TABLE test2 (c bigint, d bigint);\n" +
				"ALTER TABLE ONLY test2 ADD CONSTRAINT fk_test FOREIGN KEY (d) REFERENCES test(a) ON UPDATE CASCADE ON DELETE RESTRICT;\n",
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
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"d"}, ReferTableId: "test", ReferColumnIds: []string{"a"}}},
				}},
		},
		{
			name: "Alter table with single foreign key multiple column",
			input: "CREATE TABLE test (a bigint PRIMARY KEY, b bigint, c text );\n" +
				"CREATE TABLE test2 (c bigint, d bigint);\n" +
				"ALTER TABLE ONLY test2 ADD CONSTRAINT fk_test FOREIGN KEY (c,d) REFERENCES test(a,b) ON UPDATE CASCADE ON DELETE RESTRICT;\n",
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
					ColIds: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"c", "d"}, ReferTableId: "test", ReferColumnIds: []string{"a", "b"}}},
				}},
		},
		{
			name: "Alter table with multiple foreign keys",
			input: "CREATE TABLE test (a bigint PRIMARY KEY, b text );\n" +
				"CREATE TABLE test2 (c bigint PRIMARY KEY, d text );\n" +
				"CREATE TABLE test3 (e bigint, f bigint, g text );\n" +
				"ALTER TABLE ONLY test3 ADD CONSTRAINT fk_test FOREIGN KEY (e) REFERENCES test (a);\n" +
				"ALTER TABLE ONLY test3 ADD CONSTRAINT fk_test2 FOREIGN KEY (f) REFERENCES test2 (c);",
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
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"e"}, ReferTableId: "test", ReferColumnIds: []string{"a"}},
						ddl.Foreignkey{Name: "fk_test2", ColIds: []string{"f"}, ReferTableId: "test2", ReferColumnIds: []string{"c"}}}}},
		},
		{
			name: "Create index statement",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text" +
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
			name: "Create index statement with order",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text" +
				");\n" +
				"CREATE INDEX custom_index ON test (b DESC, c ASC);\n",
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
					Indexes:     []ddl.CreateIndex{ddl.CreateIndex{Name: "custom_index", TableId: "test", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Desc: true, Order: 1}, ddl.IndexKey{ColId: "c", Desc: false, Order: 2}}}}}},
		},
		{
			name: "Create index statement with different sequence order",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text" +
				");\n" +
				"CREATE UNIQUE INDEX custom_index ON test (c DESC, b ASC);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:   "test",
					ColIds: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c", Desc: true, Order: 1}, ddl.IndexKey{ColId: "b", Desc: false, Order: 2}},
					Indexes:     []ddl.CreateIndex{},
				}},
		},
		{
			name: "Create table with unique constraint",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text," +
				"CONSTRAINT custom_index UNIQUE(b)" +
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
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "b", Order: 1}},
					Indexes:     []ddl.CreateIndex{},
				}},
		},
		{
			name: "Alter table add unique constraint",
			input: "CREATE TABLE test (" +
				"a smallint," +
				"b text," +
				"c text" +
				");\n" +
				"ALTER TABLE test ADD CONSTRAINT custom_index UNIQUE (b,c);\n",
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
			name:  "Create table with pg schema",
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
			name: "ALTER TABLE SET NOT NULL",
			input: "CREATE TABLE test (a text PRIMARY KEY, b text);\n" +
				"ALTER TABLE test ALTER COLUMN b SET NOT NULL;\n",
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
			input: "CREATE TABLE t1 (a text, b text); CREATE TABLE t2 (c text);" +
				"ALTER TABLE ONLY t1 ADD CONSTRAINT t1_pkey PRIMARY KEY (a);" +
				"ALTER TABLE ONLY t2 ADD CONSTRAINT t2_pkey PRIMARY KEY (c);",
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
			name: "COPY FROM",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"COPY public.test (a, b, n) FROM stdin;\n" +
				"a1	b1	42\n" +
				"a22	b99	6\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "COPY FROM with renamed table/cols",
			input: "CREATE TABLE _test (_a text, b text, n bigint);\n" +
				"ALTER TABLE ONLY _test ADD CONSTRAINT test_pkey PRIMARY KEY (_a, b);" +
				"COPY public._test (_a, b, n) FROM stdin;\n" +
				"a1	b1	42\n" +
				"a22	b99	6\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "Atest", cols: []string{"Aa", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "Atest", cols: []string{"Aa", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "COPY FROM with CRLF",
			input: "CREATE TABLE test (a text, b text, n bigint);\r\n" +
				"COPY public.test (a, b, n) FROM stdin;\r\n" +
				"a1	b1	42\r\n" +
				"a22	b99	6\r\n" +
				"\\.\r\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\r\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1", "b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b99", int64(6)}}},
		},
		{
			name: "COPY FROM with spaces",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"COPY public.test (a, b, n) FROM stdin;\n" +
				"a1 	 b1	42\n" +
				"a22	b 99 	6\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a1 ", " b1", int64(42)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a22", "b 99 ", int64(6)}}},
		},
		{
			name: "COPY FROM with no primary key",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"COPY public.test (a, b, n) FROM stdin;\n" +
				"a1	b1	42\n" +
				"a22	b99	6\n" +
				"a33	b	9\n" +
				"a3	b	7\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a1", "b1", int64(42), fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a22", "b99", int64(6), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a33", "b", int64(9), fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a3", "b", int64(7), fmt.Sprintf("%d", bitReverse(3))}}},
		},
		{
			name: "COPY FROM with empty cols",
			input: "CREATE TABLE test (a text, b text, n bigint);\n" +
				"COPY public.test (a, b, n) FROM stdin;\n" +
				"\\N	b1	42\n" +
				"a22	\\N	6\n" +
				"a33	b	\\N\n" +
				"a3	b	7\n" +
				"\\.\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"b", "n", "synth_id"}, vals: []interface{}{"b1", int64(42), fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"a", "n", "synth_id"}, vals: []interface{}{"a22", int64(6), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"a", "b", "synth_id"}, vals: []interface{}{"a33", "b", fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a3", "b", int64(7), fmt.Sprintf("%d", bitReverse(3))}}},
		},
		{
			name: "INSERT",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"INSERT INTO test (a, b, n) VALUES ('a42', 'b6', 2);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a42", "b6", int64(2)}}},
		},
		{
			name: "INSERT with multiple rows",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"INSERT INTO test (a, b, n) VALUES ('a42', 'b6', 2), ('a43', 'b7', 3);",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a42", "b6", int64(2)}},
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a43", "b7", int64(3)}},
			},
		},
		{
			name: "INSERT with no cols",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);" +
				"INSERT INTO test VALUES ('a42', 'b6', 2);",
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
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				"INSERT INTO test (a, b, n) VALUES (' a42 ', '\nb6\n', 2);\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{" a42 ", "\nb6\n", int64(2)}}},
		},
		{
			name: "Statements with embedded semicolons",
			input: "CREATE TABLE test (a text NOT NULL, b text NOT NULL, n bigint);\n" +
				"ALTER TABLE ONLY test ADD CONSTRAINT test_pkey PRIMARY KEY (a, b);\n" +
				"INSERT INTO test (a, b, n) VALUES ('a;\n2', 'b;6\n', 2);\n",
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"a", "b", "n"}, vals: []interface{}{"a;\n2", "b;6\n", int64(2)}}},
			expectIssues: true,
		},
		{
			name: "Tables and columns with illegal characters",
			input: `CREATE TABLE "te.s t" ("a?^" text PRIMARY KEY, "b.b" text, "n*n" bigint);
                                INSERT INTO "te.s t" ("a?^", "b.b", "n*n") VALUES ('a', 'b', 2);`,
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
			input: `CREATE TABLE "te.s t" ("a?^" text, "b.b" text, "n*n" bigint);
				ALTER TABLE ONLY "te.s t" ADD CONSTRAINT test_pkey PRIMARY KEY ("a?^");
                                INSERT INTO "te.s t" ("a?^", "b.b", "n*n") VALUES ('a', 'b', 2);`,
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
			name: "Data conversion: bool, bigserial, char, bytea",
			input: `
CREATE TABLE test (id integer PRIMARY KEY, a bool, b bigserial, c char, d bytea);
COPY test (id, a, b, c, d) FROM stdin;
1	true	42	x	\\x0001beef
\.
`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(1), true, int64(42), "x", []byte{0x0, 0x1, 0xbe, 0xef}}}},
		},
		{
			name: "Data conversion: date, float8, float4, int8",
			input: `
CREATE TABLE test (id integer PRIMARY KEY, a date, b float8, c float4);
COPY test (id, a, b, c) FROM stdin;
1	2019-10-29	4.444	5.44444
\.
`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c"}, vals: []interface{}{int64(1), getDate("2019-10-29"), float64(4.444), float64(5.44444)}}},
		},
		{
			name: "Data conversion: int8, int4, int2, numeric",
			input: `
CREATE TABLE test (id integer PRIMARY KEY, a int8, b int4, c int2, d numeric);
COPY test (id, a, b, c, d) FROM stdin;
1	88	44	22	444.9876
\.
`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(1), int64(88), int64(44), int64(22), spanner.PGNumeric{Numeric: "444.9876", Valid: true}}}},
		},
		{
			name: "Data conversion: serial, text, timestamp, timestamptz, varchar",
			input: `
CREATE TABLE test (id integer PRIMARY KEY, a serial, b text, c timestamp, d timestamptz, e varchar);
COPY test (id, a, b, c, d, e) FROM stdin;
1	2	my text	2019-10-29 05:30:00	2019-10-29 05:30:00+10:30	my varchar
\.
`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d", "e"}, vals: []interface{}{int64(1), int64(2), "my text", getTime(t, "2019-10-29T05:30:00Z"), getTime(t, "2019-10-29T05:30:00+10:30"), "my varchar"}}},
		},
	}
	for _, tc := range multiColTests {
		conv, rows := runProcessPgDumpPGTarget(tc.input)
		if !tc.expectIssues {
			noIssues(conv, t, tc.name)
		}
		if tc.expectedSchema != nil {
			internal.AssertSpSchema(conv, t, tc.expectedSchema, stripSchemaComments(conv.SpSchema))
		}
		if tc.expectedData != nil {
			assert.Equal(t, tc.expectedData, rows, tc.name+": Data rows did not match")
		}
	}

	{ // Test set timezone statement.
		conv, _ := runProcessPgDumpPGTarget("set timezone='US/Eastern';")
		loc, _ := time.LoadLocation("US/Eastern")
		assert.Equal(t, conv.Location, loc, "Set timezone")
	}

	// Finally test data conversion errors.
	dataErrorTests := []struct {
		name         string
		input        string
		expectedData []spannerData
	}{
		{
			// Test bad data for each scalar type (except text, which accepts all values) and an array type.
			name: "Data conversion errors",
			input: "CREATE TABLE test (int8 int8, float8 float8, bool bool, timestamp timestamp, date date, bytea bytea, arr integer array);\n" +
				"COPY public.test (int8, float8, bool, timestamp, date, bytea, arr) FROM stdin;\n" +
				"7	42.1	true	2019-10-29 05:30:00	2019-10-29	\\\\x0001beef	{42,6}\n" + // Baseline (good)
				"7	\\N	\\N	\\N	\\N	\\N	\\N\n" + // Good
				"7-	\\N	\\N	\\N	\\N	\\N	\\N\n" + // Error
				"\\N	42.1	\\N	\\N	\\N	\\N	\\N\n" + // Good
				"\\N	4.2.1	\\N	\\N	\\N	\\N	\\N\n" + // Error
				"\\N	\\N	true	\\N	\\N	\\N	\\N\n" + // Good
				"\\N	\\N	truefalse	\\N	\\N	\\N	\\N\n" + // Error
				"\\N	\\N	\\N	2019-10-29 05:30:00	\\N	\\N	\\N\n" + // Good
				"\\N	\\N	\\N	2019-100-29 05:30:00	\\N	\\N	\\N\n" + // Error
				"\\N	\\N	\\N	\\N	2019-10-29	\\N	\\N\n" + // Good
				"\\N	\\N	\\N	\\N	2019-10-42	\\N	\\N\n" + // Error
				"\\N	\\N	\\N	\\N	\\N	\\\\x0001beef	\\N\n" + // Good
				"\\N	\\N	\\N	\\N	\\N	\\ \\x0001beef	\\N\n" + // Error
				"\\N	\\N	\\N	\\N	\\N	\\N	{42,6}\n" + // Good
				"\\N	\\N	\\N	\\N	\\N	\\N	{42, 6}\n" + // Good
				"\\.\n",
			expectedData: []spannerData{
				spannerData{
					table: "test", cols: []string{"int8", "float8", "bool", "timestamp", "date", "bytea", "arr", "synth_id"},
					vals: []interface{}{int64(7), float64(42.1), true, getTime(t, "2019-10-29T05:30:00Z"),
						getDate("2019-10-29"), []byte{0x0, 0x1, 0xbe, 0xef},
						"{42,6}",
						fmt.Sprintf("%d", bitReverse(0))}},
				spannerData{table: "test", cols: []string{"int8", "synth_id"}, vals: []interface{}{int64(7), fmt.Sprintf("%d", bitReverse(1))}},
				spannerData{table: "test", cols: []string{"float8", "synth_id"}, vals: []interface{}{float64(42.1), fmt.Sprintf("%d", bitReverse(2))}},
				spannerData{table: "test", cols: []string{"bool", "synth_id"}, vals: []interface{}{true, fmt.Sprintf("%d", bitReverse(3))}},
				spannerData{table: "test", cols: []string{"timestamp", "synth_id"}, vals: []interface{}{getTime(t, "2019-10-29T05:30:00Z"), fmt.Sprintf("%d", bitReverse(4))}},
				spannerData{table: "test", cols: []string{"date", "synth_id"}, vals: []interface{}{getDate("2019-10-29"), fmt.Sprintf("%d", bitReverse(5))}},
				spannerData{table: "test", cols: []string{"bytea", "synth_id"}, vals: []interface{}{[]byte{0x0, 0x1, 0xbe, 0xef}, fmt.Sprintf("%d", bitReverse(6))}},
				spannerData{table: "test", cols: []string{"arr", "synth_id"}, vals: []interface{}{"{42,6}", fmt.Sprintf("%d", bitReverse(7))}},
				spannerData{table: "test", cols: []string{"arr", "synth_id"}, vals: []interface{}{"{42, 6}", fmt.Sprintf("%d", bitReverse(8))}},
			},
		},
	}
	for _, tc := range dataErrorTests {
		conv, rows := runProcessPgDumpPGTarget(tc.input)
		assert.Equal(t, tc.expectedData, rows, tc.name+": Data rows did not match")
		assert.Equal(t, conv.BadRows(), int64(6), tc.name+": Error count did not match")
	}
}

// The following test Conv API calls based on data generated by ProcessPgDump.

func TestProcessPgDump_GetDDL(t *testing.T) {
	conv, _ := runProcessPgDump("CREATE TABLE cart (productid text, userid text, quantity bigint);\n" +
		"ALTER TABLE ONLY cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);")
	expected :=
		"CREATE TABLE cart (\n" +
			"	productid STRING(MAX) NOT NULL,\n" +
			"	userid STRING(MAX) NOT NULL,\n" +
			"	quantity INT64,\n" +
			") PRIMARY KEY (productid, userid)"
	c := ddl.Config{Tables: true}
	assert.Equal(t, expected, strings.Join(conv.SpSchema.GetDDL(c), " "))
}

func TestProcessPgDump_GetPGDDL(t *testing.T) {
	conv, _ := runProcessPgDumpPGTarget("CREATE TABLE cart (productid text, userid text, quantity bigint);\n" +
		"ALTER TABLE ONLY cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);")
	expected :=
		"CREATE TABLE cart (\n" +
			"	productid VARCHAR(2621440) NOT NULL,\n" +
			"	userid VARCHAR(2621440) NOT NULL,\n" +
			"	quantity INT8,\n" +
			"	PRIMARY KEY (productid, userid)\n" +
			")"
	c := ddl.Config{Tables: true, TargetDb: conv.TargetDb}
	assert.Equal(t, expected, strings.Join(conv.SpSchema.GetDDL(c), " "))
}

func TestProcessPgDump_Rows(t *testing.T) {
	conv, _ := runProcessPgDump("CREATE TABLE cart (a text, n bigint);\n" +
		"INSERT INTO cart (a, n) VALUES ('a42', 2);")
	assert.Equal(t, int64(0), conv.Rows())
}

func TestProcessPgDump_BadRows(t *testing.T) {
	conv, _ := runProcessPgDump("CREATE TABLE cart (a text, n bigint);\n" +
		"INSERT INTO cart (a, n) VALUES ('a42', 'not_a_number');")
	assert.Equal(t, int64(1), conv.BadRows())
}

func TestProcessPgDump_GetBadRows(t *testing.T) {
	conv, _ := runProcessPgDump("CREATE TABLE cart (a text, n bigint);\n" +
		"INSERT INTO cart (a, n) VALUES ('a42', 'not_a_number');")
	assert.Equal(t, 1, len(conv.SampleBadRows(100)))
}

func TestProcessPgDump_AddPrimaryKeys(t *testing.T) {
	cases := []struct {
		name           string
		input          string
		expectedSchema map[string]ddl.CreateTable
	}{
		{
			name:  "Shopping cart",
			input: "CREATE TABLE cart (productid text, userid text, quantity bigint);",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": {
					Name:   "cart",
					ColIds: []string{"productid", "userid", "quantity", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": {Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"userid":    {Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"quantity":  {Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
						"synth_id":  {Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Order: 1}}}},
		},
		{
			name:  "synth_id clash",
			input: "CREATE TABLE test (synth_id text, synth_id0 text, synth_id1 bigint);",
			expectedSchema: map[string]ddl.CreateTable{
				"test": {
					Name:   "test",
					ColIds: []string{"synth_id", "synth_id0", "synth_id1", "synth_id2"},
					ColDefs: map[string]ddl.ColumnDef{
						"synth_id":  {Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id0": {Name: "synth_id0", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id1": {Name: "synth_id1", T: ddl.Type{Name: ddl.Int64}},
						"synth_id2": {Name: "synth_id2", T: ddl.Type{Name: ddl.String, Len: 50}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id2", Order: 1}}}},
		},
	}
	for _, tc := range cases {
		conv, _ := runProcessPgDump(tc.input)
		conv.AddPrimaryKeys()
		if tc.expectedSchema != nil {
			internal.AssertSpSchema(conv, t, tc.expectedSchema, stripSchemaComments(conv.SpSchema))
		}
	}
}

func TestProcessPgDump_WithUnparsableContent(t *testing.T) {
	s := "This is unparsable content"
	conv := internal.MakeConv()
	conv.SetLocation(time.UTC)
	conv.SetSchemaMode()
	err := common.ProcessDbDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil), DbDumpImpl{})
	if err == nil {
		t.Fatalf("Expect an error, but got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "error parsing") {
		t.Fatalf("Expect a parsing error, but got %q", err)
	}
}

func runProcessPgDump(s string) (*internal.Conv, []spannerData) {
	conv := internal.MakeConv()
	conv.SetLocation(time.UTC)
	conv.SetSchemaMode()
	pgDump := DbDumpImpl{}
	common.ProcessDbDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil), pgDump)
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	common.ProcessDbDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil), pgDump)
	return conv, rows
}

func runProcessPgDumpPGTarget(s string) (*internal.Conv, []spannerData) {
	conv := internal.MakeConv()
	conv.TargetDb = constants.TargetExperimentalPostgres
	conv.SetLocation(time.UTC)
	conv.SetSchemaMode()
	pgDump := DbDumpImpl{}
	common.ProcessDbDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil), pgDump)
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	common.ProcessDbDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil), pgDump)
	return conv, rows
}

// noIssues verifies that conversion was issue-free by checking that conv
// contains no unexpected conditions, statement errors, etc. Note that
// many tests are issue-free, but several explicitly test handling of
// various issues (so don't call nonIssue for them!).
func noIssues(conv *internal.Conv, t *testing.T, name string) {
	assert.Zero(t, len(conv.Stats.Unexpected), fmt.Sprintf("'%s' generated unexpected conditions: %v", name, conv.Stats.Unexpected))
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

// printJSON prints the JSON version of the AST for PostgreSQL statement 's'.
// It is not used by any tests, but is a useful utility for debugging.
func printJSON(s string) {
	json, err := pg_query.ParseToJSON(s)
	if err == nil {
		fmt.Printf("JSON for: %s\n %s\n", s, json)
	} else {
		fmt.Printf("Can't parse %s\n", s)
	}
}

// printJSONType prints the JSON version of the AST for PostgresSQL type 'ty'.
// It is not used by any tests, but is a useful utility for debugging.
func printJSONType(ty string) {
	printJSON(fmt.Sprintf("CREATE TABLE t (a %s);", ty))
}
