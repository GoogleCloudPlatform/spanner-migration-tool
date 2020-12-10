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
	"fmt"
	"math/bits"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestProcessMySQLDump_Scalar(t *testing.T) {
	// First, test scalar types.
	scalarTests := []struct {
		ty       string
		expected ddl.Type
	}{
		{"bigint", ddl.Type{Name: ddl.Int64}},
		{"bool", ddl.Type{Name: ddl.Bool}},
		{"boolean", ddl.Type{Name: ddl.Bool}},
		{"tinyint(1)", ddl.Type{Name: ddl.Bool}},
		{"tinyint(4)", ddl.Type{Name: ddl.Int64}},
		{"blob", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		{"char(42)", ddl.Type{Name: ddl.String, Len: int64(42)}},
		{"date", ddl.Type{Name: ddl.Date}},
		{"decimal(4,10)", ddl.Type{Name: ddl.Float64}},
		{"double(4,10)", ddl.Type{Name: ddl.Float64}},
		{"float(4,10)", ddl.Type{Name: ddl.Float64}},
		{"integer", ddl.Type{Name: ddl.Int64}},
		{"mediumint", ddl.Type{Name: ddl.Int64}},
		{"int", ddl.Type{Name: ddl.Int64}},
		{"bit", ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		{"smallint", ddl.Type{Name: ddl.Int64}},
		{"text", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"tinytext", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"mediumtext", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"longtext", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"enum('a','b')", ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		{"timestamp", ddl.Type{Name: ddl.Timestamp}},
		{"datetime", ddl.Type{Name: ddl.Timestamp}},
		{"varchar(42)", ddl.Type{Name: ddl.String, Len: int64(42)}},
	}
	for _, tc := range scalarTests {
		t.Run(tc.ty, func(t *testing.T) {
			conv, _ := runProcessMySQLDump(fmt.Sprintf("CREATE TABLE t (a %s);", tc.ty))
			noIssues(conv, t, "Scalar type: "+tc.ty)
			assert.Equal(t, conv.SpSchema["t"].ColDefs["a"].T, tc.expected, "Scalar type: "+tc.ty)
		})
	}
}

func TestProcessMySQLDump_SingleCol(t *testing.T) {
	// Test array types and not null.
	singleColTests := []struct {
		ty       string
		expected ddl.ColumnDef
	}{
		{"set('a','b','c')", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}}},
		{"text NOT NULL", ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
	}
	for _, tc := range singleColTests {
		t.Run(tc.ty, func(t *testing.T) {
			conv, _ := runProcessMySQLDump(fmt.Sprintf("CREATE TABLE t (a %s);", tc.ty))
			noIssues(conv, t, "Not null: "+tc.ty)
			cd := conv.SpSchema["t"].ColDefs["a"]
			cd.Comment = ""
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
			input: "CREATE TABLE cart (productid text, userid text, quantity bigint);\n" +
				"ALTER TABLE cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": ddl.CreateTable{
					Name:     "cart",
					ColNames: []string{"productid", "userid", "quantity"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "productid"}, ddl.IndexKey{Col: "userid"}}}},
		},
		{
			name:  "Shopping cart with no primary key",
			input: "CREATE TABLE cart (productid text, userid text NOT NULL, quantity bigint);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"cart": ddl.CreateTable{
					Name:     "cart",
					ColNames: []string{"productid", "userid", "quantity", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
						"synth_id":  ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}}}},
		},
		{
			name:  "Create table with single primary key",
			input: "CREATE TABLE test (a text PRIMARY KEY, b text);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}}},
		},
		{
			name:  "Create table with multiple primary keys",
			input: "CREATE TABLE test (a text, b text, n bigint, PRIMARY KEY (a, b) );\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b", "n"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"n": ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}, ddl.IndexKey{Col: "b"}}}},
		},
		{
			name: "Create table with single foreign key",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d SMALLINT, CONSTRAINT `fk_test` FOREIGN KEY (d) REFERENCES test (a) ON DELETE RESTRICT ON UPDATE CASCADE );",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}},
				"test2": ddl.CreateTable{
					Name:     "test2",
					ColNames: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"d"}, ReferTable: "test", ReferColumns: []string{"a"}}}}},
		},
		{
			name: "Create table with multiple foreign key test constraint name",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test3 (e SMALLINT, f text, PRIMARY KEY (e) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d SMALLINT, CONSTRAINT `1_fk_test_2` FOREIGN KEY (d) REFERENCES test (a) ON DELETE RESTRICT ON UPDATE CASCADE );\n" +
				"ALTER TABLE test2 ADD CONSTRAINT __fk_test_2 FOREIGN KEY (c) REFERENCES test3(e);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}},
				"test3": ddl.CreateTable{
					Name:     "test3",
					ColNames: []string{"e", "f"},
					ColDefs: map[string]ddl.ColumnDef{
						"e": ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"f": ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "e"}}},
				"test2": ddl.CreateTable{
					Name:     "test2",
					ColNames: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "A_fk_test_2", Columns: []string{"d"}, ReferTable: "test", ReferColumns: []string{"a"}},
						ddl.Foreignkey{Name: "A_fk_test_2_1", Columns: []string{"c"}, ReferTable: "test3", ReferColumns: []string{"e"}}}}},
		},
		{
			name: "Alter table add foreign key",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d SMALLINT );\n" +
				"ALTER TABLE test2 ADD FOREIGN KEY (d) REFERENCES test(a);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}},
				"test2": ddl.CreateTable{
					Name:     "test2",
					ColNames: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Columns: []string{"d"}, ReferTable: "test", ReferColumns: []string{"a"}}}}},
		},
		{
			name: "Alter table add constraint foreign key",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d SMALLINT );\n" +
				"ALTER TABLE test2 ADD CONSTRAINT fk_test FOREIGN KEY (d) REFERENCES test(a);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}},
				"test2": ddl.CreateTable{
					Name:     "test2",
					ColNames: []string{"c", "d", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
						"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Int64}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"d"}, ReferTable: "test", ReferColumns: []string{"a"}}}}},
		},
		{
			name: "Create table with multiple foreign keys",
			input: "CREATE TABLE test (a SMALLINT, b text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (c SMALLINT, d text, PRIMARY KEY (c) );\n" +
				"CREATE TABLE test3 (e SMALLINT, f SMALLINT, g text, CONSTRAINT `fk_test` FOREIGN KEY (e) REFERENCES test (a) ON DELETE RESTRICT ON UPDATE CASCADE,CONSTRAINT `fk_test2` FOREIGN KEY (f) REFERENCES test2 (c) ON DELETE RESTRICT ON UPDATE CASCADE );",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}},
				"test2": ddl.CreateTable{
					Name:     "test2",
					ColNames: []string{"c", "d"},
					ColDefs: map[string]ddl.ColumnDef{
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"d": ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "c"}}},
				"test3": ddl.CreateTable{
					Name:     "test3",
					ColNames: []string{"e", "f", "g", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"e":        ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Int64}},
						"f":        ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Int64}},
						"g":        ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"e"}, ReferTable: "test", ReferColumns: []string{"a"}},
						ddl.Foreignkey{Name: "fk_test2", Columns: []string{"f"}, ReferTable: "test2", ReferColumns: []string{"c"}}}}},
		},
		{
			name: "Create table with single foreign key multiple column",
			input: "CREATE TABLE test (a SMALLINT, b SMALLINT, c text, PRIMARY KEY (a) );\n" +
				"CREATE TABLE test2 (e SMALLINT, f SMALLINT, g text, CONSTRAINT `fk_test` FOREIGN KEY (e,f) REFERENCES test (a,b) ON DELETE RESTRICT ON UPDATE CASCADE );",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}},

				"test2": ddl.CreateTable{
					Name:     "test2",
					ColNames: []string{"e", "f", "g", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"e":        ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Int64}},
						"f":        ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Int64}},
						"g":        ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"e", "f"}, ReferTable: "test", ReferColumns: []string{"a", "b"}}}}},
		},
		{
			name:  "Create table with mysql schema",
			input: "CREATE TABLE myschema.test (a text PRIMARY KEY, b text);\n",
			expectedSchema: map[string]ddl.CreateTable{
				"myschema_test": ddl.CreateTable{
					Name:     "myschema_test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}}},
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
					Name:     "test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}}},
			expectIssues: true, // conv.Stats.Reparsed != 0
		},
		{
			name: "ALTER TABLE SET NOT NULL",
			input: "CREATE TABLE test (a text PRIMARY KEY, b text);\n" +
				"ALTER TABLE test MODIFY b text NOT NULL;\n",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}}},
		},
		{
			name: "Multiple statements on one line",
			input: "CREATE TABLE t1 (a text, b text); CREATE TABLE t2 (c text);\n" +
				"ALTER TABLE t1 ADD CONSTRAINT t1_pkey PRIMARY KEY (a);\n" +
				"ALTER TABLE t2 ADD CONSTRAINT t2_pkey PRIMARY KEY (c);",
			expectedSchema: map[string]ddl.CreateTable{
				"t1": ddl.CreateTable{
					Name:     "t1",
					ColNames: []string{"a", "b"},
					ColDefs: map[string]ddl.ColumnDef{
						"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}}},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}},
				"t2": ddl.CreateTable{
					Name:     "t2",
					ColNames: []string{"c"},
					ColDefs: map[string]ddl.ColumnDef{
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "c"}}}},
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
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a1", "b1", int64(42), bitReverse(0)}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a22", "b99", int64(6), bitReverse(1)}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a33", "b", int64(9), bitReverse(2)}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a3", "b", int64(7), bitReverse(3)}}},
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
				spannerData{table: "test", cols: []string{"b", "n", "synth_id"}, vals: []interface{}{"b1", int64(42), bitReverse(0)}},
				spannerData{table: "test", cols: []string{"a", "n", "synth_id"}, vals: []interface{}{"a22", int64(6), bitReverse(1)}},
				spannerData{table: "test", cols: []string{"a", "b", "synth_id"}, vals: []interface{}{"a33", "b", bitReverse(2)}},
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a3", "b", int64(7), bitReverse(3)}}},
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
				spannerData{table: "test", cols: []string{"a", "b", "n", "synth_id"}, vals: []interface{}{"a42", "b6", int64(2), bitReverse(0)}}},
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
					Name:     "te_s_t",
					ColNames: []string{"a__", "b_b", "n_n"},
					ColDefs: map[string]ddl.ColumnDef{
						"a__": ddl.ColumnDef{Name: "a__", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b_b": ddl.ColumnDef{Name: "b_b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"n_n": ddl.ColumnDef{Name: "n_n", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a__"}}}},
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
					Name:     "te_s_t",
					ColNames: []string{"a__", "b_b", "n_n"},
					ColDefs: map[string]ddl.ColumnDef{
						"a__": ddl.ColumnDef{Name: "a__", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"b_b": ddl.ColumnDef{Name: "b_b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"n_n": ddl.ColumnDef{Name: "n_n", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a__"}}}},
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
				spannerData{table: "test", cols: []string{"id", "a", "b", "c"}, vals: []interface{}{int64(1), getDate("2019-10-29"), float64(4.444), float64(5.44444)}}},
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
		// test with different timezone
		{
			name: "Data conversion:  text, timestamp, datetime, varchar",
			input: `
	SET TIME_ZONE='+02:30';
	CREATE TABLE test (id integer PRIMARY KEY, a text, b timestamp, c datetime, d varchar(15));
	INSERT INTO test (id, a, b, c, d) VALUES (1, 'my text', '2019-10-29 05:30:00', '2019-10-29 05:30:00', 'my varchar');
	`,
			expectedData: []spannerData{
				spannerData{table: "test", cols: []string{"id", "a", "b", "c", "d"}, vals: []interface{}{int64(1), "my text", getTime(t, "2019-10-29T05:30:00+02:30"), getTimeWithoutTimezone(t, "2019-10-29 05:30:00"), "my varchar"}}},
		},
	}
	for _, tc := range multiColTests {
		t.Run(tc.name, func(t *testing.T) {
			conv, rows := runProcessMySQLDump(tc.input)
			if !tc.expectIssues {
				noIssues(conv, t, tc.name)
			}
			if tc.expectedSchema != nil {
				assert.Equal(t, tc.expectedSchema, stripSchemaComments(conv.SpSchema), tc.name+": Schema did not match")
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
			input: "CREATE TABLE test (a int, b float, c bool, d date, e blob, f set('42','6'));\n" +
				`INSERT INTO test (a, b, c, d, e, f) VALUES (7,42.1,1,'2019-10-29',_binary '` + string([]byte{137, 80}) + `','42,6');` + // Baseline (good)
				"INSERT INTO test (a, b, c, d, e, f) VALUES (7,NULL,NULL,NULL,NULL,NULL);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (7.1,NULL,NULL,NULL,NULL,NULL);\n" + // Error
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,42.1,NULL,NULL,NULL,NULL);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,'42-1',NULL,NULL,NULL,NULL);\n" + // Error
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,true,NULL,NULL,NULL);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,'truefalse',NULL,NULL,NULL);\n" + // Error
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,'2019-10-29',NULL,NULL);\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,'2019-10-42',NULL,NULL);\n" + // Error
				`INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,NULL,_binary '` + string([]byte{137, 80}) + `',NULL);` + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,NULL,NULL,'42,6');\n" + // Good
				"INSERT INTO test (a, b, c, d, e, f) VALUES (NULL,NULL,NULL,NULL,NULL,42,6);\n", // Error
			expectedData: []spannerData{
				spannerData{
					table: "test", cols: []string{"a", "b", "c", "d", "e", "f", "synth_id"},
					vals: []interface{}{int64(7), float64(42.1), true,
						getDate("2019-10-29"), []byte{0x89, 0x50},
						[]spanner.NullString{{StringVal: "42", Valid: true}, {StringVal: "6", Valid: true}},
						bitReverse(0)}},
				spannerData{table: "test", cols: []string{"a", "synth_id"}, vals: []interface{}{int64(7), bitReverse(1)}},
				spannerData{table: "test", cols: []string{"b", "synth_id"}, vals: []interface{}{float64(42.1), bitReverse(2)}},
				spannerData{table: "test", cols: []string{"c", "synth_id"}, vals: []interface{}{true, bitReverse(3)}},
				spannerData{table: "test", cols: []string{"d", "synth_id"}, vals: []interface{}{getDate("2019-10-29"), bitReverse(4)}},
				spannerData{table: "test", cols: []string{"e", "synth_id"}, vals: []interface{}{[]byte{0x89, 0x50}, bitReverse(5)}},
				spannerData{table: "test", cols: []string{"f", "synth_id"},
					vals: []interface{}{[]spanner.NullString{{StringVal: "42", Valid: true}, {StringVal: "6", Valid: true}}, bitReverse(6)}},
			},
		},
	}
	for _, tc := range dataErrorTests {
		conv, rows := runProcessMySQLDump(tc.input)
		assert.Equal(t, tc.expectedData, rows, tc.name+": Data rows did not match")
		assert.Equal(t, conv.BadRows(), int64(5), tc.name+": Error count did not match")
	}
}

// The following test Conv API calls based on data generated by ProcessMySQLDump.
func TestProcessMySQLDump_GetDDL(t *testing.T) {
	conv, _ := runProcessMySQLDump("CREATE TABLE cart (productid text, userid text, quantity bigint);\n" +
		"ALTER TABLE cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);")
	expected := "CREATE TABLE cart (\n" +
		"productid STRING(MAX) NOT NULL,\n" +
		"userid STRING(MAX) NOT NULL,\n" +
		"quantity INT64\n" +
		") PRIMARY KEY (productid, userid)"
	c := ddl.Config{}
	// normalizeSpace isn't perfect, but it handles most of the
	// usual discretionary space issues.
	assert.Equal(t, normalizeSpace(expected), normalizeSpace(strings.Join(conv.GetDDL(c), " ")))
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
					Name:     "cart",
					ColNames: []string{"productid", "userid", "quantity", "synth_id"},
					ColDefs: map[string]ddl.ColumnDef{
						"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
						"synth_id":  ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}}}},
		},
		{
			name:  "synth_id clash",
			input: "CREATE TABLE test (synth_id text, synth_id0 text, synth_id1 bigint);",
			expectedSchema: map[string]ddl.CreateTable{
				"test": ddl.CreateTable{
					Name:     "test",
					ColNames: []string{"synth_id", "synth_id0", "synth_id1", "synth_id2"},
					ColDefs: map[string]ddl.ColumnDef{
						"synth_id":  ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id0": ddl.ColumnDef{Name: "synth_id0", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						"synth_id1": ddl.ColumnDef{Name: "synth_id1", T: ddl.Type{Name: ddl.Int64}},
						"synth_id2": ddl.ColumnDef{Name: "synth_id2", T: ddl.Type{Name: ddl.Int64}},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id2"}}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conv, _ := runProcessMySQLDump(tc.input)
			conv.AddPrimaryKeys()
			if tc.expectedSchema != nil {
				assert.Equal(t, tc.expectedSchema, stripSchemaComments(conv.SpSchema), tc.name+": Schema did not match")
			}
		})
	}
}

func runProcessMySQLDump(s string) (*internal.Conv, []spannerData) {
	conv := internal.MakeConv()
	conv.SetLocation(time.UTC)
	conv.SetSchemaMode()
	ProcessMySQLDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil))
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(func(table string, cols []string, vals []interface{}) {
		rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
	})
	ProcessMySQLDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil))
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

func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func bitReverse(i int64) int64 {
	return int64(bits.Reverse64(uint64(i)))
}
