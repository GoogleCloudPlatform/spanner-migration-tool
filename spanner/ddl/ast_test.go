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

package ddl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	check(t, "BOOL", Bool{}.PrintScalarType())
	check(t, "INT64", Int64{}.PrintScalarType())
	check(t, "FLOAT64", Float64{}.PrintScalarType())
	check(t, "STRING(MAX)", String{MaxLength{}}.PrintScalarType())
	check(t, "STRING(42)", String{Int64Length{42}}.PrintScalarType())
	check(t, "BYTES(MAX)", Bytes{MaxLength{}}.PrintScalarType())
	check(t, "BYTES(42)", Bytes{Int64Length{42}}.PrintScalarType())
	check(t, "DATE", Date{}.PrintScalarType())
	check(t, "TIMESTAMP", Timestamp{}.PrintScalarType())
}

func TestColumnDef(t *testing.T) {
	c := Config{ProtectIds: false}
	check(t, "col1 INT64", pr(c, ColumnDef{Name: "col1", T: Int64{}}))
	check(t, "col1 ARRAY<INT64>", pr(c, ColumnDef{Name: "col1", T: Int64{}, IsArray: true}))
	check(t, "col1 INT64 NOT NULL", pr(c, ColumnDef{Name: "col1", T: Int64{}, NotNull: true}))
	check(t, "col1 ARRAY<INT64> NOT NULL", pr(c, ColumnDef{Name: "col1", T: Int64{}, IsArray: true, NotNull: true}))
	c = Config{ProtectIds: true}
	check(t, "`col1` INT64", pr(c, ColumnDef{Name: "col1", T: Int64{}}))
}

func TestIndexKey(t *testing.T) {
	c := Config{ProtectIds: false}
	check(t, "col1", IndexKey{Col: "col1"}.PrintIndexKey(c))
	check(t, "col1 DESC", IndexKey{Col: "col1", Desc: true}.PrintIndexKey(c))
	c = Config{ProtectIds: true}
	check(t, "`col1`", IndexKey{Col: "col1"}.PrintIndexKey(c))
}

func TestCreateTable(t *testing.T) {
	cds := make(map[string]ColumnDef)
	cds["col1"] = ColumnDef{Name: "col1", T: Int64{}, NotNull: true}
	cds["col2"] = ColumnDef{Name: "col2", T: String{MaxLength{}}, NotNull: false}
	cds["col3"] = ColumnDef{Name: "col3", T: Bytes{Int64Length{42}}, NotNull: false}
	ct := CreateTable{
		"mytable",
		[]string{"col1", "col2", "col3"},
		cds,
		[]IndexKey{IndexKey{Col: "col1", Desc: true}},
		"",
	}
	c := Config{ProtectIds: false}
	check(t, "CREATE TABLE mytable (col1 INT64 NOT NULL, col2 STRING(MAX), col3 BYTES(42)) PRIMARY KEY (col1 DESC)", ct.PrintCreateTable(c))
	c = Config{ProtectIds: true}
	check(t, "CREATE TABLE `mytable` (`col1` INT64 NOT NULL, `col2` STRING(MAX), `col3` BYTES(42)) PRIMARY KEY (`col1` DESC)", ct.PrintCreateTable(c))
}

func TestCreateIndex(t *testing.T) {
	ci := CreateIndex{
		"myindex",
		"mytable",
		[]IndexKey{IndexKey{Col: "col1", Desc: true}, IndexKey{Col: "col2"}},
	}
	c := Config{ProtectIds: false}
	check(t, "CREATE INDEX myindex ON mytable (col1 DESC, col2)", ci.PrintCreateIndex(c))
	c = Config{ProtectIds: true}
	check(t, "CREATE INDEX `myindex` ON `mytable` (`col1` DESC, `col2`)", ci.PrintCreateIndex(c))
}

func normalizeSpace(s string) string {
	// Insert whitespace around parenthesis and commas.
	s = strings.ReplaceAll(s, ")", " ) ")
	s = strings.ReplaceAll(s, "(", " ( ")
	s = strings.ReplaceAll(s, ",", " , ")
	return strings.Join(strings.Fields(s), " ")
}

// check verifies that actual and expected are the same, ignoring
// variations in whitespace.
func check(t *testing.T, expected, actual string) {
	assert.Equal(t, normalizeSpace(expected), normalizeSpace(actual))
}

func pr(c Config, cd ColumnDef) string {
	s, _ := cd.PrintColumnDef(c)
	return s
}
