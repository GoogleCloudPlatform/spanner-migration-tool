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
	"harbourbridge/spanner/ddl"
	"strings"
	"testing"

	pg_query "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"github.com/stretchr/testify/assert"
)

// This file contains very basic tests of Cvt API functionality.
// Most of the Cvt APIs are also tested in process_test.go (where
// they are tested using data from schema/data conversion).

func TestSetSchemaMode(t *testing.T) {
	cvt := MakeCvt()
	cvt.SetSchemaMode()
	assert.True(t, cvt.schemaMode())
	assert.False(t, cvt.dataMode())
}

func TestSetDataMode(t *testing.T) {
	cvt := MakeCvt()
	cvt.SetDataMode()
	assert.False(t, cvt.schemaMode())
	assert.True(t, cvt.dataMode())
}

func TestGetDDL(t *testing.T) {
	cvt := MakeCvt()
	cvt.sSchema["table1"] = ddl.CreateTable{
		"table1",
		[]string{"a", "b"},
		map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Float64{}},
		},
		[]ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		"",
	}
	cvt.sSchema["table2"] = ddl.CreateTable{
		"table2",
		[]string{"a"},
		map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
		},
		[]ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		"",
	}
	ddl := cvt.GetDDL(ddl.Config{})
	assert.Equal(t, 2, len(ddl))
	normalize := func(s string) string {
		return strings.Join(strings.Fields(s), " ")
	}
	if len(ddl) >= 2 {
		e0 := "CREATE TABLE table1 ( a INT64, b FLOAT64 ) PRIMARY KEY (a)"
		e1 := "CREATE TABLE table2 ( a INT64 ) PRIMARY KEY (a)"
		assert.Equal(t, normalize(e0), normalize(ddl[0]))
		assert.Equal(t, normalize(e1), normalize(ddl[1]))
	}
}

func TestRows(t *testing.T) {
	cvt := MakeCvt()
	cvt.stats.rows["table1"] = 42
	cvt.stats.rows["table2"] = 6
	assert.Equal(t, int64(48), cvt.Rows())
}

func TestBadRows(t *testing.T) {
	cvt := MakeCvt()
	cvt.stats.badRows["table1"] = 6
	cvt.stats.badRows["table2"] = 2
	assert.Equal(t, int64(8), cvt.BadRows())
}

func TestStatements(t *testing.T) {
	cvt := MakeCvt()
	cvt.errorInStatement(parse(t, "CREATE TABLE cart (pid text NOT NULL);"))
	cvt.schemaStatement(parse(t, "CREATE TABLE cart (pid text NOT NULL);"))
	cvt.dataStatement(parse(t, "INSERT INTO cart (pid) VALUES ('p42');"))
	cvt.skipStatement(parse(t, "GRANT ALL ON SCHEMA public TO PUBLIC;"))
	assert.Equal(t, int64(4), cvt.Statements())
}

func TestStatementErrors(t *testing.T) {
	cvt := MakeCvt()
	cvt.errorInStatement(parse(t, "CREATE TABLE cart (pid text NOT NULL);"))
	assert.Equal(t, int64(1), cvt.StatementErrors())
}

func TestUnexpecteds(t *testing.T) {
	cvt := MakeCvt()
	cvt.unexpected("expected-the-unexpected")
	assert.Equal(t, int64(1), cvt.Unexpecteds())
}

func TestGetBadRows(t *testing.T) {
	cvt := MakeCvt()
	row1 := row{"table", []string{"col1", "col2"}, []string{"a", "1"}}
	row2 := row{"table", []string{"col1"}, []string{"bb"}}
	cvt.sampleBadRows.l = []*row{&row1, &row2}
	assert.Equal(t, 2, len(cvt.SampleBadRows(100)))
}

func TestAddPrimaryKeys(t *testing.T) {
	cvt := MakeCvt()
	cvt.sSchema["table"] = ddl.CreateTable{
		"table",
		[]string{"a", "b"},
		map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Float64{}},
		},
		[]ddl.IndexKey{},
		"",
	}
	cvt.AddPrimaryKeys()
	e := ddl.CreateTable{
		"table",
		[]string{"a", "b", "synth_id"},
		map[string]ddl.ColumnDef{
			"a":        ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
			"b":        ddl.ColumnDef{Name: "b", T: ddl.Float64{}},
			"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Int64{}},
		},
		[]ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
		"",
	}
	assert.Equal(t, e, cvt.sSchema["table"])
	assert.Equal(t, syntheticPKey{col: "synth_id", sequence: 0}, cvt.syntheticPKeys["table"])
}

func parse(t *testing.T, s string) []nodes.Node {
	tree, err := pg_query.Parse(s)
	assert.Nil(t, err, "Failed to parse")
	return tree.Statements
}
