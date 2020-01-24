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

// This file contains very basic tests of Conv API functionality.
// Most of the Conv APIs are also tested in process_test.go (where
// they are tested using data from schema/data conversion).

func TestSetSchemaMode(t *testing.T) {
	conv := MakeConv()
	conv.SetSchemaMode()
	assert.True(t, conv.schemaMode())
	assert.False(t, conv.dataMode())
}

func TestSetDataMode(t *testing.T) {
	conv := MakeConv()
	conv.SetDataMode()
	assert.False(t, conv.schemaMode())
	assert.True(t, conv.dataMode())
}

func TestGetDDL(t *testing.T) {
	conv := MakeConv()
	conv.spSchema["table1"] = ddl.CreateTable{
		"table1",
		[]string{"a", "b"},
		map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Float64{}},
		},
		[]ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		"",
	}
	conv.spSchema["table2"] = ddl.CreateTable{
		"table2",
		[]string{"a"},
		map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
		},
		[]ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		"",
	}
	ddl := conv.GetDDL(ddl.Config{})
	normalize := func(l []string) (nl []string) {
		for _, s := range l {
			nl = append(nl, strings.Join(strings.Fields(s), " "))
		}
		return nl
	}
	e := []string{
		"CREATE TABLE table1 ( a INT64, b FLOAT64 ) PRIMARY KEY (a)",
		"CREATE TABLE table2 ( a INT64 ) PRIMARY KEY (a)",
	}
	assert.ElementsMatch(t, normalize(e), normalize(ddl))
}

func TestRows(t *testing.T) {
	conv := MakeConv()
	conv.stats.rows["table1"] = 42
	conv.stats.rows["table2"] = 6
	assert.Equal(t, int64(48), conv.Rows())
}

func TestBadRows(t *testing.T) {
	conv := MakeConv()
	conv.stats.badRows["table1"] = 6
	conv.stats.badRows["table2"] = 2
	assert.Equal(t, int64(8), conv.BadRows())
}

func TestStatements(t *testing.T) {
	conv := MakeConv()
	conv.errorInStatement(parse(t, "CREATE TABLE cart (pid text NOT NULL);"))
	conv.schemaStatement(parse(t, "CREATE TABLE cart (pid text NOT NULL);"))
	conv.dataStatement(parse(t, "INSERT INTO cart (pid) VALUES ('p42');"))
	conv.skipStatement(parse(t, "GRANT ALL ON SCHEMA public TO PUBLIC;"))
	assert.Equal(t, int64(4), conv.Statements())
}

func TestStatementErrors(t *testing.T) {
	conv := MakeConv()
	conv.errorInStatement(parse(t, "CREATE TABLE cart (pid text NOT NULL);"))
	assert.Equal(t, int64(1), conv.StatementErrors())
}

func TestUnexpecteds(t *testing.T) {
	conv := MakeConv()
	conv.unexpected("expected-the-unexpected")
	assert.Equal(t, int64(1), conv.Unexpecteds())
}

func TestGetBadRows(t *testing.T) {
	conv := MakeConv()
	row1 := row{"table", []string{"col1", "col2"}, []string{"a", "1"}}
	row2 := row{"table", []string{"col1"}, []string{"bb"}}
	conv.sampleBadRows.l = []*row{&row1, &row2}
	assert.Equal(t, 2, len(conv.SampleBadRows(100)))
}

func TestAddPrimaryKeys(t *testing.T) {
	conv := MakeConv()
	conv.spSchema["table"] = ddl.CreateTable{
		"table",
		[]string{"a", "b"},
		map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Float64{}},
		},
		[]ddl.IndexKey{},
		"",
	}
	conv.AddPrimaryKeys()
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
	assert.Equal(t, e, conv.spSchema["table"])
	assert.Equal(t, syntheticPKey{col: "synth_id", sequence: 0}, conv.syntheticPKeys["table"])
}

func parse(t *testing.T, s string) []nodes.Node {
	tree, err := pg_query.Parse(s)
	assert.Nil(t, err, "Failed to parse")
	return tree.Statements
}
