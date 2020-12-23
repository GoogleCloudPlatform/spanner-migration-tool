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

package internal

import (
	"strings"
	"testing"

	pg_query "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"github.com/stretchr/testify/assert"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// This file contains very basic tests of Conv API functionality.
// Most of the Conv APIs are also tested in process_test.go (where
// they are tested using data from schema/data conversion).

func TestSetSchemaMode(t *testing.T) {
	conv := MakeConv()
	conv.SetSchemaMode()
	assert.True(t, conv.SchemaMode())
	assert.False(t, conv.DataMode())
}

func TestSetDataMode(t *testing.T) {
	conv := MakeConv()
	conv.SetDataMode()
	assert.False(t, conv.SchemaMode())
	assert.True(t, conv.DataMode())
}

func TestGetDDL(t *testing.T) {
	conv := MakeConv()
	conv.SpSchema["table1"] = ddl.CreateTable{
		Name:     "table1",
		ColNames: []string{"a", "b"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}}
	conv.SpSchema["table2"] = ddl.CreateTable{
		Name:     "table2",
		ColNames: []string{"a"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}}}
	conv.SpSchema["table3"] = ddl.CreateTable{
		Name:     "table3",
		ColNames: []string{"a", "b"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"b"}, ReferTable: "ref_table1", ReferColumns: []string{"ref_c"}}},
	}
	conv.SpSchema["table4"] = ddl.CreateTable{
		Name:     "table4",
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
		},
		Pks:    []ddl.IndexKey{ddl.IndexKey{Col: "a"}, ddl.IndexKey{Col: "b"}},
		Fks:    []ddl.Foreignkey{ddl.Foreignkey{Name: "fk2", Columns: []string{"c"}, ReferTable: "ref_table2", ReferColumns: []string{"ref_c"}}},
		Parent: "table1",
	}
	ddl := conv.GetDDL(ddl.Config{ForeignKeys: true})
	normalize := func(l []string) (nl []string) {
		for _, s := range l {
			nl = append(nl, strings.Join(strings.Fields(s), " "))
		}
		return nl
	}
	e := []string{
		"CREATE TABLE table1 ( a INT64, b FLOAT64 ) PRIMARY KEY (a)",
		"CREATE TABLE table2 ( a INT64 ) PRIMARY KEY (a)",
		"CREATE TABLE table3 ( a INT64, b INT64 ) PRIMARY KEY (a)",
		"CREATE TABLE table4 ( a INT64, b INT64, c INT64 ) PRIMARY KEY (a, b), INTERLEAVE IN PARENT table1 ON DELETE CASCADE",
		"ALTER TABLE table3 ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES ref_table1 (ref_c)",
		"ALTER TABLE table4 ADD CONSTRAINT fk2 FOREIGN KEY (c) REFERENCES ref_table2 (ref_c)",
	}
	assert.ElementsMatch(t, normalize(e), normalize(ddl))
}

func TestRows(t *testing.T) {
	conv := MakeConv()
	conv.Stats.Rows["table1"] = 42
	conv.Stats.Rows["table2"] = 6
	assert.Equal(t, int64(48), conv.Rows())
}

func TestBadRows(t *testing.T) {
	conv := MakeConv()
	conv.Stats.BadRows["table1"] = 6
	conv.Stats.BadRows["table2"] = 2
	assert.Equal(t, int64(8), conv.BadRows())
}

func TestStatements(t *testing.T) {
	conv := MakeConv()
	conv.ErrorInStatement("Error statement")
	conv.SchemaStatement("Schema statement")
	conv.DataStatement("Data statement")
	conv.SkipStatement("Skip statement")
	assert.Equal(t, int64(4), conv.Statements())
}

func TestStatementErrors(t *testing.T) {
	conv := MakeConv()
	conv.ErrorInStatement("Error statement")
	assert.Equal(t, int64(1), conv.StatementErrors())
}

func TestUnexpecteds(t *testing.T) {
	conv := MakeConv()
	conv.Unexpected("expected-the-unexpected")
	assert.Equal(t, int64(1), conv.Unexpecteds())
}

func TestGetBadRows(t *testing.T) {
	conv := MakeConv()
	row1 := row{"table", []string{"col1", "col2"}, []string{"a", "1"}}
	row2 := row{"table", []string{"col1"}, []string{"bb"}}
	conv.sampleBadRows.rows = []*row{&row1, &row2}
	assert.Equal(t, 2, len(conv.SampleBadRows(100)))
}

func TestAddPrimaryKeys(t *testing.T) {
	conv := MakeConv()
	conv.SpSchema["table"] = ddl.CreateTable{
		Name:     "table",
		ColNames: []string{"a", "b"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
		},
		Pks: []ddl.IndexKey{}}
	conv.AddPrimaryKeys()
	e := ddl.CreateTable{
		Name:     "table",
		ColNames: []string{"a", "b", "synth_id"},
		ColDefs: map[string]ddl.ColumnDef{
			"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}}}
	assert.Equal(t, e, conv.SpSchema["table"])
	assert.Equal(t, SyntheticPKey{Col: "synth_id", Sequence: 0}, conv.SyntheticPKeys["table"])
}

func parse(t *testing.T, s string) []nodes.Node {
	tree, err := pg_query.Parse(s)
	assert.Nil(t, err, "Failed to parse")
	return tree.Statements
}
