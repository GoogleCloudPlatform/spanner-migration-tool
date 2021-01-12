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
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

// This is just a very basic smoke-test for toSpannerType.
// The real testing of toSpannerType happens in process_test.go
// via the public API ProcessPgDump (see TestProcessPgDump).
func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "test"
	srcSchema := schema.Table{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f"},
		ColDefs: map[string]schema.Column{
			"a": schema.Column{Name: "a", Type: schema.Type{Name: "int8"}},
			"b": schema.Column{Name: "b", Type: schema.Type{Name: "float4"}},
			"c": schema.Column{Name: "c", Type: schema.Type{Name: "bool"}},
			"d": schema.Column{Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"e": schema.Column{Name: "e", Type: schema.Type{Name: "numeric"}},
			"f": schema.Column{Name: "f", Type: schema.Type{Name: "timestamptz"}},
		},
		PrimaryKeys: []schema.Key{schema.Key{Column: "a"}},
		ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			schema.ForeignKey{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aRef"}}},
		Indexes: []schema.Index{schema.Index{Name: "index1", Unique: true, Keys: []schema.Key{schema.Key{Column: "a", Desc: false}, schema.Key{Column: "d", Desc: true}}},
			schema.Index{Name: "index2", Unique: false, Keys: []schema.Key{schema.Key{Column: "d", Desc: true}}}},
	}
	conv.SrcSchema[name] = srcSchema
	conv.SpSchema["ref_table"] = ddl.CreateTable{
		Name:     "ref_table",
		ColNames: []string{"dref", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"dref": ddl.ColumnDef{Name: "dref", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"b":    ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c":    ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "dref"}},
	}
	conv.SpSchema["ref_table2"] = ddl.CreateTable{
		Name:     "ref_table2",
		ColNames: []string{"aref", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"aref": ddl.ColumnDef{Name: "aref", T: ddl.Type{Name: ddl.Int64}},
			"b":    ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c":    ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "aref"}},
	}
	assert.Nil(t, schemaToDDL(conv))
	actual := conv.SpSchema[name]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
			"d": ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"e": ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Numeric}},
			"f": ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			ddl.Foreignkey{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aref"}}},
		Indexes: []ddl.CreateIndex{ddl.CreateIndex{Name: "index1", Table: name, Unique: true, Keys: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "d", Desc: true}}},
			ddl.CreateIndex{Name: "index2", Table: name, Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{Col: "d", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"b": []internal.SchemaIssue{internal.Widened},
	}
	assert.Equal(t, expectedIssues, conv.Issues[name])
}

func dropComments(t *ddl.CreateTable) {
	t.Comment = ""
	for _, c := range t.ColNames {
		cd := t.ColDefs[c]
		cd.Comment = ""
		t.ColDefs[c] = cd
	}
}
