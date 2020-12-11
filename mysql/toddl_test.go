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
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

// This is just a very basic smoke-test for toSpannerType.
func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "test"
	srcSchema := schema.Table{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f"},
		ColDefs: map[string]schema.Column{
			"a": schema.Column{Name: "a", Type: schema.Type{Name: "int"}},
			"b": schema.Column{Name: "b", Type: schema.Type{Name: "float"}},
			"c": schema.Column{Name: "c", Type: schema.Type{Name: "tinyint", Mods: []int64{1}}},
			"d": schema.Column{Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"e": schema.Column{Name: "e", Type: schema.Type{Name: "double"}},
			"f": schema.Column{Name: "f", Type: schema.Type{Name: "timestamp"}},
		},
		PrimaryKeys: []schema.Key{schema.Key{Column: "a"}},
		ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"b"}}},
	}
	conv.SrcSchema[name] = srcSchema
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
			"e": ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Float64}},
			"f": ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"b"}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"a": []internal.SchemaIssue{internal.Widened},
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
