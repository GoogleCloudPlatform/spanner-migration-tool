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

package sqlserver

import (
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
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
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"},
		ColDefs: map[string]schema.Column{
			"a": schema.Column{Name: "a", Type: schema.Type{Name: "int"}},
			"b": schema.Column{Name: "b", Type: schema.Type{Name: "float"}},
			"c": schema.Column{Name: "c", Type: schema.Type{Name: "tinyint", Mods: []int64{1}}},
			"d": schema.Column{Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"e": schema.Column{Name: "e", Type: schema.Type{Name: "numeric"}},
			"f": schema.Column{Name: "f", Type: schema.Type{Name: "timestamp"}},
			"g": schema.Column{Name: "g", Type: schema.Type{Name: "binary", Mods: []int64{4000}}},
			"h": schema.Column{Name: "h", Type: schema.Type{Name: "date"}},
			"i": schema.Column{Name: "i", Type: schema.Type{Name: "money"}},
			"j": schema.Column{Name: "j", Type: schema.Type{Name: "smalldatetime"}},
			"k": schema.Column{Name: "k", Type: schema.Type{Name: "nvarchar", Mods: []int64{50}}},
			"l": schema.Column{Name: "l", Type: schema.Type{Name: "image"}},
			"m": schema.Column{Name: "m", Type: schema.Type{Name: "geometry"}},
		},
		PrimaryKeys: []schema.Key{schema.Key{Column: "a"}},
		ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			schema.ForeignKey{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aRef"}}},
		Indexes: []schema.Index{schema.Index{Name: "index1", Unique: true, Keys: []schema.Key{schema.Key{Column: "a", Desc: false}, schema.Key{Column: "d", Desc: true}}}},
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
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[name]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
			"d": ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"e": ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Numeric}},
			"f": ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
			"g": ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.Bytes, Len: int64(4000)}},
			"h": ddl.ColumnDef{Name: "h", T: ddl.Type{Name: ddl.Date}},
			"i": ddl.ColumnDef{Name: "i", T: ddl.Type{Name: ddl.Numeric}},
			"j": ddl.ColumnDef{Name: "j", T: ddl.Type{Name: ddl.Timestamp}},
			"k": ddl.ColumnDef{Name: "k", T: ddl.Type{Name: ddl.String, Len: int64(50)}},
			"l": ddl.ColumnDef{Name: "l", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"m": ddl.ColumnDef{Name: "m", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},

		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			ddl.Foreignkey{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aref"}}},
		Indexes: []ddl.CreateIndex{ddl.CreateIndex{Name: "index1", Table: name, Unique: true, Keys: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "d", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"a": []internal.SchemaIssue{internal.Widened},
		"b": []internal.SchemaIssue{internal.Widened},
		"c": []internal.SchemaIssue{internal.Widened},
		"f": []internal.SchemaIssue{internal.Timestamp},
		"j": []internal.SchemaIssue{internal.Timestamp},
		"m": []internal.SchemaIssue{internal.NoGoodType},
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
