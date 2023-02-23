// Copyright 2021 Google LLC
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

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
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
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"},
		ColDefs: map[string]schema.Column{
			"a": {Name: "a", Type: schema.Type{Name: "int"}},
			"b": {Name: "b", Type: schema.Type{Name: "float"}},
			"c": {Name: "c", Type: schema.Type{Name: "tinyint", Mods: []int64{1}}},
			"d": {Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"e": {Name: "e", Type: schema.Type{Name: "numeric"}},
			"f": {Name: "f", Type: schema.Type{Name: "timestamp"}},
			"g": {Name: "g", Type: schema.Type{Name: "binary", Mods: []int64{4000}}},
			"h": {Name: "h", Type: schema.Type{Name: "date"}},
			"i": {Name: "i", Type: schema.Type{Name: "money"}},
			"j": {Name: "j", Type: schema.Type{Name: "smalldatetime"}},
			"k": {Name: "k", Type: schema.Type{Name: "nvarchar", Mods: []int64{50}}},
			"l": {Name: "l", Type: schema.Type{Name: "image"}},
			"m": {Name: "m", Type: schema.Type{Name: "geometry"}},
			"n": {Name: "n", Type: schema.Type{Name: "bit"}},
			"o": {Name: "o", Type: schema.Type{Name: "uniqueidentifier"}},
		},
		PrimaryKeys: []schema.Key{{Column: "a"}},
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aRef"}}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{Column: "a", Desc: false}, {Column: "d", Desc: true}}}},
	}
	conv.SrcSchema[name] = srcSchema
	conv.SpSchema["ref_table"] = ddl.CreateTable{
		Name:     "ref_table",
		ColNames: []string{"dref", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"dref": {Name: "dref", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"b":    {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c":    {Name: "c", T: ddl.Type{Name: ddl.Bool}},
		},
		Pks: []ddl.IndexKey{{Col: "dref"}},
	}
	conv.SpSchema["ref_table2"] = ddl.CreateTable{
		Name:     "ref_table2",
		ColNames: []string{"aref", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"aref": {Name: "aref", T: ddl.Type{Name: ddl.Int64}},
			"b":    {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c":    {Name: "c", T: ddl.Type{Name: ddl.Bool}},
		},
		Pks: []ddl.IndexKey{{Col: "aref"}},
	}
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[name]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.Int64}},
			"d": {Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"e": {Name: "e", T: ddl.Type{Name: ddl.Numeric}},
			"f": {Name: "f", T: ddl.Type{Name: ddl.Int64}},
			"g": {Name: "g", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"h": {Name: "h", T: ddl.Type{Name: ddl.Date}},
			"i": {Name: "i", T: ddl.Type{Name: ddl.Numeric}},
			"j": {Name: "j", T: ddl.Type{Name: ddl.Timestamp}},
			"k": {Name: "k", T: ddl.Type{Name: ddl.String, Len: int64(50)}},
			"l": {Name: "l", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"m": {Name: "m", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"n": {Name: "n", T: ddl.Type{Name: ddl.Bool}},
			"o": {Name: "o", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},

		Pks: []ddl.IndexKey{{Col: "a"}},
		Fks: []ddl.Foreignkey{{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aref"}}},
		Indexes: []ddl.CreateIndex{{Name: "index1", Table: name, Unique: true, Keys: []ddl.IndexKey{{Col: "a", Desc: false}, {Col: "d", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"a": {internal.Widened},
		"b": {internal.Widened},
		"c": {internal.Widened},
		"j": {internal.Timestamp},
		"m": {internal.NoGoodType},
	}
	assert.Equal(t, expectedIssues, conv.Issues[name])
}

// This is just a very basic smoke-test for toExperimentalSpannerType.
func TestToExperimentalSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	conv.SpDialect = constants.DIALECT_POSTGRESQL
	name := "test"
	srcSchema := schema.Table{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"},
		ColDefs: map[string]schema.Column{
			"a": {Name: "a", Type: schema.Type{Name: "int"}},
			"b": {Name: "b", Type: schema.Type{Name: "float"}},
			"c": {Name: "c", Type: schema.Type{Name: "tinyint", Mods: []int64{1}}},
			"d": {Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"e": {Name: "e", Type: schema.Type{Name: "numeric"}},
			"f": {Name: "f", Type: schema.Type{Name: "timestamp"}},
			"g": {Name: "g", Type: schema.Type{Name: "binary", Mods: []int64{4000}}},
			"h": {Name: "h", Type: schema.Type{Name: "date"}},
			"i": {Name: "i", Type: schema.Type{Name: "money"}},
			"j": {Name: "j", Type: schema.Type{Name: "smalldatetime"}},
			"k": {Name: "k", Type: schema.Type{Name: "nvarchar", Mods: []int64{50}}},
			"l": {Name: "l", Type: schema.Type{Name: "image"}},
			"m": {Name: "m", Type: schema.Type{Name: "geometry"}},
			"n": {Name: "n", Type: schema.Type{Name: "bit"}},
			"o": {Name: "o", Type: schema.Type{Name: "uniqueidentifier"}},
		},
		PrimaryKeys: []schema.Key{{Column: "a"}},
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aRef"}}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{Column: "a", Desc: false}, {Column: "d", Desc: true}}}},
	}
	conv.SrcSchema[name] = srcSchema
	conv.SpSchema["ref_table"] = ddl.CreateTable{
		Name:     "ref_table",
		ColNames: []string{"dref", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"dref": {Name: "dref", T: ddl.Type{Name: ddl.PGVarchar, Len: int64(6)}},
			"b":    {Name: "b", T: ddl.Type{Name: ddl.PGFloat8}},
			"c":    {Name: "c", T: ddl.Type{Name: ddl.PGBool}},
		},
		Pks: []ddl.IndexKey{{Col: "dref"}},
	}
	conv.SpSchema["ref_table2"] = ddl.CreateTable{
		Name:     "ref_table2",
		ColNames: []string{"aref", "b", "c"},
		ColDefs: map[string]ddl.ColumnDef{
			"aref": {Name: "aref", T: ddl.Type{Name: ddl.PGInt8}},
			"b":    {Name: "b", T: ddl.Type{Name: ddl.PGFloat8}},
			"c":    {Name: "c", T: ddl.Type{Name: ddl.PGBool}},
		},
		Pks: []ddl.IndexKey{{Col: "aref"}},
	}
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[name]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.PGInt8}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.PGFloat8}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.PGInt8}},
			"d": {Name: "d", T: ddl.Type{Name: ddl.PGVarchar, Len: int64(6)}},
			"e": {Name: "e", T: ddl.Type{Name: ddl.PGNumeric}},
			"f": {Name: "f", T: ddl.Type{Name: ddl.PGInt8}},
			"g": {Name: "g", T: ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}},
			"h": {Name: "h", T: ddl.Type{Name: ddl.PGDate}},
			"i": {Name: "i", T: ddl.Type{Name: ddl.PGNumeric}},
			"j": {Name: "j", T: ddl.Type{Name: ddl.PGTimestamptz}},
			"k": {Name: "k", T: ddl.Type{Name: ddl.PGVarchar, Len: int64(50)}},
			"l": {Name: "l", T: ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}},
			"m": {Name: "m", T: ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}},
			"n": {Name: "n", T: ddl.Type{Name: ddl.PGBool}},
			"o": {Name: "o", T: ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}},
		},

		Pks: []ddl.IndexKey{{Col: "a"}},
		Fks: []ddl.Foreignkey{{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aref"}}},
		Indexes: []ddl.CreateIndex{{Name: "index1", Table: name, Unique: true, Keys: []ddl.IndexKey{{Col: "a", Desc: false}, {Col: "d", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"a": {internal.Widened},
		"b": {internal.Widened},
		"c": {internal.Widened},
		"j": {internal.Timestamp},
		"m": {internal.NoGoodType},
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
