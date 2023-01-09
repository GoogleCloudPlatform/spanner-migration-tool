// Copyright 2022 Google LLC
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

package oracle

import (
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "test"
	srcSchema := schema.Table{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		ColDefs: map[string]schema.Column{
			"a": {Name: "a", Type: schema.Type{Name: "NUMBER"}},
			"b": {Name: "b", Type: schema.Type{Name: "FLOAT"}},
			"c": {Name: "c", Type: schema.Type{Name: "BFILE"}},
			"d": {Name: "d", Type: schema.Type{Name: "VARCHAR2", Mods: []int64{20}}},
			"e": {Name: "e", Type: schema.Type{Name: "DATE"}},
			"f": {Name: "f", Type: schema.Type{Name: "TIMESTAMP"}},
			"g": {Name: "g", Type: schema.Type{Name: "LONG"}},
			"h": {Name: "h", Type: schema.Type{Name: "NUMBER", Mods: []int64{13}}},
		},
		PrimaryKeys: []schema.Key{{Column: "a"}},
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			{Name: "fk_fake", Columns: []string{"x"}, ReferTable: "ref_table", ReferColumns: []string{"x"}},
			{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aRef"}}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{Column: "a", Desc: false}, {Column: "d", Desc: true}}},
			{Name: "index_with_0_key", Unique: true, Keys: []schema.Key{{Column: "m", Desc: false}, {Column: "y", Desc: true}}},
		},
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
			"aref": {Name: "aref", T: ddl.Type{Name: ddl.Numeric}},
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
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.Numeric}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"d": {Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(20)}},
			"e": {Name: "e", T: ddl.Type{Name: ddl.Date}},
			"f": {Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
			"g": {Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"h": {Name: "h", T: ddl.Type{Name: ddl.Int64}},
		},
		Pks: []ddl.IndexKey{{Col: "a"}},
		Fks: []ddl.Foreignkey{{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aref"}}},
		Indexes: []ddl.CreateIndex{{Name: "index1", Table: name, Unique: true, Keys: []ddl.IndexKey{{Col: "a", Desc: false}, {Col: "d", Desc: true}}},
			{Name: "index_with_0_key", Table: name, Unique: true, Keys: nil}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{}
	assert.Equal(t, expectedIssues, conv.Issues[name])
	// 1 FK issue, 2 index col not found
	assert.Equal(t, int64(3), conv.Unexpecteds())
}

// This is just a very basic smoke-test for toExperimentalSpannerType.
func TestToExperimentalSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	conv.TargetDb = constants.TargetExperimentalPostgres
	name := "test"
	srcSchema := schema.Table{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
		ColDefs: map[string]schema.Column{
			"a": {Name: "a", Type: schema.Type{Name: "NUMBER"}},
			"b": {Name: "b", Type: schema.Type{Name: "FLOAT"}},
			"c": {Name: "c", Type: schema.Type{Name: "BFILE"}},
			"d": {Name: "d", Type: schema.Type{Name: "VARCHAR2", Mods: []int64{20}}},
			"e": {Name: "e", Type: schema.Type{Name: "DATE"}},
			"f": {Name: "f", Type: schema.Type{Name: "TIMESTAMP"}},
			"g": {Name: "g", Type: schema.Type{Name: "LONG"}},
			"h": {Name: "h", Type: schema.Type{Name: "NUMBER", Mods: []int64{13}}},
			"i": {Name: "i", Type: schema.Type{Name: "JSON"}},
			"j": {Name: "j", Type: schema.Type{Name: "NCLOB"}},
			"k": {Name: "k", Type: schema.Type{Name: "XMLTYPE"}},
		},
		PrimaryKeys: []schema.Key{{Column: "a"}},
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			{Name: "fk_fake", Columns: []string{"x"}, ReferTable: "ref_table", ReferColumns: []string{"x"}},
			{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aRef"}}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{Column: "a", Desc: false}, {Column: "d", Desc: true}}},
			{Name: "index_with_0_key", Unique: true, Keys: []schema.Key{{Column: "m", Desc: false}, {Column: "y", Desc: true}}},
		},
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
			"aref": {Name: "aref", T: ddl.Type{Name: ddl.Numeric}},
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
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.Numeric}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"d": {Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(20)}},
			"e": {Name: "e", T: ddl.Type{Name: ddl.Date}},
			"f": {Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
			"g": {Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"h": {Name: "h", T: ddl.Type{Name: ddl.Int64}},
			"i": {Name: "i", T: ddl.Type{Name: ddl.JSONB}},
			"j": {Name: "j", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"k": {Name: "k", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},
		Pks: []ddl.IndexKey{{Col: "a"}},
		Fks: []ddl.Foreignkey{{Name: "fk_test", Columns: []string{"d"}, ReferTable: "ref_table", ReferColumns: []string{"dref"}},
			{Name: "fk_test2", Columns: []string{"a"}, ReferTable: "ref_table2", ReferColumns: []string{"aref"}}},
		Indexes: []ddl.CreateIndex{{Name: "index1", Table: name, Unique: true, Keys: []ddl.IndexKey{{Col: "a", Desc: false}, {Col: "d", Desc: true}}},
			{Name: "index_with_0_key", Table: name, Unique: true, Keys: nil}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{}
	assert.Equal(t, expectedIssues, conv.Issues[name])
	// 1 FK issue, 2 index col not found
	assert.Equal(t, int64(3), conv.Unexpecteds())
}

func dropComments(t *ddl.CreateTable) {
	t.Comment = ""
	for _, c := range t.ColNames {
		cd := t.ColDefs[c]
		cd.Comment = ""
		t.ColDefs[c] = cd
	}
}
