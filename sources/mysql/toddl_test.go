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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

// This is just a very basic smoke-test for toSpannerType.
func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "test"
	tableId := "t1"
	srcSchema := schema.Table{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		ColDefs: map[string]schema.Column{
			"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: "int"}},
			"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: "float"}},
			"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: "tinyint", Mods: []int64{1}}},
			"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
			"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: "timestamp"}},
			"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: "json"}},
			"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: "date"}},
			"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: "timestamp"}},
			"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: "bit"}},
		},
		PrimaryKeys: []schema.Key{schema.Key{ColId: "c1"}},
		ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c11"}},
			schema.ForeignKey{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c14"}}},
		Indexes: []schema.Index{schema.Index{Name: "index1", Unique: true, Keys: []schema.Key{schema.Key{ColId: "c1", Desc: false}, schema.Key{ColId: "c4", Desc: true}}}},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c11", "c12", "c13"},
		ColDefs: map[string]schema.Column{
			"c11": {Name: "dref", Id: "c11", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c12": {Name: "b", Id: "c12", Type: schema.Type{Name: "float"}},
			"c13": {Name: "c", Id: "c13", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c11"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c14", "c15", "c16"},
		ColDefs: map[string]schema.Column{
			"c14": {Name: "aref", Id: "c14", Type: schema.Type{Name: "int"}},
			"c15": {Name: "b", Id: "c15", Type: schema.Type{Name: "float"}},
			"c16": {Name: "c", Id: "c16", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c14"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2":  ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3":  ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
			"c4":  ddl.ColumnDef{Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"c5":  ddl.ColumnDef{Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
			"c6":  ddl.ColumnDef{Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}},
			"c7":  ddl.ColumnDef{Name: "g", Id: "c7", T: ddl.Type{Name: ddl.JSON}},
			"c8":  ddl.ColumnDef{Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Date}},
			"c9":  ddl.ColumnDef{Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Timestamp}},
			"c10": ddl.ColumnDef{Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		},
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1"}},
		ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c11"}},
			ddl.Foreignkey{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c14"}}},
		Indexes: []ddl.CreateIndex{ddl.CreateIndex{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false}, ddl.IndexKey{ColId: "c4", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"c1": []internal.SchemaIssue{internal.Widened},
		"c2": []internal.SchemaIssue{internal.Widened},
	}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId].ColumnLevelIssues)
	tableList, _ := common.GetIncludedSrcTablesFromConv(conv)
	assert.Equal(t, len(conv.SrcSchema), len(tableList))
}

// This is just a very basic smoke-test for toPostgreSQLDialectType.
func TestToSpannerPostgreSQLDialectType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	conv.SpDialect = constants.DIALECT_POSTGRESQL
	name := "test"
	tableId := "t1"
	srcSchema := schema.Table{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		ColDefs: map[string]schema.Column{
			"c1":  schema.Column{Name: "a", Id: "c1", Type: schema.Type{Name: "int"}},
			"c2":  schema.Column{Name: "b", Id: "c2", Type: schema.Type{Name: "float"}},
			"c3":  schema.Column{Name: "c", Id: "c3", Type: schema.Type{Name: "tinyint", Mods: []int64{1}}},
			"c4":  schema.Column{Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c5":  schema.Column{Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
			"c6":  schema.Column{Name: "f", Id: "c6", Type: schema.Type{Name: "timestamp"}},
			"c7":  schema.Column{Name: "g", Id: "c7", Type: schema.Type{Name: "json"}},
			"c8":  schema.Column{Name: "h", Id: "c8", Type: schema.Type{Name: "date"}},
			"c9":  schema.Column{Name: "i", Id: "c9", Type: schema.Type{Name: "timestamp"}},
			"c10": schema.Column{Name: "j", Id: "c10", Type: schema.Type{Name: "bit"}},
		},
		PrimaryKeys: []schema.Key{schema.Key{ColId: "c1"}},
		ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c11"}},
			schema.ForeignKey{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c14"}}},
		Indexes: []schema.Index{schema.Index{Name: "index1", Unique: true, Keys: []schema.Key{schema.Key{ColId: "c1", Desc: false}, schema.Key{ColId: "c4", Desc: true}}}},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c11", "c12", "c13"},
		ColDefs: map[string]schema.Column{
			"c11": {Name: "dref", Id: "c11", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c12": {Name: "b", Id: "c12", Type: schema.Type{Name: "float"}},
			"c13": {Name: "c", Id: "c13", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c11"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c14", "15", "c16"},
		ColDefs: map[string]schema.Column{
			"c14": {Name: "aref", Id: "c14", Type: schema.Type{Name: "int"}},
			"c15": {Name: "b", Id: "c15", Type: schema.Type{Name: "float"}},
			"c16": {Name: "c", Id: "c16", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c14"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2":  ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3":  ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
			"c4":  ddl.ColumnDef{Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"c5":  ddl.ColumnDef{Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
			"c6":  ddl.ColumnDef{Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}},
			"c7":  ddl.ColumnDef{Name: "g", Id: "c7", T: ddl.Type{Name: ddl.JSON}},
			"c8":  ddl.ColumnDef{Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Date}},
			"c9":  ddl.ColumnDef{Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Timestamp}},
			"c10": ddl.ColumnDef{Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
		},
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1"}},
		ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c11"}},
			ddl.Foreignkey{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c14"}}},
		Indexes: []ddl.CreateIndex{ddl.CreateIndex{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false}, ddl.IndexKey{ColId: "c4", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"c1": []internal.SchemaIssue{internal.Widened},
		"c2": []internal.SchemaIssue{internal.Widened},
	}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId].ColumnLevelIssues)
}

func dropComments(t *ddl.CreateTable) {
	t.Comment = ""
	for _, c := range t.ColIds {
		cd := t.ColDefs[c]
		cd.Comment = ""
		t.ColDefs[c] = cd
	}
}
