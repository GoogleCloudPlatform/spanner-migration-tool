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
	tableId := "t1"
	srcSchema := schema.Table{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15"},
		ColDefs: map[string]schema.Column{
			"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: "int"}},
			"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: "float"}},
			"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: "tinyint", Mods: []int64{1}}},
			"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
			"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: "timestamp"}},
			"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: "binary", Mods: []int64{4000}}},
			"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: "date"}},
			"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: "money"}},
			"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: "smalldatetime"}},
			"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: "nvarchar", Mods: []int64{50}}},
			"c12": {Name: "l", Id: "c12", Type: schema.Type{Name: "image"}},
			"c13": {Name: "m", Id: "c13", Type: schema.Type{Name: "geometry"}},
			"c14": {Name: "n", Id: "c14", Type: schema.Type{Name: "bit"}},
			"c15": {Name: "o", Id: "c15", Type: schema.Type{Name: "uniqueidentifier"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}},
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c16"}},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c19"}}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}}},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c16", "c17", "c18"},
		ColDefs: map[string]schema.Column{
			"c16": {Name: "dref", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c17": {Name: "b", Type: schema.Type{Name: "float"}},
			"c18": {Name: "c", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c16"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c19", "c20", "c21"},
		ColDefs: map[string]schema.Column{
			"c19": {Name: "aref", Type: schema.Type{Name: "int"}},
			"c20": {Name: "b", Type: schema.Type{Name: "float"}},
			"c21": {Name: "c", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c19"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
			"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
			"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Int64}},
			"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Date}},
			"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Numeric}},
			"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Timestamp}},
			"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.String, Len: int64(50)}},
			"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.Bool}},
			"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},

		PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
		ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c16"}},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c19"}}},
		Indexes: []ddl.CreateIndex{{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"c1":  {internal.Widened},
		"c2":  {internal.Widened},
		"c3":  {internal.Widened},
		"c10": {internal.Timestamp},
		"c13": {internal.NoGoodType},
	}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId])
}

// This is just a very basic smoke-test for toExperimentalSpannerType.
func TestToExperimentalSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	conv.TargetDb = constants.TargetExperimentalPostgres
	name := "test"
	tableId := "t1"
	srcSchema := schema.Table{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15"},
		ColDefs: map[string]schema.Column{
			"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: "int"}},
			"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: "float"}},
			"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: "tinyint", Mods: []int64{1}}},
			"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
			"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: "timestamp"}},
			"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: "binary", Mods: []int64{4000}}},
			"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: "date"}},
			"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: "money"}},
			"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: "smalldatetime"}},
			"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: "nvarchar", Mods: []int64{50}}},
			"c12": {Name: "l", Id: "c12", Type: schema.Type{Name: "image"}},
			"c13": {Name: "m", Id: "c13", Type: schema.Type{Name: "geometry"}},
			"c14": {Name: "n", Id: "c14", Type: schema.Type{Name: "bit"}},
			"c15": {Name: "o", Id: "c15", Type: schema.Type{Name: "uniqueidentifier"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}},
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c16"}},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c19"}}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}}},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c16", "c17", "c18"},
		ColDefs: map[string]schema.Column{
			"c16": {Name: "dref", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c17": {Name: "b", Type: schema.Type{Name: "float"}},
			"c18": {Name: "c", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c16"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c19", "c20", "c21"},
		ColDefs: map[string]schema.Column{
			"c19": {Name: "aref", Type: schema.Type{Name: "int"}},
			"c20": {Name: "b", Type: schema.Type{Name: "float"}},
			"c21": {Name: "c", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c19"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
			"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
			"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Int64}},
			"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Date}},
			"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Numeric}},
			"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Timestamp}},
			"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.String, Len: int64(50)}},
			"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.Bool}},
			"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},

		PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
		ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c16"}},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c19"}}},
		Indexes: []ddl.CreateIndex{{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"c1":  {internal.Widened},
		"c2":  {internal.Widened},
		"c3":  {internal.Widened},
		"c10": {internal.Timestamp},
		"c13": {internal.NoGoodType},
	}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId])
}

func dropComments(t *ddl.CreateTable) {
	t.Comment = ""
	for _, c := range t.ColIds {
		cd := t.ColDefs[c]
		cd.Comment = ""
		t.ColDefs[c] = cd
	}
}
