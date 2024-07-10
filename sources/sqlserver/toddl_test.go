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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToSpannerTypeInternal(t *testing.T) {
	_, errCheck := toSpannerTypeInternal(schema.Type{"bigint", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in bigint of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bigint", []int64{1, 2, 3}, []int64{1, 2, 3}}, "INT64")
	if errCheck == nil {
		t.Errorf("Error in bigint of sptype int64")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"tinyint", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in tinyint of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"tinyint", []int64{1, 2, 3}, []int64{1, 2, 3}}, "INT64")
	if errCheck == nil {
		t.Errorf("Error in tinyint of sptype int64")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"float", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in float of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"numeric", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in numeric of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bit", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck != nil {
		t.Errorf("Error in bit of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"uniqueidentifier", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in uniqueidentifier of sptype bytes")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"uniqueidentifier", []int64{1}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in uniqueidentifier of sptype bytes")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"uniqueidentifier", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck != nil {
		t.Errorf("Error in uniqueidentifier of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"varchar", []int64{1, 2, 3}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in varchar of sptype bytes")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"varchar", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in varchar of sptype bytes")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"varchar", []int64{}, []int64{1, 2, 3}}, "")
	if errCheck != nil {
		t.Errorf("Error in varchar of default sptype")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"ntext", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in ntext of sptype bytes")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"binary", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck != nil {
		t.Errorf("Error in binary of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"date", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in date of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"datetime", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in datetime of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"timestamp", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in timestamp of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"time", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in time of sptype string")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"DEFAULT", []int64{1, 2, 3}, []int64{1, 2, 3}}, "")
	if errCheck == nil {
		t.Errorf("Error in default case")
	}
}

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
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c16"}, OnDelete: "", OnUpdate: ""},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c19"}, OnDelete: "", OnUpdate: ""}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}}},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c16", "c17", "c18"},
		ColDefs: map[string]schema.Column{
			"c16": {Name: "dref", Id: "c16", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c17": {Name: "b", Id: "c17", Type: schema.Type{Name: "float"}},
			"c18": {Name: "c", Id: "c18", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c16"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c19", "c20", "c21"},
		ColDefs: map[string]schema.Column{
			"c19": {Name: "aref", Id: "c19", Type: schema.Type{Name: "int"}},
			"c20": {Name: "b", Id: "c20", Type: schema.Type{Name: "float"}},
			"c21": {Name: "c", Id: "c21", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c19"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}))
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
		ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c16"}, OnDelete: "", OnUpdate: ""},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c19"}, OnDelete: "", OnUpdate: ""}},
		Indexes: []ddl.CreateIndex{{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)

	expectedIssues := internal.TableIssues{
		TableLevelIssues: []internal.SchemaIssue{internal.ForeignKeyActionNotSupported},
		ColumnLevelIssues: map[string][]internal.SchemaIssue{
			"c1":  {internal.Widened},
			"c2":  {internal.Widened},
			"c3":  {internal.Widened},
			"c10": {internal.Timestamp},
			"c13": {internal.NoGoodType},
		},
	}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId])
}

// This is just a very basic smoke-test for toSpannerPostgreSQLDialectType.
func TestToSpannerPostgreSQLDialectType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	conv.SpDialect = constants.DIALECT_POSTGRESQL
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
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c16"}, OnDelete: "", OnUpdate: ""},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c19"}, OnDelete: "", OnUpdate: ""}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}}},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c16", "c17", "c18"},
		ColDefs: map[string]schema.Column{
			"c16": {Name: "dref", Id: "c16", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c17": {Name: "b", Id: "c17", Type: schema.Type{Name: "float"}},
			"c18": {Name: "c", Id: "c18", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c16"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c19", "c20", "c21"},
		ColDefs: map[string]schema.Column{
			"c19": {Name: "aref", Id: "c19", Type: schema.Type{Name: "int"}},
			"c20": {Name: "b", Id: "c20", Type: schema.Type{Name: "float"}},
			"c21": {Name: "c", Id: "c21", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c19"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}))
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
		ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c16"}, OnDelete: "", OnUpdate: ""},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c19"}, OnDelete: "", OnUpdate: ""}},
		Indexes: []ddl.CreateIndex{{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)

	expectedIssues := internal.TableIssues{
		TableLevelIssues: []internal.SchemaIssue{internal.ForeignKeyActionNotSupported},
		ColumnLevelIssues: map[string][]internal.SchemaIssue{
			"c1":  {internal.Widened},
			"c2":  {internal.Widened},
			"c3":  {internal.Widened},
			"c10": {internal.Timestamp},
			"c13": {internal.NoGoodType},
		},
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
