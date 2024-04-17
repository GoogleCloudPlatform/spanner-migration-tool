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

func TestToSpannerTypeInternal(t *testing.T) {

	_, errCheck := toSpannerTypeInternal(schema.Type{"bool", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in boolean to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bool", []int64{1, 2, 3}, []int64{1, 2, 3}}, "INT64")
	if errCheck == nil {
		t.Errorf("Error in boolean to int64 conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"tinyint", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in tinyint to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"tinyint", []int64{1, 2, 3}, []int64{1, 2, 3}}, "INT64")
	if errCheck == nil {
		t.Errorf("Error in tinyint to int64 conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"double", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in double to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"float", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in float to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"decimal", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in decimal to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bigint", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in bigint to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"int", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in int to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bit", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck != nil {
		t.Errorf("Error in bit to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"char", []int64{1, 2, 3}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in char to bytes conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"char", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in char to bytes conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"char", []int64{}, []int64{1, 2, 3}}, "DEFAULT")
	if errCheck != nil {
		t.Errorf("Error in char to default conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"text", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in text to bytes conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"json", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in json to bytes conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"binary", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck != nil {
		t.Errorf("Error in binary to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"binary", []int64{1, 2, 3}, []int64{1, 2, 3}}, "DEFAULT")
	if errCheck != nil {
		t.Errorf("Error in binary to default conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"blob", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck != nil {
		t.Errorf("Error in blob to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"date", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in date to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"datetime", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in datetime to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"timestamp", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in timestamp to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"time", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in time to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"DEFAULT", []int64{1, 2, 3}, []int64{1, 2, 3}}, "")
	if errCheck == nil {
		t.Errorf("Error in default conversion for unidentified source datatype")
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
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c2":  ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c3":  ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c4":  ddl.ColumnDef{Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c5":  ddl.ColumnDef{Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c6":  ddl.ColumnDef{Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c7":  ddl.ColumnDef{Name: "g", Id: "c7", T: ddl.Type{Name: ddl.JSON}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c8":  ddl.ColumnDef{Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Date}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c9":  ddl.ColumnDef{Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Timestamp}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c10": ddl.ColumnDef{Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
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
	commonInfoSchema := common.InfoSchemaImpl{}
	tableList, _ := commonInfoSchema.GetIncludedSrcTablesFromConv(conv)
	keys := make([]string, 0, len(tableList))
	for k := range tableList {
		keys = append(keys, k)
	}

	assert.Equal(t, len(conv.SrcSchema), len(tableList[keys[0]].TableDetails))
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
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c2":  ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c3":  ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c4":  ddl.ColumnDef{Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c5":  ddl.ColumnDef{Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c6":  ddl.ColumnDef{Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c7":  ddl.ColumnDef{Name: "g", Id: "c7", T: ddl.Type{Name: ddl.JSON}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c8":  ddl.ColumnDef{Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Date}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c9":  ddl.ColumnDef{Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Timestamp}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c10": ddl.ColumnDef{Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
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
