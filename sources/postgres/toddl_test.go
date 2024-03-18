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
		t.Errorf("Error in bool to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bool", []int64{1, 2, 3}, []int64{1, 2, 3}}, "INT64")
	if errCheck == nil {
		t.Errorf("Error in bool to int64 conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bigserial", []int64{1, 2, 3}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in bigserial to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bpchar", []int64{1, 2, 3}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in bpchar to bytes conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bpchar", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in bpchar to bytes conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bpchar", []int64{}, []int64{1, 2, 3}}, "")
	if errCheck != nil {
		t.Errorf("Error in bpchar to default conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"bytea", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck != nil {
		t.Errorf("Error in bytea to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"date", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in date to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"float8", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in float8 to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"float4", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in float4 to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"int8", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in int8 to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"int4", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in int4 to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"int2", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in int2 to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"numeric", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in numeric to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"serial", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in serial to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"text", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in text to bytes conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"timestamptz", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in timestamptz to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"timestamp", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck == nil {
		t.Errorf("Error in timestamp to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"json", []int64{}, []int64{1, 2, 3}}, "STRING")
	if errCheck != nil {
		t.Errorf("Error in json to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"varchar", []int64{}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in varchar to bytes conversion")
	}
	_, errCheck = toSpannerTypeInternal(schema.Type{"varchar", []int64{1, 2, 3}, []int64{1, 2, 3}}, "BYTES")
	if errCheck != nil {
		t.Errorf("Error in varchar to bytes conversion")
	}
}

// This is just a very basic smoke-test for toSpannerType.
// The real testing of toSpannerType happens in process_test.go
// via the public API ProcessPgDump (see TestProcessPgDump).
func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "test"
	tableId := "t1"
	srcSchema := schema.Table{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6"},
		ColDefs: map[string]schema.Column{
			"c1": schema.Column{Name: "a", Id: "c1", Type: schema.Type{Name: "int8"}},
			"c2": schema.Column{Name: "b", Id: "c2", Type: schema.Type{Name: "float4"}},
			"c3": schema.Column{Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
			"c4": schema.Column{Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c5": schema.Column{Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
			"c6": schema.Column{Name: "f", Id: "c6", Type: schema.Type{Name: "timestamptz"}},
		},
		PrimaryKeys: []schema.Key{schema.Key{ColId: "c1"}},
		ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c7"}},
			schema.ForeignKey{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c10"}}},
		Indexes: []schema.Index{schema.Index{Name: "index1", Unique: true, Keys: []schema.Key{schema.Key{ColId: "c1", Desc: false}, schema.Key{ColId: "c4", Desc: true}}},
			schema.Index{Name: "index2", Unique: false, Keys: []schema.Key{schema.Key{ColId: "c4", Desc: true}}}},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c7", "c8", "c9"},
		ColDefs: map[string]schema.Column{
			"c7": {Name: "dref", Id: "c7", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c8": {Name: "b", Id: "c8", Type: schema.Type{Name: "float4"}},
			"c9": {Name: "c", Id: "c9", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c7"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c10", "c11", "c12"},
		ColDefs: map[string]schema.Column{
			"c10": {Name: "aref", Id: "c10", Type: schema.Type{Name: "int8"}},
			"c11": {Name: "b", Id: "c11", Type: schema.Type{Name: "float4"}},
			"c12": {Name: "c", Id: "c12", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c10"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1": ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2": ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float32}},
			"c3": ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
			"c4": ddl.ColumnDef{Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"c5": ddl.ColumnDef{Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
			"c6": ddl.ColumnDef{Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}},
		},
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1"}},
		ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c7"}},
			ddl.Foreignkey{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c10"}}},
		Indexes: []ddl.CreateIndex{ddl.CreateIndex{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false}, ddl.IndexKey{ColId: "c4", Desc: true}}},
			ddl.CreateIndex{Name: "index2", TableId: tableId, Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c4", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId].ColumnLevelIssues)
}

// This is just a very basic smoke-test for toExperimentalSpannerType.
// The real testing of toSpannerType happens in process_test.go
// via the public API ProcessPgDump (see TestProcessPgDump).
func TestToExperimentalSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	conv.SpDialect = constants.DIALECT_POSTGRESQL
	name := "test"
	tableId := "t1"
	srcSchema := schema.Table{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c13"},
		ColDefs: map[string]schema.Column{
			"c1":  schema.Column{Name: "a", Id: "c1", Type: schema.Type{Name: "int8"}},
			"c2":  schema.Column{Name: "b", Id: "c2", Type: schema.Type{Name: "float4"}},
			"c3":  schema.Column{Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
			"c4":  schema.Column{Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c5":  schema.Column{Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
			"c6":  schema.Column{Name: "f", Id: "c6", Type: schema.Type{Name: "date"}},
			"c7":  schema.Column{Name: "g", Id: "c7", Type: schema.Type{Name: "json"}},
			"c13": schema.Column{Name: "h", Id: "c13", Type: schema.Type{Name: "float8"}},
		},
		PrimaryKeys: []schema.Key{schema.Key{ColId: "c1"}},
		ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c7"}},
			schema.ForeignKey{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c10"}}},
		Indexes: []schema.Index{schema.Index{Name: "index1", Unique: true, Keys: []schema.Key{schema.Key{ColId: "c1", Desc: false}, schema.Key{ColId: "c4", Desc: true}}},
			schema.Index{Name: "index2", Unique: false, Keys: []schema.Key{schema.Key{ColId: "c4", Desc: true}}}},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c7", "c8", "c9"},
		ColDefs: map[string]schema.Column{
			"c7": {Name: "dref", Id: "c7", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"c8": {Name: "b", Id: "c8", Type: schema.Type{Name: "float4"}},
			"c9": {Name: "c", Id: "c9", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c7"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c10", "c11", "c12"},
		ColDefs: map[string]schema.Column{
			"c10": {Name: "aref", Id: "c10", Type: schema.Type{Name: "int8"}},
			"c11": {Name: "b", Id: "c11", Type: schema.Type{Name: "float4"}},
			"c12": {Name: "c", Id: "c12", Type: schema.Type{Name: "bool"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c10"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c13"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			"c2":  ddl.ColumnDef{Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float32}},
			"c3":  ddl.ColumnDef{Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
			"c4":  ddl.ColumnDef{Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
			"c5":  ddl.ColumnDef{Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
			"c6":  ddl.ColumnDef{Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Date}},
			"c7":  ddl.ColumnDef{Name: "g", Id: "c7", T: ddl.Type{Name: ddl.JSON}},
			"c13": ddl.ColumnDef{Name: "h", Id: "c13", T: ddl.Type{Name: ddl.Float64}},
		},
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1"}},
		ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c7"}},
			ddl.Foreignkey{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c10"}}},
		Indexes: []ddl.CreateIndex{ddl.CreateIndex{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false}, ddl.IndexKey{ColId: "c4", Desc: true}}},
			ddl.CreateIndex{Name: "index2", TableId: tableId, Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c4", Desc: true}}}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{}
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
