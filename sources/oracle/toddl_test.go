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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestToSpannerTypeInternal(t *testing.T) {
	conv := internal.MakeConv()
	_, errCheck := toSpannerTypeInternal(conv, "STRING", schema.Type{"TIMESTAMP", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in timestamp to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"INTERVAL", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in interval to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "", schema.Type{"INTERVAL", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in interval to default conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "", schema.Type{"INTERVAL", []int64{}, []int64{}})
	if errCheck != nil {
		t.Errorf("Error in interval to default conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"NUMBER", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in number to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "", schema.Type{"NUMBER", []int64{31}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in number to default conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "", schema.Type{"NUMBER", []int64{31, 11}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in number to default conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"BLOB", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in blob to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"CHAR", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in char to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"CHAR", []int64{}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in char to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"CLOB", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in clob to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"DATE", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in date to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"FLOAT", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in float to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"RAW", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in raw to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "", schema.Type{"RAW", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in raw to default conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"ROWID", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in rowid to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"UROWID", []int64{1, 2, 3}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in urowid to string conversion")
	}
	_, errCheck = toSpannerTypeInternal(conv, "STRING", schema.Type{"UROWID", []int64{}, []int64{1, 2, 3}})
	if errCheck != nil {
		t.Errorf("Error in urowid to string conversion")
	}
}

func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "test"
	tableId := "t1"
	srcSchema := schema.Table{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
		ColDefs: map[string]schema.Column{
			"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "NUMBER"}},
			"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "FLOAT"}},
			"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "BFILE"}},
			"c4": {Name: "d", Id: "c4", Type: schema.Type{Name: "VARCHAR2", Mods: []int64{20}}},
			"c5": {Name: "e", Id: "c5", Type: schema.Type{Name: "DATE"}},
			"c6": {Name: "f", Id: "c6", Type: schema.Type{Name: "TIMESTAMP"}},
			"c7": {Name: "g", Id: "c7", Type: schema.Type{Name: "LONG"}},
			"c8": {Name: "h", Id: "c8", Type: schema.Type{Name: "NUMBER", Mods: []int64{13}}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}},
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c9"}},
			{Name: "fk_fake", ColIds: []string{"x"}, ReferTableId: "t2", ReferColumnIds: []string{"x"}},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c12"}}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}},
			{Name: "index_with_0_key", Unique: true, Keys: []schema.Key{{ColId: "m", Desc: false}, {ColId: "y", Desc: true}}},
		},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c9", "c10", "c11"},
		ColDefs: map[string]schema.Column{
			"c9":  {Name: "dref", Id: "c9", Type: schema.Type{Name: "VARCHAR2", Mods: []int64{6}}},
			"c10": {Name: "b", Id: "c10", Type: schema.Type{Name: "FLOAT"}},
			"c11": {Name: "c", Id: "c11", Type: schema.Type{Name: "BOOL"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c9"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c12", "c13", "c14"},
		ColDefs: map[string]schema.Column{
			"c12": {Name: "aref", Id: "c12", Type: schema.Type{Name: "NUMBER"}},
			"c13": {Name: "b", Id: "c13", Type: schema.Type{Name: "FLOAT"}},
			"c14": {Name: "c", Id: "c14", Type: schema.Type{Name: "BOOL"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c12"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}, constants.ORACLE))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Numeric}},
			"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"c4": {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(20)}},
			"c5": {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Date}},
			"c6": {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}},
			"c7": {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c8": {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Int64}},
		},
		PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
		ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c9"}},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c12"}}},
		Indexes: []ddl.CreateIndex{{Name: "index1", TableId: tableId, Unique: true, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}},
			{Name: "index_with_0_key", TableId: tableId, Unique: true, Keys: nil}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId].ColumnLevelIssues)
	// 1 FK issue, 2 index col not found
	assert.Equal(t, int64(3), conv.Unexpecteds())
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
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]schema.Column{
			"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: "NUMBER"}},
			"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: "FLOAT"}},
			"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: "BFILE"}},
			"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: "VARCHAR2", Mods: []int64{20}}},
			"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: "DATE"}},
			"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: "TIMESTAMP"}},
			"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: "LONG"}},
			"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: "NUMBER", Mods: []int64{13}}},
			"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: "JSON"}},
			"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: "NCLOB"}},
			"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: "XMLTYPE"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}},
		ForeignKeys: []schema.ForeignKey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c12"}},
			{Name: "fk_fake", ColIds: []string{"x"}, ReferTableId: "t2", ReferColumnIds: []string{"x"}},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c15"}}},
		Indexes: []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c4", Desc: true}}},
			{Name: "index_with_0_key", Unique: true, Keys: []schema.Key{{ColId: "m", Desc: false}, {ColId: "y", Desc: true}}},
		},
	}
	conv.SrcSchema[tableId] = srcSchema
	conv.SrcSchema["t2"] = schema.Table{
		Name:   "ref_table",
		Id:     "t2",
		ColIds: []string{"c12", "c13", "c14"},
		ColDefs: map[string]schema.Column{
			"c12": {Name: "dref", Id: "c12", Type: schema.Type{Name: "VARCHAR2", Mods: []int64{6}}},
			"c13": {Name: "b", Id: "c13", Type: schema.Type{Name: "FLOAT"}},
			"c14": {Name: "c", Id: "c14", Type: schema.Type{Name: "BOOL"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c12"}},
	}
	conv.SrcSchema["t3"] = schema.Table{
		Name:   "ref_table2",
		Id:     "t3",
		ColIds: []string{"c15", "c16", "c17"},
		ColDefs: map[string]schema.Column{
			"c15": {Name: "aref", Id: "c15", Type: schema.Type{Name: "NUMBER"}},
			"c16": {Name: "b", Id: "c16", Type: schema.Type{Name: "FLOAT"}},
			"c17": {Name: "c", Id: "c17", Type: schema.Type{Name: "BOOL"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c15"}},
	}
	conv.UsedNames = map[string]bool{"ref_table": true, "ref_table2": true}
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}, constants.MYSQL))
	actual := conv.SpSchema[tableId]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   name,
		Id:     tableId,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Numeric}},
			"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
			"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
			"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(20)}},
			"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Date}},
			"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}},
			"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Int64}},
			"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.JSON}},
			"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},
		PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
		ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"c4"}, ReferTableId: "t2", ReferColumnIds: []string{"c12"}},
			{Name: "fk_test2", ColIds: []string{"c1"}, ReferTableId: "t3", ReferColumnIds: []string{"c15"}}},
		Indexes: []ddl.CreateIndex{{Name: "index_with_0_key", TableId: tableId, Unique: true, Keys: nil}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId].ColumnLevelIssues)
	// 1 FK issue, 2 index col not found
	assert.Equal(t, int64(3), conv.Unexpecteds())
}

func dropComments(t *ddl.CreateTable) {
	t.Comment = ""
	for _, c := range t.ColIds {
		cd := t.ColDefs[c]
		cd.Comment = ""
		t.ColDefs[c] = cd
	}
}
