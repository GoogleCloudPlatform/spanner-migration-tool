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

package dynamodb

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "t1"
	srcSchema := schema.Table{
		Id:     "t1",
		Name:   name,
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]schema.Column{
			"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: typeString}},
			"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: typeNumber}},
			"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: typeNumberString}},
			"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: typeBool}},
			"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: typeBinary}},
			"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: typeList}},
			"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: typeMap}},
			"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: typeStringSet}},
			"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: typeBinarySet}},
			"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: typeNumberSet}},
			"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: typeNumberStringSet}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}, {ColId: "c2"}},
		Indexes: []schema.Index{
			{Name: "index1", Keys: []schema.Key{{ColId: "c2"}, {ColId: "c3"}}},
			{Name: "test", Keys: []schema.Key{{ColId: "c4"}}},
		},
	}
	audit := internal.Audit{
		MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
	}
	conv.SrcSchema[name] = srcSchema
	conv.Audit = audit
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[name]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   "t1",
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c1", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c10": ddl.ColumnDef{Name: "j", T: ddl.Type{Name: "NUMERIC", Len: 0, IsArray: true}, NotNull: false, Comment: "", Id: "c10", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c11": ddl.ColumnDef{Name: "k", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: true}, NotNull: false, Comment: "", Id: "c11", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c2":  ddl.ColumnDef{Name: "b", T: ddl.Type{Name: "NUMERIC", Len: 0, IsArray: false}, NotNull: false, Comment: "", Id: "c2", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c3":  ddl.ColumnDef{Name: "c", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c3", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c4":  ddl.ColumnDef{Name: "d", T: ddl.Type{Name: "BOOL", Len: 0, IsArray: false}, NotNull: false, Comment: "", Id: "c4", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c5":  ddl.ColumnDef{Name: "e", T: ddl.Type{Name: "BYTES", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c5", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c6":  ddl.ColumnDef{Name: "f", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c6", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c7":  ddl.ColumnDef{Name: "g", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c7", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c8":  ddl.ColumnDef{Name: "h", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: true}, NotNull: false, Comment: "", Id: "c8", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c9":  ddl.ColumnDef{Name: "i", T: ddl.Type{Name: "BYTES", Len: 9223372036854775807, IsArray: true}, NotNull: false, Comment: "", Id: "c9", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}}},
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false, Order: 0}, ddl.IndexKey{ColId: "c2", Desc: false, Order: 0}},
		ForeignKeys: []ddl.Foreignkey(nil),
		Indexes: []ddl.CreateIndex{
			ddl.CreateIndex{Name: "index1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c2", Desc: false, Order: 0}, ddl.IndexKey{ColId: "c3", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)},
			ddl.CreateIndex{Name: "test", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c4", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}},
		Id: "t1"}
	assert.Equal(t, expected, actual)
}

func TestToSpannerPostgreSQLDialectType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	conv.SpDialect = constants.DIALECT_POSTGRESQL
	name := "test"
	srcSchema := schema.Table{
		Name:   name,
		Id:     "t1",
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]schema.Column{
			"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: typeString}},
			"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: typeNumber}},
			"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: typeNumberString}},
			"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: typeBool}},
			"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: typeBinary}},
			"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: typeList}},
			"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: typeMap}},
			"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: typeStringSet}},
			"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: typeBinarySet}},
			"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: typeNumberSet}},
			"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: typeNumberStringSet}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}, {ColId: "c2"}},
		Indexes: []schema.Index{
			{Name: "index1", Keys: []schema.Key{{ColId: "c2"}, {ColId: "c3"}}},
			{Name: "test", Keys: []schema.Key{{ColId: "c4"}}},
		},
	}
	audit := internal.Audit{
		MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
	}
	conv.SrcSchema["t1"] = srcSchema
	conv.Audit = audit
	schemaToSpanner := common.SchemaToSpannerImpl{}
	assert.Nil(t, schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema["t1"]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   "test",
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c1", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c10": ddl.ColumnDef{Name: "j", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c10", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c11": ddl.ColumnDef{Name: "k", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c11", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c2":  ddl.ColumnDef{Name: "b", T: ddl.Type{Name: "NUMERIC", Len: 0, IsArray: false}, NotNull: false, Comment: "", Id: "c2", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c3":  ddl.ColumnDef{Name: "c", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c3", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c4":  ddl.ColumnDef{Name: "d", T: ddl.Type{Name: "BOOL", Len: 0, IsArray: false}, NotNull: false, Comment: "", Id: "c4", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c5":  ddl.ColumnDef{Name: "e", T: ddl.Type{Name: "BYTES", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c5", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c6":  ddl.ColumnDef{Name: "f", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c6", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c7":  ddl.ColumnDef{Name: "g", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c7", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c8":  ddl.ColumnDef{Name: "h", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c8", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			"c9":  ddl.ColumnDef{Name: "i", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c9", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}}},
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false, Order: 0}, ddl.IndexKey{ColId: "c2", Desc: false, Order: 0}},
		ForeignKeys: []ddl.Foreignkey(nil),
		Indexes: []ddl.CreateIndex{
			ddl.CreateIndex{Name: "test_1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c4", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}},
		Id: "t1"}
	assert.Equal(t, expected, actual)
}

func dropComments(t *ddl.CreateTable) {
	t.Comment = ""
	for _, c := range t.ColIds {
		cd := t.ColDefs[c]
		cd.Comment = ""
		t.ColDefs[c] = cd
	}
}
