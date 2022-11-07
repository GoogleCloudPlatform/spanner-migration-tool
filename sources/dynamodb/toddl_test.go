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

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
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
			"c1":  {Name: "a", Type: schema.Type{Name: typeString}},
			"c2":  {Name: "b", Type: schema.Type{Name: typeNumber}},
			"c3":  {Name: "c", Type: schema.Type{Name: typeNumberString}},
			"c4":  {Name: "d", Type: schema.Type{Name: typeBool}},
			"c5":  {Name: "e", Type: schema.Type{Name: typeBinary}},
			"c6":  {Name: "f", Type: schema.Type{Name: typeList}},
			"c7":  {Name: "g", Type: schema.Type{Name: typeMap}},
			"c8":  {Name: "h", Type: schema.Type{Name: typeStringSet}},
			"c9":  {Name: "i", Type: schema.Type{Name: typeBinarySet}},
			"c10": {Name: "j", Type: schema.Type{Name: typeNumberSet}},
			"c11": {Name: "k", Type: schema.Type{Name: typeNumberStringSet}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}, {ColId: "c2"}},
		Indexes: []schema.Index{
			{Name: "index1", Keys: []schema.Key{{ColId: "c2"}, {ColId: "c3"}}},
			{Name: "test", Keys: []schema.Key{{ColId: "c4"}}},
		},
	}
	audit := internal.Audit{
		MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		ToSourceFkIdx: map[string]internal.FkeyAndIdxs{
			"t1": {
				Name:       "t1",
				ForeignKey: map[string]string{},
				Index:      map[string]string{},
			},
		},
		ToSpannerFkIdx: map[string]internal.FkeyAndIdxs{
			"t1": {
				Name:       "t1",
				ForeignKey: map[string]string{},
				Index:      map[string]string{},
			},
		},
	}
	conv.SrcSchema[name] = srcSchema
	conv.Audit = audit
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema[name]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   "t1",
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c1"},
			"c10": ddl.ColumnDef{Name: "j", T: ddl.Type{Name: "NUMERIC", Len: 0, IsArray: true}, NotNull: false, Comment: "", Id: "c10"},
			"c11": ddl.ColumnDef{Name: "k", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: true}, NotNull: false, Comment: "", Id: "c11"},
			"c2":  ddl.ColumnDef{Name: "b", T: ddl.Type{Name: "NUMERIC", Len: 0, IsArray: false}, NotNull: false, Comment: "", Id: "c2"},
			"c3":  ddl.ColumnDef{Name: "c", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c3"},
			"c4":  ddl.ColumnDef{Name: "d", T: ddl.Type{Name: "BOOL", Len: 0, IsArray: false}, NotNull: false, Comment: "", Id: "c4"},
			"c5":  ddl.ColumnDef{Name: "e", T: ddl.Type{Name: "BYTES", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c5"},
			"c6":  ddl.ColumnDef{Name: "f", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c6"},
			"c7":  ddl.ColumnDef{Name: "g", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c7"},
			"c8":  ddl.ColumnDef{Name: "h", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: true}, NotNull: false, Comment: "", Id: "c8"},
			"c9":  ddl.ColumnDef{Name: "i", T: ddl.Type{Name: "BYTES", Len: 9223372036854775807, IsArray: true}, NotNull: false, Comment: "", Id: "c9"}},
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false, Order: 0}, ddl.IndexKey{ColId: "c2", Desc: false, Order: 0}},
		ForeignKeys: []ddl.Foreignkey(nil),
		Indexes: []ddl.CreateIndex{
			ddl.CreateIndex{Name: "index1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c2", Desc: false, Order: 0}, ddl.IndexKey{ColId: "c3", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)},
			ddl.CreateIndex{Name: "test", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c4", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}},
		Id: "t1"}
	assert.Equal(t, expected, actual)
}

func TestToExperimentalSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	conv.TargetDb = constants.TargetExperimentalPostgres
	name := "test"
	srcSchema := schema.Table{
		Name: name,
		Id:   "t1",
		// ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]schema.Column{
			"c1":  {Name: "a", Type: schema.Type{Name: typeString}},
			"c2":  {Name: "b", Type: schema.Type{Name: typeNumber}},
			"c3":  {Name: "c", Type: schema.Type{Name: typeNumberString}},
			"c4":  {Name: "d", Type: schema.Type{Name: typeBool}},
			"c5":  {Name: "e", Type: schema.Type{Name: typeBinary}},
			"c6":  {Name: "f", Type: schema.Type{Name: typeList}},
			"c7":  {Name: "g", Type: schema.Type{Name: typeMap}},
			"c8":  {Name: "h", Type: schema.Type{Name: typeStringSet}},
			"c9":  {Name: "i", Type: schema.Type{Name: typeBinarySet}},
			"c10": {Name: "j", Type: schema.Type{Name: typeNumberSet}},
			"c11": {Name: "k", Type: schema.Type{Name: typeNumberStringSet}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}, {ColId: "c2"}},
		Indexes: []schema.Index{
			{Name: "index1", Keys: []schema.Key{{ColId: "c2"}, {ColId: "c3"}}},
			{Name: "test", Keys: []schema.Key{{ColId: "c4"}}},
		},
	}
	audit := internal.Audit{
		MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		ToSourceFkIdx: map[string]internal.FkeyAndIdxs{
			"t1": {
				Name:       "t1",
				ForeignKey: map[string]string{},
				Index:      map[string]string{},
			},
		},
		ToSpannerFkIdx: map[string]internal.FkeyAndIdxs{
			"t1": {
				Name:       "t1",
				ForeignKey: map[string]string{},
				Index:      map[string]string{},
			},
		},
	}
	conv.SrcSchema["t1"] = srcSchema
	conv.Audit = audit
	assert.Nil(t, common.SchemaToSpannerDDL(conv, ToDdlImpl{}))
	actual := conv.SpSchema["t1"]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:   "test",
		ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"},
		ColDefs: map[string]ddl.ColumnDef{
			"c1":  ddl.ColumnDef{Name: "a", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c1"},
			"c10": ddl.ColumnDef{Name: "j", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c10"},
			"c11": ddl.ColumnDef{Name: "k", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c11"},
			"c2":  ddl.ColumnDef{Name: "b", T: ddl.Type{Name: "NUMERIC", Len: 0, IsArray: false}, NotNull: false, Comment: "", Id: "c2"},
			"c3":  ddl.ColumnDef{Name: "c", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c3"},
			"c4":  ddl.ColumnDef{Name: "d", T: ddl.Type{Name: "BOOL", Len: 0, IsArray: false}, NotNull: false, Comment: "", Id: "c4"},
			"c5":  ddl.ColumnDef{Name: "e", T: ddl.Type{Name: "BYTES", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c5"},
			"c6":  ddl.ColumnDef{Name: "f", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c6"},
			"c7":  ddl.ColumnDef{Name: "g", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c7"},
			"c8":  ddl.ColumnDef{Name: "h", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c8"},
			"c9":  ddl.ColumnDef{Name: "i", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: false, Comment: "", Id: "c9"}},
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false, Order: 0}, ddl.IndexKey{ColId: "c2", Desc: false, Order: 0}},
		ForeignKeys: []ddl.Foreignkey(nil),
		Indexes: []ddl.CreateIndex{
			ddl.CreateIndex{Name: "index1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c2", Desc: false, Order: 0}, ddl.IndexKey{ColId: "c3", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)},
			ddl.CreateIndex{Name: "test_2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{ColId: "c4", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}},
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
