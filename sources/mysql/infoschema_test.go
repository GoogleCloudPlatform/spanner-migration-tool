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
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

type mockSpec struct {
	query string
	args  []driver.Value   // Query args.
	cols  []string         // Columns names for returned rows.
	rows  [][]driver.Value // Set of rows returned.
}

func TestProcessSchemaMYSQL(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT (.+) FROM information_schema.tables where table_type = 'BASE TABLE' and (.+)",
			cols:  []string{"table_name"},
			rows: [][]driver.Value{
				{"user"},
				{"cart"},
				{"product"},
				{"test"},
				{"test_ref"},
			},
		},
		{
			query: "(?is)SELECT c.table_name.+?FROM information_schema.COLUMNS.+?table_name IN \\(.+\\).+",
			args:  []driver.Value{"test", "user", "cart", "product", "test", "test_ref"},
			cols:  []string{"table_name", "column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "generation_expression", "extra"},
			rows: [][]driver.Value{
				// user
				{"user", "user_id", "text", "text", "NO", "uuid()", nil, nil, nil, nil, constants.DEFAULT_GENERATED},
				{"user", "name", "text", "text", "NO", "default_name", nil, nil, nil, nil, nil},
				{"user", "ref", "bigint", "bigint", "NO", nil, nil, nil, nil, nil, nil},
				// cart
				{"cart", "productid", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"cart", "userid", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"cart", "quantity", "bigint", "bigint", "YES", nil, nil, 64, 0, nil, nil},
				// product
				{"product", "product_id", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"product", "product_name", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				// test
				{"test", "id", "bigint", "bigint", "NO", nil, nil, 64, 0, nil, nil},
				{"test", "s", "set", "set", "YES", nil, nil, nil, nil, nil, nil},
				{"test", "txt", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"test", "b", "boolean", "boolean", "YES", nil, nil, nil, nil, nil, nil},
				{"test", "bs", "bigint", "bigint", "NO", "nextval('test11_bs_seq'::regclass)", nil, 64, 0, nil, nil},
				{"test", "bl", "blob", "blob", "YES", nil, nil, nil, nil, nil, nil},
				{"test", "c", "char", "char(1)", "YES", nil, 1, nil, nil, nil, nil},
				{"test", "c8", "char", "char(8)", "YES", nil, 8, nil, nil, nil, nil},
				{"test", "d", "date", "date", "YES", nil, nil, nil, nil, nil, nil},
				{"test", "dec", "decimal", "decimal(20,5)", "YES", nil, nil, 20, 5, nil, nil},
				{"test", "f8", "double", "double", "YES", nil, nil, 53, nil, nil, nil},
				{"test", "f4", "float", "float", "YES", nil, nil, 24, nil, nil, nil},
				{"test", "i8", "bigint", "bigint", "YES", nil, nil, 64, 0, nil, nil},
				{"test", "i4", "integer", "integer", "YES", nil, nil, 32, 0, nil, "auto_increment"},
				{"test", "i2", "smallint", "smallint", "YES", nil, nil, 16, 0, nil, nil},
				{"test", "si", "integer", "integer", "NO", "nextval('test11_s_seq'::regclass)", nil, 32, 0, nil, nil},
				{"test", "ts", "datetime", "datetime", "YES", nil, nil, nil, nil, nil, nil},
				{"test", "tz", "timestamp", "timestamp", "YES", nil, nil, nil, nil, nil, nil},
				{"test", "vc", "varchar", "varchar", "YES", nil, nil, nil, nil, nil, nil},
				{"test", "vc6", "varchar", "varchar(6)", "YES", nil, 6, nil, nil, nil, nil},
				{"test", "bu", "bigint", "bigint(20) unsigned", "YES", nil, nil, 20, 0, nil, nil},
				// test_ref
				{"test_ref", "ref_id", "bigint", "bigint", "NO", nil, nil, 64, 0, nil, nil},
				{"test_ref", "ref_txt", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"test_ref", "abc", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
			},
		},
		{
			query: regexp.QuoteMeta(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'information_schema' OR TABLE_SCHEMA = 'INFORMATION_SCHEMA') AND TABLE_NAME = 'CHECK_CONSTRAINTS';`),
			args:  nil,
			cols:  []string{"count"},
			rows: [][]driver.Value{
				{int64(0)},
			},
		},
		{
			query: "(?is)SELECT t.TABLE_NAME, k.COLUMN_NAME, t.CONSTRAINT_TYPE FROM (.+) ORDER BY t.TABLE_NAME, k.ORDINAL_POSITION;",
			args:  []driver.Value{"test", "user", "cart", "product", "test", "test_ref"},
			cols:  []string{"table_name", "column_name", "constraint_type"},
			rows: [][]driver.Value{
				// user
				{"user", "user_id", "PRIMARY KEY"},
				{"user", "ref", "FOREIGN KEY"},
				// cart
				{"cart", "productid", "PRIMARY KEY"},
				{"cart", "userid", "PRIMARY KEY"},
				// product
				{"product", "product_id", "PRIMARY KEY"},
				// test
				{"test", "id", "PRIMARY KEY"},
				{"test", "id", "FOREIGN KEY"},
				// test_ref
				{"test_ref", "ref_id", "PRIMARY KEY"},
				{"test_ref", "ref_txt", "PRIMARY KEY"},
			},
		},
		{
			query: "(?is)SELECT k.TABLE_NAME, (.+) FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "user", "cart", "product", "test", "test_ref"},
			cols:  []string{"table_name", "referenced_table_name", "column_name", "referenced_column_name", "constraint_name", "delete_rule", "update_rule"},
			rows: [][]driver.Value{
				// user
				{"user", "test", "ref", "id", "fk_test", constants.FK_SET_NULL, constants.FK_CASCADE},
				// cart
				{"cart", "product", "productid", "product_id", "fk_test2", constants.FK_NO_ACTION, constants.FK_NO_ACTION},
				{"cart", "user", "userid", "user_id", "fk_test3", constants.FK_RESTRICT, constants.FK_SET_NULL},
				// test
				{"test", "test_ref", "id", "ref_id", "fk_test4", constants.FK_CASCADE, constants.FK_RESTRICT},
				{"test", "test_ref", "txt", "ref_txt", "fk_test4", constants.FK_CASCADE, constants.FK_RESTRICT},
			},
		},
		{
			query: "(?is)SELECT DISTINCT TABLE_NAME, (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+) table_name IN \\(.+\\) (.+)",
			args:  []driver.Value{"test", "user", "cart", "product", "test", "test_ref"},
			cols:  []string{"table_name", "index_name", "column_name", "seq_in_index", "collation", "non_unique"},
			rows: [][]driver.Value{
				{"cart", "index1", "userid", 1, sql.NullString{Valid: false}, "0"},
				{"cart", "index2", "userid", 1, "A", "1"},
				{"cart", "index2", "productid", 2, "D", "1"},
				{"cart", "index3", "productid", 1, "A", "0"},
				{"cart", "index3", "userid", 2, "D", "0"},
			},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	isi := InfoSchemaImpl{"test", db, "migration-project-id", profiles.SourceProfile{}, profiles.TargetProfile{}}
	commonInfoSchema := common.InfoSchemaImpl{}
	_, err := commonInfoSchema.GenerateSrcSchema(conv, isi, 1)
	assert.Nil(t, err)
	for tName, table := range conv.SrcSchema {
		for cName, col := range table.ColDefs {
			col.GeneratedColumn.Value.ExpressionId = ""
			col.DefaultValue.Value.ExpressionId = ""
			table.ColDefs[cName] = col
		}
		conv.SrcSchema[tName] = table
	}
	expectedSchema := map[string]schema.Table{
		"cart": {
			Name: "cart", Schema: "test", ColIds: []string{"productid", "userid", "quantity"}, ColDefs: map[string]schema.Column{
				"productid": {Name: "productid", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
				"quantity":  {Name: "quantity", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
				"userid":    {Name: "userid", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			},
			PrimaryKeys: []schema.Key{{ColId: "productid", Desc: false, Order: 0}, {ColId: "userid", Desc: false, Order: 0}},
			ForeignKeys: []schema.ForeignKey{{Name: "fk_test2", ColIds: []string{"productid"}, ReferTableId: "product", ReferColumnIds: []string{"product_id"}, OnDelete: constants.FK_NO_ACTION, OnUpdate: constants.FK_NO_ACTION, Id: ""}, {Name: "fk_test3", ColIds: []string{"userid"}, ReferTableId: "user", ReferColumnIds: []string{"user_id"}, OnUpdate: constants.FK_SET_NULL, OnDelete: constants.FK_RESTRICT, Id: ""}},
			Indexes:     []schema.Index{{Name: "index1", Unique: true, Keys: []schema.Key{{ColId: "userid", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}, {Name: "index2", Unique: false, Keys: []schema.Key{{ColId: "userid", Desc: false, Order: 0}, {ColId: "productid", Desc: true, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}, {Name: "index3", Unique: true, Keys: []schema.Key{{ColId: "productid", Desc: false, Order: 0}, {ColId: "userid", Desc: true, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}}, Id: "",
		},

		"product": {
			Name: "product", Schema: "test", ColIds: []string{"product_id", "product_name"}, ColDefs: map[string]schema.Column{
				"product_id":   {Name: "product_id", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
				"product_name": {Name: "product_name", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			},
			PrimaryKeys: []schema.Key{{ColId: "product_id", Desc: false, Order: 0}},
			ForeignKeys: []schema.ForeignKey(nil),
			Indexes:     []schema.Index(nil), Id: ""},
		"test": schema.Table{Name: "test", Schema: "test", ColIds: []string{"id", "s", "txt", "b", "bs", "bl", "c", "c8", "d", "dec", "f8", "f4", "i8", "i4", "i2", "si", "ts", "tz", "vc", "vc6", "bu"}, ColDefs: map[string]schema.Column{
			"b":   schema.Column{Name: "b", Type: schema.Type{Name: "boolean", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"bl":  schema.Column{Name: "bl", Type: schema.Type{Name: "blob", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"bs":  schema.Column{Name: "bs", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: true, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "", DefaultValue: ddl.DefaultValue{IsPresent: true, Value: ddl.Expression{ExpressionId: "", Statement: "nextval('test11_bs_seq'::regclass)"}}},
			"c":   schema.Column{Name: "c", Type: schema.Type{Name: "char", Mods: []int64{1}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"c8":  schema.Column{Name: "c8", Type: schema.Type{Name: "char", Mods: []int64{8}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"d":   schema.Column{Name: "d", Type: schema.Type{Name: "date", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"dec": schema.Column{Name: "dec", Type: schema.Type{Name: "decimal", Mods: []int64{20, 5}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"f4":  schema.Column{Name: "f4", Type: schema.Type{Name: "float", Mods: []int64{24}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"f8":  schema.Column{Name: "f8", Type: schema.Type{Name: "double", Mods: []int64{53}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"i2":  schema.Column{Name: "i2", Type: schema.Type{Name: "smallint", Mods: []int64{16}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"i4":  schema.Column{Name: "i4", Type: schema.Type{Name: "integer", Mods: []int64{32}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "", AutoGen: ddl.AutoGenCol{Name: constants.AUTO_INCREMENT, GenerationType: constants.AUTO_INCREMENT}},
			"i8":  schema.Column{Name: "i8", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"id":  schema.Column{Name: "id", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"s":   schema.Column{Name: "s", Type: schema.Type{Name: "set", Mods: []int64(nil), ArrayBounds: []int64{-1}}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"si":  schema.Column{Name: "si", Type: schema.Type{Name: "integer", Mods: []int64{32}, ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: true, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "", DefaultValue: ddl.DefaultValue{IsPresent: true, Value: ddl.Expression{ExpressionId: "", Statement: "nextval('test11_s_seq'::regclass)"}}},
			"ts":  schema.Column{Name: "ts", Type: schema.Type{Name: "datetime", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"txt": schema.Column{Name: "txt", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"tz":  schema.Column{Name: "tz", Type: schema.Type{Name: "timestamp", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"vc":  schema.Column{Name: "vc", Type: schema.Type{Name: "varchar", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"vc6": schema.Column{Name: "vc6", Type: schema.Type{Name: "varchar", Mods: []int64{6}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"bu":  schema.Column{Name: "bu", Type: schema.Type{Name: "bigint unsigned", Mods: []int64{20}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""}},
			PrimaryKeys: []schema.Key{schema.Key{ColId: "id", Desc: false, Order: 0}},
			ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test4", ColIds: []string{"id", "txt"}, ReferTableId: "test_ref", ReferColumnIds: []string{"ref_id", "ref_txt"}, OnUpdate: constants.FK_RESTRICT, OnDelete: constants.FK_CASCADE, Id: ""}},
			Indexes:     []schema.Index(nil), Id: ""},
		"test_ref": schema.Table{Name: "test_ref", Schema: "test", ColIds: []string{"ref_id", "ref_txt", "abc"}, ColDefs: map[string]schema.Column{
			"abc":     schema.Column{Name: "abc", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"ref_id":  schema.Column{Name: "ref_id", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"ref_txt": schema.Column{Name: "ref_txt", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""}},
			PrimaryKeys: []schema.Key{schema.Key{ColId: "ref_id", Desc: false, Order: 0}, schema.Key{ColId: "ref_txt", Desc: false, Order: 0}},
			ForeignKeys: []schema.ForeignKey(nil),
			Indexes:     []schema.Index(nil), Id: ""},
		"user": schema.Table{Name: "user", Schema: "test", ColIds: []string{"user_id", "name", "ref"}, ColDefs: map[string]schema.Column{
			"name":    schema.Column{Name: "name", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: true, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "", DefaultValue: ddl.DefaultValue{Value: ddl.Expression{ExpressionId: "", Statement: "'default_name'"}, IsPresent: true}},
			"ref":     schema.Column{Name: "ref", Type: schema.Type{Name: "bigint", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"user_id": schema.Column{Name: "user_id", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: true, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "", DefaultValue: ddl.DefaultValue{Value: ddl.Expression{ExpressionId: "", Statement: "uuid()"}, IsPresent: true}}},
			PrimaryKeys: []schema.Key{schema.Key{ColId: "user_id", Desc: false, Order: 0}},
			ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", ColIds: []string{"ref"}, ReferTableId: "test", ReferColumnIds: []string{"id"}, OnUpdate: constants.FK_CASCADE, OnDelete: constants.FK_SET_NULL, Id: ""}},
			Indexes:     []schema.Index(nil), Id: ""}}
	internal.AssertSrcSchema(t, conv, expectedSchema, conv.SrcSchema)
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func TestProcessSchemaMYSQLPKOrdering(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT (.+) FROM information_schema.tables where table_type = 'BASE TABLE' and (.+)",
			args:  []driver.Value{"test"},
			cols:  []string{"table_name"},
			rows: [][]driver.Value{
				{"pk_order"},
			},
		},
		{
			query: "(?is)SELECT c.table_name.+?FROM information_schema.COLUMNS.+?table_name IN \\(.+\\).+",
			args:  []driver.Value{"test", "pk_order"},
			cols:  []string{"table_name", "column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "generation_expression", "extra"},
			rows: [][]driver.Value{
				{"pk_order", "pk_1", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"pk_order", "pk_2", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
			},
		},
		{
			query: regexp.QuoteMeta(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'information_schema' OR TABLE_SCHEMA = 'INFORMATION_SCHEMA') AND TABLE_NAME = 'CHECK_CONSTRAINTS';`),
			args:  nil,
			cols:  []string{"count"},
			rows: [][]driver.Value{
				{int64(1)},
			},
		},
		{
			query: "(?is)SELECT.+?FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS.+?TABLE_NAME IN.+?",
			args:  []driver.Value{"test", "pk_order"},
			cols:  []string{"table_name", "column_name", "constraint_name", "constraint_type", "check_clause", "ordinal_position"},
			rows: [][]driver.Value{
				{"pk_order", "pk_2", "PRIMARY KEY", "PRIMARY KEY", "PRIMARY KEY", 0},
				{"pk_order", "pk_1", "PRIMARY KEY", "PRIMARY KEY", "PRIMARY KEY", 1},
			},
		},
		{
			query: "(?is)SELECT.+?FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS.+?TABLE_NAME IN.+?",
			args:  []driver.Value{"test", "pk_order"},
			cols:  []string{"table_name", "referenced_table_name", "column_name", "referenced_column_name", "constraint_name", "delete_rule", "update_rule"},
		},
		{
			query: "(?is)SELECT DISTINCT TABLE_NAME, (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+) table_name IN \\(.+\\) (.+)",
			args:  []driver.Value{"test", "pk_order"},
			cols:  []string{"table_name", "index_name", "column_name", "seq_in_index", "collation", "non_unique"},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	isi := InfoSchemaImpl{"test", db, "migration-project-id", profiles.SourceProfile{}, profiles.TargetProfile{}}
	commonInfoSchema := common.InfoSchemaImpl{}
	_, err := commonInfoSchema.GenerateSrcSchema(conv, isi, 1)
	assert.Nil(t, err)
	for tName, table := range conv.SrcSchema {
		for cName, col := range table.ColDefs {
			col.GeneratedColumn.Value.ExpressionId = ""
			col.DefaultValue.Value.ExpressionId = ""
			table.ColDefs[cName] = col
		}
		conv.SrcSchema[tName] = table
	}
	expectedSchema := map[string]schema.Table{
		"pk_order": {
			Name: "pk_order", Schema: "test", ColIds: []string{"pk_1", "pk_2"}, ColDefs: map[string]schema.Column{
				"pk_1": {Name: "pk_1", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
				"pk_2": {Name: "pk_2", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			},
			PrimaryKeys: []schema.Key{{ColId: "pk_2", Desc: false, Order: 0}, {ColId: "pk_1", Desc: false, Order: 0}},
			ForeignKeys: []schema.ForeignKey(nil),
			Indexes:     []schema.Index(nil), Id: "",
		},
	}
	internal.AssertSrcSchema(t, conv, expectedSchema, conv.SrcSchema)
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func TestProcessData(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT (.+) FROM `test`.`te st`",
			cols:  []string{"a a", " b", " c "},
			rows: [][]driver.Value{
				{42.3, 3, "cat"},
				{6.6, 22, "dog"},
				{6.6, "2006-01-02", "dog"},
			}, // Test bad row logic.
		},
	}
	db := mkMockDB(t, ms)
	conv := buildConv(
		ddl.CreateTable{
			Name:   "te_st",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1": {Name: "a_a", Id: "c1", T: ddl.Type{Name: ddl.Float64}},
				"c2": {Name: "Ab", Id: "c2", T: ddl.Type{Name: ddl.Int64}},
				"c3": {Name: "Ac_", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			},
		},
		schema.Table{
			Name:   "te st",
			Id:     "t1",
			Schema: "test",
			ColIds: []string{"c1", "c2", "c3"},
			ColDefs: map[string]schema.Column{
				"c1": {Name: "a a", Id: "c1", Type: schema.Type{Name: "float"}},
				"c2": {Name: " b", Id: "c2", Type: schema.Type{Name: "int"}},
				"c3": {Name: " c ", Id: "c3", Type: schema.Type{Name: "text"}},
			},
			ColNameIdMap: map[string]string{
				"a a": "c1",
				" b":  "c2",
				" c ": "c3",
			},
		})

	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	isi := InfoSchemaImpl{"test", db, "migration-project-id", profiles.SourceProfile{}, profiles.TargetProfile{}}
	commonInfoSchema := common.InfoSchemaImpl{}
	commonInfoSchema.ProcessData(conv, isi, internal.AdditionalDataAttributes{})
	assert.Equal(t,
		[]spannerData{
			{table: "te_st", cols: []string{"a_a", "Ab", "Ac_"}, vals: []interface{}{float64(42.3), int64(3), "cat"}},
			{table: "te_st", cols: []string{"a_a", "Ab", "Ac_"}, vals: []interface{}{float64(6.6), int64(22), "dog"}},
		},
		rows)
	assert.Equal(t, conv.BadRows(), int64(1))
	assert.Equal(t, conv.SampleBadRows(10), []string{"table=te st cols=[a a  b  c ] data=[6.6 2006-01-02 dog]\n"})
	assert.Equal(t, int64(1), conv.Unexpecteds()) // Bad row generates an entry in unexpected.
}

func TestProcessData_MultiCol(t *testing.T) {
	// Tests multi-column behavior of ProcessSQLData (including
	// handling of null columns and synthetic keys). Also tests
	// the combination of ProcessInfoSchema and ProcessSQLData
	// i.e. ProcessSQLData uses the schemas built by
	// ProcessInfoSchema.
	ms := []mockSpec{
		{
			query: "SELECT table_name FROM information_schema.tables where table_type = 'BASE TABLE' and (.+)",
			args:  []driver.Value{"test"},
			cols:  []string{"table_name"},
			rows:  [][]driver.Value{{"test"}},
		},
		{
			query: "(?is)SELECT c.table_name.+?FROM information_schema.COLUMNS.+?table_name IN \\(.+\\).+",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"table_name", "column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "generation_expression", "extra"},
			rows: [][]driver.Value{
				{"test", "a", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"test", "b", "double", "double", "YES", nil, nil, 53, nil, []byte("a+2.0"), "STORED GENERATED"},
				{"test", "c", "bigint", "bigint", "YES", nil, nil, 64, 0, []byte("a+1"), "VIRTUAL GENERATED"},
			},
		},
		{
			query: regexp.QuoteMeta(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'information_schema' OR TABLE_SCHEMA = 'INFORMATION_SCHEMA') AND TABLE_NAME = 'CHECK_CONSTRAINTS';`),
			args:  nil,
			cols:  []string{"count"},
			rows: [][]driver.Value{
				{int64(0)},
			},
		},
		{
			query: "(?is)SELECT.+?FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS.+?TABLE_NAME IN.+?",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"table_name", "column_name", "constraint_name", "constraint_type", "check_clause", "ordinal_position"},
			rows:  [][]driver.Value{},
		},
		{
			query: "(?is)SELECT.+?FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS.+?TABLE_NAME IN.+?",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"table_name", "referenced_table_name", "column_name", "referenced_column_name", "constraint_name", "delete_rule", "update_rule"},
		},
		{
			query: "(?is)SELECT DISTINCT TABLE_NAME, (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+) table_name IN \\(.+\\) (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"table_name", "index_name", "column_name", "seq_in_index", "collation", "non_unique"},
		},
		{
			query: "SELECT (.+) FROM `test`.`test`",
			cols:  []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"cat", 42.3, nil},
				{"dog", nil, 22},
			},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	conv.Source = constants.MYSQL
	conv.SpProjectId = "p"
	conv.SpInstanceId = "i"
	isi := InfoSchemaImpl{"test", db, "migration-project-id", profiles.SourceProfile{}, profiles.TargetProfile{}}
	processSchema := common.ProcessSchemaImpl{}
	mockAccessor := &expressions_api.MockExpressionVerificationAccessor{
		RefreshSpannerClientMock: func(ctx context.Context, project, instance string) error {
			return nil
		},
		VerifyExpressionsMock: func(ctx context.Context, input internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput {
			var outputs []internal.ExpressionVerificationOutput
			for _, ed := range input.ExpressionDetailList {
				outputs = append(outputs, internal.ExpressionVerificationOutput{
					Result:           true,
					Err:              nil,
					ExpressionDetail: ed,
				})
			}
			return internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: outputs,
			}
		},
		VerifyPrimaryKeysExpressionsUsingCreateTableMock: func(ctx context.Context, verifyExpressionsInput internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput {
			return internal.VerifyExpressionsOutput{Err: nil, ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{}}
		},
	}
	schemaToSpanner := common.SchemaToSpannerImpl{
		ExpressionVerificationAccessor: mockAccessor,
		DdlV: &expressions_api.DDLVerifierImpl{
			Expressions: mockAccessor,
		},
	}
	err := processSchema.ProcessSchema(conv, isi, 1, internal.AdditionalSchemaAttributes{}, &schemaToSpanner, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
	assert.Nil(t, err)
	for tName, table := range conv.SpSchema {
		for cName, col := range table.ColDefs {
			col.GeneratedColumn.Value.ExpressionId = ""
			table.ColDefs[cName] = col
		}
		conv.SpSchema[tName] = table
	}
	expectedSchema := map[string]ddl.CreateTable{
		"test": {
			Name:   "test",
			ColIds: []string{"a", "b", "c", "synth_id"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"b": {
					Name: "b", T: ddl.Type{Name: ddl.Float64},
					GeneratedColumn: ddl.GeneratedColumn{
						IsPresent: true,
						Value: ddl.Expression{
							Statement: "(a+2.0)",
						},
						Type: ddl.GeneratedColStored,
					},
				},
				"c": {
					Name: "c", T: ddl.Type{Name: ddl.Int64},
					GeneratedColumn: ddl.GeneratedColumn{
						IsPresent: true,
						Value: ddl.Expression{
							Statement: "(a+1)",
						},
						Type: ddl.GeneratedColVirtual,
					},
				},
				"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Order: 1}},
		},
	}
	internal.AssertSpSchema(conv, t, expectedSchema, stripSchemaComments(conv.SpSchema))
	tableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "test")
	assert.Nil(t, err)
	synthId, _ := internal.GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, "synth_id")
	columnLevelIssues := map[string][]internal.SchemaIssue{
		synthId: []internal.SchemaIssue{
			2,
		},
	}
	expectedIssues := internal.TableIssues{
		ColumnLevelIssues: columnLevelIssues,
	}
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId])
	assert.Equal(t, int64(0), conv.Unexpecteds())
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	commonInfoSchema := common.InfoSchemaImpl{}
	commonInfoSchema.ProcessData(conv, isi, internal.AdditionalDataAttributes{})
	assert.Equal(t, []spannerData{
		{table: "test", cols: []string{"a", "b", "synth_id"}, vals: []interface{}{"cat", float64(42.3), "0"}},
		{table: "test", cols: []string{"a", "c", "synth_id"}, vals: []interface{}{"dog", int64(22), "-9223372036854775808"}},
	},
		rows)
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func TestProcessSchema_Sharded(t *testing.T) {
	// Tests multi-column behavior of ProcessSQLData (including
	// handling of null columns and synthetic keys). Also tests
	// the combination of ProcessInfoSchema and ProcessSQLData
	// i.e. ProcessSQLData uses the schemas built by
	// ProcessInfoSchema.
	ms := []mockSpec{
		{
			query: "SELECT table_name FROM information_schema.tables where table_type = 'BASE TABLE' and (.+)",
			args:  []driver.Value{"test"},
			cols:  []string{"table_name"},
			rows:  [][]driver.Value{{"test"}},
		},
		{
			query: "(?is)SELECT c.table_name.+?FROM information_schema.COLUMNS.+?table_name IN \\(.+\\).+",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"table_name", "column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "generation_expression", "extra"},
			rows: [][]driver.Value{
				{"test", "a", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"test", "b", "double", "double", "YES", nil, nil, 53, nil, []byte("a+2.0"), nil},
				{"test", "c", "bigint", "bigint", "YES", nil, nil, 64, 0, []byte("a+1"), []byte("VIRTUAL GENERATED")},
			},
		},
		{
			query: regexp.QuoteMeta(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'information_schema' OR TABLE_SCHEMA = 'INFORMATION_SCHEMA') AND TABLE_NAME = 'CHECK_CONSTRAINTS';`),
			args:  nil,
			cols:  []string{"count"},
			rows: [][]driver.Value{
				{int64(0)},
			},
		},
		{
			query: "(?is)SELECT.+?FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS.+?TABLE_NAME IN.+?",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"table_name", "column_name", "constraint_name", "constraint_type", "check_clause", "ordinal_position"},
			rows:  [][]driver.Value{},
		},
		{
			query: "(?is)SELECT.+?FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS.+?TABLE_NAME IN.+?",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"table_name", "referenced_table_name", "column_name", "referenced_column_name", "constraint_name", "delete_rule", "update_rule"},
		},
		{
			query: "(?is)SELECT DISTINCT TABLE_NAME, (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+) table_name IN \\(.+\\) (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"table_name", "index_name", "column_name", "seq_in_index", "collation", "non_unique"},
		},
		{
			query: "SELECT (.+) FROM `test`.`test`",
			cols:  []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"cat", 42.3, nil},
				{"dog", nil, 22},
			},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	conv.Source = constants.MYSQL
	conv.SpProjectId = "p"
	conv.SpInstanceId = "i"
	isi := InfoSchemaImpl{"test", db, "migration-project-id", profiles.SourceProfile{}, profiles.TargetProfile{}}
	mockAccessor := &expressions_api.MockExpressionVerificationAccessor{
		RefreshSpannerClientMock: func(ctx context.Context, project, instance string) error {
			return nil
		},
		VerifyExpressionsMock: func(ctx context.Context, input internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput {
			var outputs []internal.ExpressionVerificationOutput
			for _, ed := range input.ExpressionDetailList {
				outputs = append(outputs, internal.ExpressionVerificationOutput{
					Result:           true,
					Err:              nil,
					ExpressionDetail: ed,
				})
			}
			return internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: outputs,
			}
		},
		VerifyPrimaryKeysExpressionsUsingCreateTableMock: func(ctx context.Context, verifyExpressionsInput internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput {
			return internal.VerifyExpressionsOutput{Err: nil, ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{}}
		},
	}
	processSchema := common.ProcessSchemaImpl{}
	schemaToSpanner := common.SchemaToSpannerImpl{
		ExpressionVerificationAccessor: mockAccessor,
		DdlV: &expressions_api.DDLVerifierImpl{
			Expressions: mockAccessor,
		},
	}
	err := processSchema.ProcessSchema(conv, isi, 1, internal.AdditionalSchemaAttributes{IsSharded: true}, &schemaToSpanner, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
	assert.Nil(t, err)
	for tName, table := range conv.SpSchema {
		for cName, col := range table.ColDefs {
			col.GeneratedColumn.Value.ExpressionId = ""
			table.ColDefs[cName] = col
		}
		conv.SpSchema[tName] = table
	}
	expectedSchema := map[string]ddl.CreateTable{
		"test": {
			Name:   "test",
			ColIds: []string{"a", "b", "c", "synth_id", "migration_shard_id"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"b": {
					Name: "b", T: ddl.Type{Name: ddl.Float64},
					GeneratedColumn: ddl.GeneratedColumn{
						IsPresent: true,
						Value: ddl.Expression{
							Statement: "(a+2.0)",
						},
						Type: ddl.GeneratedColStored,
					},
				},
				"c": {
					Name: "c", T: ddl.Type{Name: ddl.Int64},
					GeneratedColumn: ddl.GeneratedColumn{
						IsPresent: true,
						Value: ddl.Expression{
							Statement: "(a+1)",
						},
						Type: ddl.GeneratedColVirtual,
					},
				},
				"synth_id":           {Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
				"migration_shard_id": {Name: "migration_shard_id", T: ddl.Type{Name: ddl.String, Len: 50}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Order: 1}},
		},
	}
	internal.AssertSpSchema(conv, t, expectedSchema, stripSchemaComments(conv.SpSchema))
}

func TestSetRowStats(t *testing.T) {
	ms := []mockSpec{
		{
			query: "SELECT table_name FROM information_schema.tables where table_type = 'BASE TABLE' and (.+)",
			args:  []driver.Value{"test"},
			cols:  []string{"table_name"},
			rows:  [][]driver.Value{{"test1"}, {"test2"}},
		}, {
			query: "SELECT COUNT[(][*][)] FROM `test`.`test1`",
			cols:  []string{"count"},
			rows:  [][]driver.Value{{5}},
		}, {
			query: "SELECT COUNT[(][*][)] FROM `test`.`test2`",
			cols:  []string{"count"},
			rows:  [][]driver.Value{{142}},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	conv.SetDataMode()
	isi := InfoSchemaImpl{"test", db, "migration-project-id", profiles.SourceProfile{}, profiles.TargetProfile{}}
	commonInfoSchema := common.InfoSchemaImpl{}
	commonInfoSchema.SetRowStats(conv, isi)
	assert.Equal(t, int64(5), conv.Stats.Rows["test1"])
	assert.Equal(t, int64(142), conv.Stats.Rows["test2"])
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func mkMockDB(t *testing.T, ms []mockSpec) *sql.DB {
	db, mock, err := sqlmock.New()
	assert.Nil(t, err)
	for _, m := range ms {
		rows := sqlmock.NewRows(m.cols)
		for _, r := range m.rows {
			rows.AddRow(r...)
		}
		if len(m.args) > 0 {
			mock.ExpectQuery(m.query).WithArgs(m.args...).WillReturnRows(rows)
		} else {
			mock.ExpectQuery(m.query).WillReturnRows(rows)
		}
	}
	return db
}

func TestGetConstraints_CheckConstraintsTableExists(t *testing.T) {
	ms := []mockSpec{
		{
			query: regexp.QuoteMeta(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'information_schema' OR TABLE_SCHEMA = 'INFORMATION_SCHEMA') AND TABLE_NAME = 'CHECK_CONSTRAINTS';`),
			cols:  []string{"COUNT(*)"},
			rows:  [][]driver.Value{{1}},
		},
		{
			query: regexp.QuoteMeta(`SELECT DISTINCT t.TABLE_NAME, COALESCE(k.COLUMN_NAME,'') AS COLUMN_NAME, t.CONSTRAINT_NAME, t.CONSTRAINT_TYPE, COALESCE(c.CHECK_CLAUSE, '') AS CHECK_CLAUSE, COALESCE(k.ORDINAL_POSITION, 0) AS ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
            LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
            ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
            AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA 
            AND t.TABLE_NAME = k.TABLE_NAME
            LEFT JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS c
            ON t.CONSTRAINT_NAME = c.CONSTRAINT_NAME
	    AND t.TABLE_SCHEMA = c.CONSTRAINT_SCHEMA
            WHERE t.TABLE_SCHEMA = ? 
            AND t.TABLE_NAME IN (?) 
            ORDER BY t.TABLE_NAME, COALESCE(k.ORDINAL_POSITION, 0);`),
			args: []driver.Value{"test_schema", "test_table"},
			cols: []string{"TABLE_NAME", "COLUMN_NAME", "CONSTRAINT_NAME", "CONSTRAINT_TYPE", "CHECK_CLAUSE", "ORDINAL_POSITION"},
			rows: [][]driver.Value{{"test_table", "column1", "PRIMARY", "PRIMARY KEY", "", 0}, {"test_table", "column2", "check_name", "CHECK", "(column2 > 0)", 0}},
		},
	}
	db := mkMockDB(t, ms)
	isi := InfoSchemaImpl{Db: db, DbName: "test_schema"}
	conv := &internal.Conv{}

	primaryKeys, checkKeys, m, err := isi.GetConstraintsBatch(conv, []common.SchemaAndName{{Schema: "test_schema", Name: "test_table"}})
	assert.NoError(t, err)
	assert.Equal(t, []string{"column1"}, primaryKeys["test_table"])
	assert.Equal(t, len(checkKeys["test_table"]), 1)
	assert.Equal(t, checkKeys["test_table"][0].Name, "check_name")
	assert.Equal(t, checkKeys["test_table"][0].Expr, "(column2 > 0)")
	assert.NotNil(t, m)
}

func TestGetConstraints_CheckConstraintsTableAbsent(t *testing.T) {
	ms := []mockSpec{
		{
			query: regexp.QuoteMeta(`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'information_schema' OR TABLE_SCHEMA = 'INFORMATION_SCHEMA') AND TABLE_NAME = 'CHECK_CONSTRAINTS';`),
			cols:  []string{"COUNT(*)"},
			rows:  [][]driver.Value{{0}},
		},
		{
			query: regexp.QuoteMeta(`SELECT t.TABLE_NAME, k.COLUMN_NAME, t.CONSTRAINT_TYPE
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
            ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
            AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA 
            AND t.TABLE_NAME = k.TABLE_NAME
            WHERE t.TABLE_SCHEMA = ?
            AND t.TABLE_NAME IN (?)
            ORDER BY t.TABLE_NAME, k.ORDINAL_POSITION;`),
			args: []driver.Value{"your_schema", "your_table"},
			cols: []string{"TABLE_NAME", "COLUMN_NAME", "CONSTRAINT_TYPE"},
			rows: [][]driver.Value{{"your_table", "column1", "PRIMARY KEY"}},
		},
	}
	db := mkMockDB(t, ms)
	isi := InfoSchemaImpl{Db: db, DbName: "your_schema"}
	conv := &internal.Conv{}

	primaryKeys, checkKeys, m, err := isi.GetConstraintsBatch(conv, []common.SchemaAndName{{Schema: "your_schema", Name: "your_table"}})
	assert.NoError(t, err)
	assert.Equal(t, []string{"column1"}, primaryKeys["your_table"])
	assert.Empty(t, checkKeys["your_table"])
	assert.NotNil(t, m)
}

func TestGetColumnsBatch(t *testing.T) {
	ms := []mockSpec{
		{
			query: regexp.QuoteMeta("SELECT c.table_name, c.column_name, c.data_type, c.column_type, c.is_nullable, c.column_default, c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.generation_expression, c.extra FROM information_schema.COLUMNS c where table_schema = ? and table_name IN (?,?) ORDER BY c.table_name, c.ordinal_position;"),
			args:  []driver.Value{"test", "user", "cart"},
			cols:  []string{"table_name", "column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "generation_expression", "extra"},
			rows: [][]driver.Value{
				{"cart", "productid", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"cart", "userid", "text", "text", "NO", nil, nil, nil, nil, nil, nil},
				{"user", "user_id", "text", "text", "NO", "uuid()", nil, nil, nil, nil, "DEFAULT_GENERATED"},
			},
		},
	}
	db := mkMockDB(t, ms)
	isi := InfoSchemaImpl{"test", db, "migration-project-id", profiles.SourceProfile{}, profiles.TargetProfile{}}
	conv := internal.MakeConv()

	colDefs, colIds, err := isi.GetColumnsBatch(conv, []common.SchemaAndName{{Schema: "test", Name: "user"}, {Schema: "test", Name: "cart"}})
	assert.NoError(t, err)
	assert.NotNil(t, colDefs)
	assert.NotNil(t, colIds)

	assert.Contains(t, colDefs, "cart")
	assert.Contains(t, colDefs, "user")
	assert.Len(t, colDefs["cart"], 2)
	assert.Len(t, colDefs["user"], 1)
}
