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
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
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
			query: "SELECT (.+) FROM information_schema.tables where table_type = 'BASE TABLE'  and (.+)",
			args:  []driver.Value{"test"},
			cols:  []string{"table_name"},
			rows: [][]driver.Value{
				{"user"},
				{"cart"},
				{"product"},
				{"test"},
				{"test_ref"}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "user"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"user_id", "PRIMARY KEY"},
				{"ref", "FOREIGN KEY"}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "user"},
			cols:  []string{"REFERENCED_TABLE_NAME", "COLUMN_NAME", "REFERENCED_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows: [][]driver.Value{
				{"test", "ref", "id", "fk_test"},
			},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"test", "user"},
			cols:  []string{"column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "extra"},
			rows: [][]driver.Value{
				{"user_id", "text", "text", "NO", nil, nil, nil, nil, nil},
				{"name", "text", "text", "NO", nil, nil, nil, nil, nil},
				{"ref", "bigint", "bigint", "NO", nil, nil, nil, nil, nil}},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+)",
			args:  []driver.Value{"test", "user"},
			cols:  []string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE"},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "cart"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"productid", "PRIMARY KEY"},
				{"userid", "PRIMARY KEY"}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "cart"},
			cols:  []string{"REFERENCED_TABLE_NAME", "COLUMN_NAME", "REFERENCED_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows: [][]driver.Value{
				{"product", "productid", "product_id", "fk_test2"},
				{"user", "userid", "user_id", "fk_test3"}},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"test", "cart"},
			cols:  []string{"column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "extra"},
			rows: [][]driver.Value{
				{"productid", "text", "text", "NO", nil, nil, nil, nil, nil},
				{"userid", "text", "text", "NO", nil, nil, nil, nil, nil},
				{"quantity", "bigint", "bigint", "YES", nil, nil, 64, 0, nil}},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+)",
			args:  []driver.Value{"test", "cart"},
			cols:  []string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE"},
			rows: [][]driver.Value{
				{"index1", "userid", 1, sql.NullString{Valid: false}, "0"},
				{"index2", "userid", 1, "A", "1"},
				{"index2", "productid", 2, "D", "1"},
				{"index3", "productid", 1, "A", "0"},
				{"index3", "userid", 2, "D", "0"}},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "product"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"product_id", "PRIMARY KEY"}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "product"},
			cols:  []string{"REFERENCED_TABLE_NAME", "COLUMN_NAME", "REFERENCED_COLUMN_NAME", "CONSTRAINT_NAME"},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"test", "product"},
			cols:  []string{"column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "extra"},
			rows: [][]driver.Value{
				{"product_id", "text", "text", "NO", nil, nil, nil, nil, nil},
				{"product_name", "text", "text", "NO", nil, nil, nil, nil, nil}},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+)",
			args:  []driver.Value{"test", "product"},
			cols:  []string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE"},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"column_name", "constraint_type"},
			rows:  [][]driver.Value{{"id", "PRIMARY KEY"}, {"id", "FOREIGN KEY"}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"REFERENCED_TABLE_NAME", "COLUMN_NAME", "REFERENCED_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows: [][]driver.Value{{"test_ref", "id", "ref_id", "fk_test4"},
				{"test_ref", "txt", "ref_txt", "fk_test4"}},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "extra"},
			rows: [][]driver.Value{
				{"id", "bigint", "bigint", "NO", nil, nil, 64, 0, nil},
				{"s", "set", "set", "YES", nil, nil, nil, nil, nil},
				{"txt", "text", "text", "NO", nil, nil, nil, nil, nil},
				{"b", "boolean", "boolean", "YES", nil, nil, nil, nil, nil},
				{"bs", "bigint", "bigint", "NO", "nextval('test11_bs_seq'::regclass)", nil, 64, 0, nil},
				{"bl", "blob", "blob", "YES", nil, nil, nil, nil, nil},
				{"c", "char", "char(1)", "YES", nil, 1, nil, nil, nil},
				{"c8", "char", "char(8)", "YES", nil, 8, nil, nil, nil},
				{"d", "date", "date", "YES", nil, nil, nil, nil, nil},
				{"dec", "decimal", "decimal(20,5)", "YES", nil, nil, 20, 5, nil},
				{"f8", "double", "double", "YES", nil, nil, 53, nil, nil},
				{"f4", "float", "float", "YES", nil, nil, 24, nil, nil},
				{"i8", "bigint", "bigint", "YES", nil, nil, 64, 0, nil},
				{"i4", "integer", "integer", "YES", nil, nil, 32, 0, "auto_increment"},
				{"i2", "smallint", "smallint", "YES", nil, nil, 16, 0, nil},
				{"si", "integer", "integer", "NO", "nextval('test11_s_seq'::regclass)", nil, 32, 0, nil},
				{"ts", "datetime", "datetime", "YES", nil, nil, nil, nil, nil},
				{"tz", "timestamp", "timestamp", "YES", nil, nil, nil, nil, nil},
				{"vc", "varchar", "varchar", "YES", nil, nil, nil, nil, nil},
				{"vc6", "varchar", "varchar(6)", "YES", nil, 6, nil, nil, nil}},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE"},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "test_ref"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"ref_id", "PRIMARY KEY"},
				{"ref_txt", "PRIMARY KEY"}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "test_ref"},
			cols:  []string{"REFERENCED_TABLE_NAME", "COLUMN_NAME", "REFERENCED_COLUMN_NAME", "CONSTRAINT_NAME"},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"test", "test_ref"},
			cols:  []string{"column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "extra"},
			rows: [][]driver.Value{
				{"ref_id", "bigint", "bigint", "NO", nil, nil, 64, 0, nil},
				{"ref_txt", "text", "text", "NO", nil, nil, nil, nil, nil},
				{"abc", "text", "text", "NO", nil, nil, nil, nil, nil}},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+)",
			args:  []driver.Value{"test", "test_ref"},
			cols:  []string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE"},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	isi := InfoSchemaImpl{"test", db, profiles.SourceProfile{}, profiles.TargetProfile{}}
	err := common.GenerateSrcSchema(conv, isi, 1)
	assert.Nil(t, err)
	expectedSchema := map[string]schema.Table{
		"cart": schema.Table{Name: "cart", Schema: "test", ColIds: []string{"productid", "userid", "quantity"}, ColDefs: map[string]schema.Column{
			"productid": schema.Column{Name: "productid", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"quantity":  schema.Column{Name: "quantity", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"userid":    schema.Column{Name: "userid", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""}},
			PrimaryKeys: []schema.Key{schema.Key{ColId: "productid", Desc: false, Order: 0}, schema.Key{ColId: "userid", Desc: false, Order: 0}},
			ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test2", ColIds: []string{"productid"}, ReferTableId: "product", ReferColumnIds: []string{"product_id"}, OnDelete: "", OnUpdate: "", Id: ""}, schema.ForeignKey{Name: "fk_test3", ColIds: []string{"userid"}, ReferTableId: "user", ReferColumnIds: []string{"user_id"}, OnDelete: "", OnUpdate: "", Id: ""}},
			Indexes:     []schema.Index{schema.Index{Name: "index1", Unique: true, Keys: []schema.Key{schema.Key{ColId: "userid", Desc: false, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}, schema.Index{Name: "index2", Unique: false, Keys: []schema.Key{schema.Key{ColId: "userid", Desc: false, Order: 0}, schema.Key{ColId: "productid", Desc: true, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}, schema.Index{Name: "index3", Unique: true, Keys: []schema.Key{schema.Key{ColId: "productid", Desc: false, Order: 0}, schema.Key{ColId: "userid", Desc: true, Order: 0}}, Id: "", StoredColumnIds: []string(nil)}}, Id: ""},

		"product": schema.Table{Name: "product", Schema: "test", ColIds: []string{"product_id", "product_name"}, ColDefs: map[string]schema.Column{
			"product_id":   schema.Column{Name: "product_id", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"product_name": schema.Column{Name: "product_name", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""}},
			PrimaryKeys: []schema.Key{schema.Key{ColId: "product_id", Desc: false, Order: 0}}, ForeignKeys: []schema.ForeignKey(nil), Indexes: []schema.Index(nil), Id: ""},
		"test": schema.Table{Name: "test", Schema: "test", ColIds: []string{"id", "s", "txt", "b", "bs", "bl", "c", "c8", "d", "dec", "f8", "f4", "i8", "i4", "i2", "si", "ts", "tz", "vc", "vc6"}, ColDefs: map[string]schema.Column{
			"b":   schema.Column{Name: "b", Type: schema.Type{Name: "boolean", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"bl":  schema.Column{Name: "bl", Type: schema.Type{Name: "blob", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"bs":  schema.Column{Name: "bs", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: true, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"c":   schema.Column{Name: "c", Type: schema.Type{Name: "char", Mods: []int64{1}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"c8":  schema.Column{Name: "c8", Type: schema.Type{Name: "char", Mods: []int64{8}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"d":   schema.Column{Name: "d", Type: schema.Type{Name: "date", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"dec": schema.Column{Name: "dec", Type: schema.Type{Name: "decimal", Mods: []int64{20, 5}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"f4":  schema.Column{Name: "f4", Type: schema.Type{Name: "float", Mods: []int64{24}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"f8":  schema.Column{Name: "f8", Type: schema.Type{Name: "double", Mods: []int64{53}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"i2":  schema.Column{Name: "i2", Type: schema.Type{Name: "smallint", Mods: []int64{16}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"i4":  schema.Column{Name: "i4", Type: schema.Type{Name: "integer", Mods: []int64{32}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: true}, Id: ""},
			"i8":  schema.Column{Name: "i8", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"id":  schema.Column{Name: "id", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"s":   schema.Column{Name: "s", Type: schema.Type{Name: "set", Mods: []int64(nil), ArrayBounds: []int64{-1}}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"si":  schema.Column{Name: "si", Type: schema.Type{Name: "integer", Mods: []int64{32}, ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: true, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"ts":  schema.Column{Name: "ts", Type: schema.Type{Name: "datetime", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"txt": schema.Column{Name: "txt", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"tz":  schema.Column{Name: "tz", Type: schema.Type{Name: "timestamp", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"vc":  schema.Column{Name: "vc", Type: schema.Type{Name: "varchar", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"vc6": schema.Column{Name: "vc6", Type: schema.Type{Name: "varchar", Mods: []int64{6}, ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""}},
			PrimaryKeys: []schema.Key{schema.Key{ColId: "id", Desc: false, Order: 0}}, ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test4", ColIds: []string{"id", "txt"}, ReferTableId: "test_ref", ReferColumnIds: []string{"ref_id", "ref_txt"}, OnDelete: "", OnUpdate: "", Id: ""}}, Indexes: []schema.Index(nil), Id: ""},
		"test_ref": schema.Table{Name: "test_ref", Schema: "test", ColIds: []string{"ref_id", "ref_txt", "abc"}, ColDefs: map[string]schema.Column{
			"abc":     schema.Column{Name: "abc", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"ref_id":  schema.Column{Name: "ref_id", Type: schema.Type{Name: "bigint", Mods: []int64{64}, ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"ref_txt": schema.Column{Name: "ref_txt", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""}},
			PrimaryKeys: []schema.Key{schema.Key{ColId: "ref_id", Desc: false, Order: 0}, schema.Key{ColId: "ref_txt", Desc: false, Order: 0}}, ForeignKeys: []schema.ForeignKey(nil), Indexes: []schema.Index(nil), Id: ""},
		"user": schema.Table{Name: "user", Schema: "test", ColIds: []string{"user_id", "name", "ref"}, ColDefs: map[string]schema.Column{
			"name":    schema.Column{Name: "name", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"ref":     schema.Column{Name: "ref", Type: schema.Type{Name: "bigint", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""},
			"user_id": schema.Column{Name: "user_id", Type: schema.Type{Name: "text", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: ""}},
			PrimaryKeys: []schema.Key{schema.Key{ColId: "user_id", Desc: false, Order: 0}}, ForeignKeys: []schema.ForeignKey{schema.ForeignKey{Name: "fk_test", ColIds: []string{"ref"}, ReferTableId: "test", ReferColumnIds: []string{"id"}, OnDelete: "", OnUpdate: "", Id: ""}}, Indexes: []schema.Index(nil), Id: ""}}
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
				{6.6, "2006-01-02", "dog"}}, // Test bad row logic.
		},
	}
	db := mkMockDB(t, ms)
	conv := buildConv(
		ddl.CreateTable{
			Name:   "te_st",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1": ddl.ColumnDef{Name: "a_a", Id: "c1", T: ddl.Type{Name: ddl.Float64}},
				"c2": ddl.ColumnDef{Name: "Ab", Id: "c2", T: ddl.Type{Name: ddl.Int64}},
				"c3": ddl.ColumnDef{Name: "Ac_", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			},
		},
		schema.Table{
			Name:   "te st",
			Id:     "t1",
			Schema: "test",
			ColIds: []string{"c1", "c2", "c3"},
			ColDefs: map[string]schema.Column{
				"c1": schema.Column{Name: "a a", Id: "c1", Type: schema.Type{Name: "float"}},
				"c2": schema.Column{Name: " b", Id: "c2", Type: schema.Type{Name: "int"}},
				"c3": schema.Column{Name: " c ", Id: "c3", Type: schema.Type{Name: "text"}},
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
	isi := InfoSchemaImpl{"test", db, profiles.SourceProfile{}, profiles.TargetProfile{}}
	common.ProcessData(conv, isi)
	assert.Equal(t,
		[]spannerData{
			spannerData{table: "te_st", cols: []string{"a_a", "Ab", "Ac_"}, vals: []interface{}{float64(42.3), int64(3), "cat"}},
			spannerData{table: "te_st", cols: []string{"a_a", "Ab", "Ac_"}, vals: []interface{}{float64(6.6), int64(22), "dog"}},
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
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"column_name", "constraint_type"},
			rows:  [][]driver.Value{}, // No primary key --> force generation of synthetic key.
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"REFERENCED_TABLE_NAME", "COLUMN_NAME", "REFERENCED_COLUMN_NAME", "CONSTRAINT_NAME"},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"column_name", "data_type", "column_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale", "extra"},
			rows: [][]driver.Value{
				{"a", "text", "text", "NO", nil, nil, nil, nil, nil},
				{"b", "double", "double", "YES", nil, nil, 53, nil, nil},
				{"c", "bigint", "bigint", "YES", nil, nil, 64, 0, nil}},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.STATISTICS (.+)",
			args:  []driver.Value{"test", "test"},
			cols:  []string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE"},
		},
		{
			query: "SELECT (.+) FROM `test`.`test`",
			cols:  []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"cat", 42.3, nil},
				{"dog", nil, 22}},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	isi := InfoSchemaImpl{"test", db, profiles.SourceProfile{}, profiles.TargetProfile{}}
	err := common.ProcessSchema(conv, isi, 1)
	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"test": ddl.CreateTable{
			Name:   "test",
			ColIds: []string{"a", "b", "c", "synth_id"},
			ColDefs: map[string]ddl.ColumnDef{
				"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
				"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
				"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.String, Len: 50}},
			},
			PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "synth_id", Order: 1}}},
	}
	internal.AssertSpSchema(conv, t, expectedSchema, stripSchemaComments(conv.SpSchema))
	columnLevelIssues := make(map[string][]internal.SchemaIssue)
	expectedIssues := internal.TableIssues{
		ColumnLevelIssues: columnLevelIssues,
	}
	tableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "test")
	assert.Equal(t, nil, err)
	assert.Equal(t, expectedIssues, conv.SchemaIssues[tableId])
	assert.Equal(t, int64(0), conv.Unexpecteds())
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	common.ProcessData(conv, isi)
	assert.Equal(t, []spannerData{
		{table: "test", cols: []string{"a", "b", "synth_id"}, vals: []interface{}{"cat", float64(42.3), "0"}},
		{table: "test", cols: []string{"a", "c", "synth_id"}, vals: []interface{}{"dog", int64(22), "-9223372036854775808"}}},
		rows)
	assert.Equal(t, int64(0), conv.Unexpecteds())
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
	isi := InfoSchemaImpl{"test", db, profiles.SourceProfile{}, profiles.TargetProfile{}}
	common.SetRowStats(conv, isi)
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
