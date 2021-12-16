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

package sqlserver

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type mockSpec struct {
	query string
	args  []driver.Value   // Query args.
	cols  []string         // Columns names for returned rows.
	rows  [][]driver.Value // Set of rows returned.
}

func TestProcessSchema(t *testing.T) {
	ms := []mockSpec{
		{
			query: `SELECT (.+) WHERE TBL.type = 'U' AND TBL.is_tracked_by_cdc = 0 AND TBL.is_ms_shipped = 0 AND TBL.name <> 'sysdiagrams'`,
			cols:  []string{"table_schema", "table_name"},
			rows: [][]driver.Value{
				{"public", "user"},
				{"public", "test"},
				{"public", "cart"},
				{"public", "product"},
				{"public", "test_ref"},
			},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "user"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"user_id", "PRIMARY KEY"},
				{"ref", "FOREIGN KEY"}},
		}, {
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"public.user"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows: [][]driver.Value{
				{"public", "test", "ref", "Id", "fk_test"},
			},
		}, {
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"user", "public"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order"},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "user"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"user_id", "text", "NO", nil, nil, nil, nil},
				{"name", "text", "NO", nil, nil, nil, nil},
				{"ref", "bigint", "YES", nil, nil, nil, nil}},
		}, {
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "test"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"Id", "PRIMARY KEY"},
			},
		}, {
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"public.test"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows:  [][]driver.Value{{"public", "test_ref", "id", "ref_id", "fk_test4"}},
		}, {
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"test", "public"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order"},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "test"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"Id", "int", "NO", nil, nil, 10, 0},
				{"BigInt", "bigint", "YES", nil, nil, 19, 0},
				{"Binary", "binary", "YES", nil, 50, nil, nil},
				{"Bit", "bit", "YES", nil, nil, nil, nil},
				{"Char", "char", "YES", nil, 10, nil, nil},
				{"Date", "date", "YES", nil, nil, nil, nil},
				{"DateTime", "datetime", "YES", nil, nil, nil, nil},
				{"DateTime2", "datetime2", "YES", nil, nil, nil, nil},
				{"DateTimeOffset", "datetimeoffset", "YES", nil, nil, nil, nil},
				{"Decimal", "decimal", "YES", nil, nil, 18, 9},
				{"Float", "float", "YES", nil, nil, 53, nil},
				{"Geography", "geography", "YES", nil, -1, nil, nil},
				{"Geometry", "geometry", "YES", nil, -1, nil, nil},
				{"HierarchyId", "hierarchyid", "YES", nil, 892, nil, nil},
				{"Image", "image", "YES", nil, 2147483647, nil, nil},
				{"Int", "int", "YES", nil, nil, 10, 0},
				{"Money", "money", "YES", nil, nil, 19, 4},
				{"NChar", "nchar", "YES", nil, 10, nil, nil},
				{"NText", "ntext", "YES", nil, 1073741823, nil, nil},
				{"Numeric", "numeric", "YES", nil, nil, 18, 17},
				{"NVarChar", "nvarchar", "YES", nil, 50, nil, nil},
				{"NVarCharMax", "nvarchar", "YES", nil, -1, nil, nil},
				{"Real", "real", "YES", nil, nil, 24, nil},
				{"SmallDateTime", "smalldatetime", "YES", nil, nil, nil, nil},
				{"SmallInt", "smallint", "YES", nil, nil, 5, 0},
				{"SmallMoney", "smallmoney", "YES", nil, nil, 10, 4},
				{"SQLVariant", "sql_variant", "YES", nil, 0, nil, nil},
				{"Text", "text", "YES", nil, 2147483647, nil, nil},
				{"Time", "time", "YES", nil, nil, nil, nil},
				{"TimeStamp", "timestamp", "YES", nil, nil, nil, nil},
				{"TinyInt", "tinyint", "YES", nil, nil, 3, 0},
				{"UniqueIdentifier", "uniqueidentifier", "YES", nil, nil, nil, nil},
				{"VarBinary", "varbinary", "YES", nil, 50, nil, nil},
				{"VarBinaryMax", "varbinary", "YES", nil, -1, nil, nil},
				{"VarChar", "varchar", "YES", nil, 50, nil, nil},
				{"VarCharMax", "varchar", "YES", nil, -1, nil, nil},
				{"Xml", "xml", "YES", nil, -1, nil, nil},
			},
		},

		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "cart"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"productid", "PRIMARY KEY"},
				{"userid", "PRIMARY KEY"},
			},
		}, {
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"public.cart"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows: [][]driver.Value{
				{"public", "product", "productid", "product_id", "fk_test2"},
				{"public", "user", "userid", "user_id", "fk_test3"}},
		}, {
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"cart", "public"},
			cols:  []string{"index_name", "column_name", "is_unique", "order"},
			rows: [][]driver.Value{{"index1", "userid", "false", "ASC"},
				{"index2", "userid", "true", "ASC"},
				{"index2", "productid", "true", "DESC"},
				{"index3", "productid", "true", "DESC"},
				{"index3", "userid", "true", "ASC"},
			},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "cart"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"productid", "text", "NO", nil, nil, nil, nil},
				{"userid", "text", "NO", nil, nil, nil, nil},
				{"quantity", "bigint", "YES", nil, nil, 64, 0}},
		},

		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "product"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"product_id", "PRIMARY KEY"},
			},
		}, {
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"public.product"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
		}, {
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"product", "public"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order"},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "product"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"product_id", "text", "NO", nil, nil, nil, nil},
				{"product_name", "text", "NO", nil, nil, nil, nil},
			},
		},

		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"public", "test_ref"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"ref_id", "PRIMARY KEY"},
				{"ref_txt", "PRIMARY KEY"},
			},
		}, {
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"public.test_ref"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
		}, {
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"test_ref", "public"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order"},
		}, {
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"public", "test_ref"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"ref_id", "bigint", "NO", nil, nil, 64, 0},
				{"ref_txt", "text", "NO", nil, nil, nil, nil},
				{"abc", "text", "NO", nil, nil, nil, nil},
			},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	err := common.ProcessSchema(conv, InfoSchemaImpl{"test", db})
	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"user": ddl.CreateTable{
			Name:     "user",
			ColNames: []string{"user_id", "name", "ref"},
			ColDefs: map[string]ddl.ColumnDef{
				"user_id": ddl.ColumnDef{Name: "user_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"name":    ddl.ColumnDef{Name: "name", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"ref":     ddl.ColumnDef{Name: "ref", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "user_id"}},
			Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test", Columns: []string{"ref"}, ReferTable: "test", ReferColumns: []string{"Id"}}}},
		"test": ddl.CreateTable{
			Name: "test",
			ColNames: []string{"Id", "BigInt", "Binary", "Bit", "Char", "Date", "DateTime",
				"DateTime2", "DateTimeOffset", "Decimal", "Float", "Geography", "Geometry", "HierarchyId",
				"Image", "Int", "Money", "NChar", "NText", "Numeric", "NVarChar", "NVarCharMax", "Real", "SmallDateTime",
				"SmallInt", "SmallMoney", "SQLVariant", "Text", "Time", "TimeStamp",
				"TinyInt", "UniqueIdentifier", "VarBinary", "VarBinaryMax", "VarChar", "VarCharMax", "Xml"},
			ColDefs: map[string]ddl.ColumnDef{
				"Id":               ddl.ColumnDef{Name: "Id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
				"BigInt":           ddl.ColumnDef{Name: "BigInt", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"Binary":           ddl.ColumnDef{Name: "Binary", T: ddl.Type{Name: ddl.Bytes, Len: 50}, NotNull: false},
				"Bit":              ddl.ColumnDef{Name: "Bit", T: ddl.Type{Name: ddl.Bool}, NotNull: false},
				"Char":             ddl.ColumnDef{Name: "Char", T: ddl.Type{Name: ddl.String, Len: 10, IsArray: false}, NotNull: false},
				"Date":             ddl.ColumnDef{Name: "Date", T: ddl.Type{Name: ddl.Date}, NotNull: false},
				"DateTime":         ddl.ColumnDef{Name: "DateTime", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"DateTime2":        ddl.ColumnDef{Name: "DateTime2", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"DateTimeOffset":   ddl.ColumnDef{Name: "DateTimeOffset", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"Decimal":          ddl.ColumnDef{Name: "Decimal", T: ddl.Type{Name: ddl.Numeric}, NotNull: false},
				"Float":            ddl.ColumnDef{Name: "Float", T: ddl.Type{Name: ddl.Float64}, NotNull: false},
				"Geography":        ddl.ColumnDef{Name: "Geography", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Geometry":         ddl.ColumnDef{Name: "Geometry", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"HierarchyId":      ddl.ColumnDef{Name: "HierarchyId", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Image":            ddl.ColumnDef{Name: "Image", T: ddl.Type{Name: ddl.Bytes, Len: 9223372036854775807}, NotNull: false},
				"Int":              ddl.ColumnDef{Name: "Int", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"Money":            ddl.ColumnDef{Name: "Money", T: ddl.Type{Name: ddl.Numeric}, NotNull: false},
				"NChar":            ddl.ColumnDef{Name: "NChar", T: ddl.Type{Name: ddl.String, Len: 10}, NotNull: false},
				"NText":            ddl.ColumnDef{Name: "NText", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Numeric":          ddl.ColumnDef{Name: "Numeric", T: ddl.Type{Name: ddl.Numeric}, NotNull: false},
				"NVarChar":         ddl.ColumnDef{Name: "NVarChar", T: ddl.Type{Name: ddl.String, Len: 50}, NotNull: false},
				"NVarCharMax":      ddl.ColumnDef{Name: "NVarCharMax", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Real":             ddl.ColumnDef{Name: "Real", T: ddl.Type{Name: ddl.Float64}, NotNull: false},
				"SmallDateTime":    ddl.ColumnDef{Name: "SmallDateTime", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"SmallInt":         ddl.ColumnDef{Name: "SmallInt", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"SmallMoney":       ddl.ColumnDef{Name: "SmallMoney", T: ddl.Type{Name: ddl.Numeric}, NotNull: false},
				"SQLVariant":       ddl.ColumnDef{Name: "SQLVariant", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Text":             ddl.ColumnDef{Name: "Text", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Time":             ddl.ColumnDef{Name: "Time", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"TimeStamp":        ddl.ColumnDef{Name: "TimeStamp", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"TinyInt":          ddl.ColumnDef{Name: "TinyInt", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"UniqueIdentifier": ddl.ColumnDef{Name: "UniqueIdentifier", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"VarBinary":        ddl.ColumnDef{Name: "VarBinary", T: ddl.Type{Name: ddl.Bytes, Len: 50}, NotNull: false},
				"VarBinaryMax":     ddl.ColumnDef{Name: "VarBinaryMax", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, NotNull: false},
				"VarChar":          ddl.ColumnDef{Name: "VarChar", T: ddl.Type{Name: ddl.String, Len: 50}, NotNull: false},
				"VarCharMax":       ddl.ColumnDef{Name: "VarCharMax", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Xml":              ddl.ColumnDef{Name: "Xml", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "Id"}},
			Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test4", Columns: []string{"Id"}, ReferTable: "test_ref", ReferColumns: []string{"ref_id"}}},
		},
		"cart": ddl.CreateTable{
			Name:     "cart",
			ColNames: []string{"productid", "userid", "quantity"},
			ColDefs: map[string]ddl.ColumnDef{
				"productid": ddl.ColumnDef{Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"userid":    ddl.ColumnDef{Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"quantity":  ddl.ColumnDef{Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "productid"}, ddl.IndexKey{Col: "userid"}},
			Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk_test2", Columns: []string{"productid"}, ReferTable: "product", ReferColumns: []string{"product_id"}},
				ddl.Foreignkey{Name: "fk_test3", Columns: []string{"userid"}, ReferTable: "user", ReferColumns: []string{"user_id"}}},
			Indexes: []ddl.CreateIndex{ddl.CreateIndex{Name: "index1", Table: "cart", Unique: false, Keys: []ddl.IndexKey{ddl.IndexKey{Col: "userid", Desc: false}}},
				ddl.CreateIndex{Name: "index2", Table: "cart", Unique: true, Keys: []ddl.IndexKey{ddl.IndexKey{Col: "userid", Desc: false}, ddl.IndexKey{Col: "productid", Desc: true}}},
				ddl.CreateIndex{Name: "index3", Table: "cart", Unique: true, Keys: []ddl.IndexKey{ddl.IndexKey{Col: "productid", Desc: true}, ddl.IndexKey{Col: "userid", Desc: false}}}}},
		"product": ddl.CreateTable{
			Name:     "product",
			ColNames: []string{"product_id", "product_name"},
			ColDefs: map[string]ddl.ColumnDef{
				"product_id":   ddl.ColumnDef{Name: "product_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"product_name": ddl.ColumnDef{Name: "product_name", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "product_id"}}},
		"test_ref": ddl.CreateTable{
			Name:     "test_ref",
			ColNames: []string{"ref_id", "ref_txt", "abc"},
			ColDefs: map[string]ddl.ColumnDef{
				"ref_id":  ddl.ColumnDef{Name: "ref_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
				"ref_txt": ddl.ColumnDef{Name: "ref_txt", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"abc":     ddl.ColumnDef{Name: "abc", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "ref_id"}, ddl.IndexKey{Col: "ref_txt"}}},
	}
	assert.Equal(t, expectedSchema, stripSchemaComments(conv.SpSchema))
	assert.Equal(t, len(conv.Issues["cart"]), 0)
	assert.Equal(t, len(conv.Issues["test"]), 16)
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

// stripSchemaComments returns a schema with all comments removed.
// We mostly ignore schema comments in testing since schema comments
// are often changed and are not a core part of conversion functionality.
func stripSchemaComments(spSchema map[string]ddl.CreateTable) map[string]ddl.CreateTable {
	for t, ct := range spSchema {
		for c, cd := range ct.ColDefs {
			cd.Comment = ""
			ct.ColDefs[c] = cd
		}
		ct.Comment = ""
		spSchema[t] = ct
	}
	return spSchema
}
