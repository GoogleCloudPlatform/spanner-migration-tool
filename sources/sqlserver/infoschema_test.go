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
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

type mockSpec struct {
	query string
	args  []driver.Value   // Query args.
	cols  []string         // Columns names for returned rows.
	rows  [][]driver.Value // Set of rows returned.
}

func TestProcessSchema(t *testing.T) {
	ms := []mockSpec{
		{
			query: `SELECT (.+) WHERE TBL.type = 'U' AND TBL.is_ms_shipped = 0 AND TBL.name <> 'sysdiagrams'`,
			cols:  []string{"table_schema", "table_name"},
			rows: [][]driver.Value{
				{"dbo", "user"},
				{"dbo", "test"},
				{"dbo", "cart"},
				{"production", "product"},
				{"dbo", "test_ref"},
			},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"dbo", "user"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"user_id", "PRIMARY KEY"},
				{"ref", "FOREIGN KEY"}},
		},
		{
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"dbo.user"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows: [][]driver.Value{
				{"dbo", "test", "ref", "Id", "fk_test"},
			},
		},
		{
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"dbo", "user"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"user_id", "text", "NO", nil, nil, nil, nil},
				{"name", "text", "NO", nil, nil, nil, nil},
				{"ref", "bigint", "YES", nil, nil, nil, nil}},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"user", "dbo"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order", "is_included_column"},
		},
		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"dbo", "test"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"Id", "PRIMARY KEY"},
			},
		}, {
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"dbo.test"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows:  [][]driver.Value{{"dbo", "test_ref", "Id", "ref_id", "fk_test4"}},
		},
		{
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"dbo", "test"},
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
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"test", "dbo"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order", "is_included_column"},
		},

		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"dbo", "cart"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"productid", "PRIMARY KEY"},
				{"userid", "PRIMARY KEY"},
			},
		},
		{
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"dbo.cart"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
			rows: [][]driver.Value{
				{"production", "product", "productid", "product_id", "fk_test2"},
				{"dbo", "user", "userid", "user_id", "fk_test3"}},
		},
		{
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"dbo", "cart"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"productid", "text", "NO", nil, nil, nil, nil},
				{"userid", "text", "NO", nil, nil, nil, nil},
				{"quantity", "bigint", "YES", nil, nil, 64, 0}},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"cart", "dbo"},
			cols:  []string{"index_name", "column_name", "is_unique", "order", "is_included_column"},
			rows: [][]driver.Value{{"index1", "userid", "false", "ASC", "false"},
				{"index2", "userid", "true", "ASC", "false"},
				{"index2", "productid", "true", "DESC", "true"},
				{"index3", "productid", "true", "DESC", "false"},
				{"index3", "userid", "true", "ASC", "false"},
			},
		},

		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"production", "product"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"product_id", "PRIMARY KEY"},
			},
		},
		{
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"production.product"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
		},
		{
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"production", "product"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"product_id", "text", "NO", nil, nil, nil, nil},
				{"product_name", "text", "NO", nil, nil, nil, nil},
			},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"product", "production"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order", "is_included_column"},
		},

		{
			query: "SELECT (.+) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS (.+)",
			args:  []driver.Value{"dbo", "test_ref"},
			cols:  []string{"column_name", "constraint_type"},
			rows: [][]driver.Value{
				{"ref_id", "PRIMARY KEY"},
				{"ref_txt", "PRIMARY KEY"},
			},
		},
		{
			query: "SELECT (.+) FROM sys.foreign_keys AS FK (.+)",
			args:  []driver.Value{"dbo.test_ref"},
			cols:  []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REF_COLUMN_NAME", "CONSTRAINT_NAME"},
		},
		{
			query: "SELECT (.+) FROM information_schema.COLUMNS (.+)",
			args:  []driver.Value{"dbo", "test_ref"},
			cols:  []string{"column_name", "data_type", "is_nullable", "column_default", "character_maximum_length", "numeric_precision", "numeric_scale"},
			rows: [][]driver.Value{
				{"ref_id", "bigint", "NO", nil, nil, 64, 0},
				{"ref_txt", "text", "NO", nil, nil, nil, nil},
				{"abc", "text", "NO", nil, nil, nil, nil},
			},
		},
		// db call to fetch index happens after fetching of column
		{
			query: "SELECT (.+) FROM sys.indexes (.+)",
			args:  []driver.Value{"test_ref", "dbo"},
			cols:  []string{"index_name", "column_name", "column_position", "is_unique", "order", "is_included_column"},
		},
	}
	db := mkMockDB(t, ms)
	conv := internal.MakeConv()
	processSchema := common.ProcessSchemaImpl{}
	err := processSchema.ProcessSchema(conv, InfoSchemaImpl{"test", db}, 1, internal.AdditionalSchemaAttributes{}, &common.SchemaToSpannerImpl{}, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"user": {
			Name:   "user",
			ColIds: []string{"user_id", "name", "ref"},
			ColDefs: map[string]ddl.ColumnDef{
				"user_id": {Name: "user_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"name":    {Name: "name", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"ref":     {Name: "ref", T: ddl.Type{Name: ddl.Int64}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "user_id", Order: 1}},
			ForeignKeys: []ddl.Foreignkey{{Name: "fk_test", ColIds: []string{"ref"}, ReferTableId: "test", ReferColumnIds: []string{"Id"}}}},
		"test": {
			Name: "test",
			ColIds: []string{"Id", "BigInt", "Binary", "Bit", "Char", "Date", "DateTime",
				"DateTime2", "DateTimeOffset", "Decimal", "Float", "Geography", "Geometry", "HierarchyId",
				"Image", "Int", "Money", "NChar", "NText", "Numeric", "NVarChar", "NVarCharMax", "Real", "SmallDateTime",
				"SmallInt", "SmallMoney", "SQLVariant", "Text", "Time", "TimeStamp",
				"TinyInt", "UniqueIdentifier", "VarBinary", "VarBinaryMax", "VarChar", "VarCharMax", "Xml"},
			ColDefs: map[string]ddl.ColumnDef{
				"Id":               {Name: "Id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
				"BigInt":           {Name: "BigInt", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"Binary":           {Name: "Binary", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, NotNull: false},
				"Bit":              {Name: "Bit", T: ddl.Type{Name: ddl.Bool}, NotNull: false},
				"Char":             {Name: "Char", T: ddl.Type{Name: ddl.String, Len: 10, IsArray: false}, NotNull: false},
				"Date":             {Name: "Date", T: ddl.Type{Name: ddl.Date}, NotNull: false},
				"DateTime":         {Name: "DateTime", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"DateTime2":        {Name: "DateTime2", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"DateTimeOffset":   {Name: "DateTimeOffset", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"Decimal":          {Name: "Decimal", T: ddl.Type{Name: ddl.Numeric}, NotNull: false},
				"Float":            {Name: "Float", T: ddl.Type{Name: ddl.Float64}, NotNull: false},
				"Geography":        {Name: "Geography", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Geometry":         {Name: "Geometry", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"HierarchyId":      {Name: "HierarchyId", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Image":            {Name: "Image", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, NotNull: false},
				"Int":              {Name: "Int", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"Money":            {Name: "Money", T: ddl.Type{Name: ddl.Numeric}, NotNull: false},
				"NChar":            {Name: "NChar", T: ddl.Type{Name: ddl.String, Len: 10}, NotNull: false},
				"NText":            {Name: "NText", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Numeric":          {Name: "Numeric", T: ddl.Type{Name: ddl.Numeric}, NotNull: false},
				"NVarChar":         {Name: "NVarChar", T: ddl.Type{Name: ddl.String, Len: 50}, NotNull: false},
				"NVarCharMax":      {Name: "NVarCharMax", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Real":             {Name: "Real", T: ddl.Type{Name: ddl.Float32}, NotNull: false},
				"SmallDateTime":    {Name: "SmallDateTime", T: ddl.Type{Name: ddl.Timestamp}, NotNull: false},
				"SmallInt":         {Name: "SmallInt", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"SmallMoney":       {Name: "SmallMoney", T: ddl.Type{Name: ddl.Numeric}, NotNull: false},
				"SQLVariant":       {Name: "SQLVariant", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Text":             {Name: "Text", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Time":             {Name: "Time", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"TimeStamp":        {Name: "TimeStamp", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"TinyInt":          {Name: "TinyInt", T: ddl.Type{Name: ddl.Int64}, NotNull: false},
				"UniqueIdentifier": {Name: "UniqueIdentifier", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"VarBinary":        {Name: "VarBinary", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, NotNull: false},
				"VarBinaryMax":     {Name: "VarBinaryMax", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, NotNull: false},
				"VarChar":          {Name: "VarChar", T: ddl.Type{Name: ddl.String, Len: 50}, NotNull: false},
				"VarCharMax":       {Name: "VarCharMax", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
				"Xml":              {Name: "Xml", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: false},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "Id", Order: 1}},
			ForeignKeys: []ddl.Foreignkey{{Name: "fk_test4", ColIds: []string{"Id"}, ReferTableId: "test_ref", ReferColumnIds: []string{"ref_id"}}},
		},
		"cart": {
			Name:   "cart",
			ColIds: []string{"productid", "userid", "quantity"},
			ColDefs: map[string]ddl.ColumnDef{
				"productid": {Name: "productid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"userid":    {Name: "userid", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"quantity":  {Name: "quantity", T: ddl.Type{Name: ddl.Int64}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "productid", Order: 1}, {ColId: "userid", Order: 2}},
			ForeignKeys: []ddl.Foreignkey{{Name: "fk_test2", ColIds: []string{"productid"}, ReferTableId: "production_product", ReferColumnIds: []string{"product_id"}},
				{Name: "fk_test3", ColIds: []string{"userid"}, ReferTableId: "user", ReferColumnIds: []string{"user_id"}}},
			Indexes: []ddl.CreateIndex{{Name: "index1", TableId: "cart", Unique: false, Keys: []ddl.IndexKey{{ColId: "userid", Desc: false, Order: 1}}},
				{Name: "index2", TableId: "cart", Unique: true, Keys: []ddl.IndexKey{{ColId: "userid", Desc: false, Order: 1}}, StoredColumnIds: []string{"productid"}},
				{Name: "index3", TableId: "cart", Unique: true, Keys: []ddl.IndexKey{{ColId: "productid", Desc: true, Order: 1}, {ColId: "userid", Desc: false, Order: 2}}}}},
		"production_product": {
			Name:   "production_product",
			ColIds: []string{"product_id", "product_name"},
			ColDefs: map[string]ddl.ColumnDef{
				"product_id":   {Name: "product_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"product_name": {Name: "product_name", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "product_id", Order: 1}}},
		"test_ref": {
			Name:   "test_ref",
			ColIds: []string{"ref_id", "ref_txt", "abc"},
			ColDefs: map[string]ddl.ColumnDef{
				"ref_id":  {Name: "ref_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
				"ref_txt": {Name: "ref_txt", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"abc":     {Name: "abc", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "ref_id", Order: 1}, {ColId: "ref_txt", Order: 2}}},
	}
	internal.AssertSpSchema(conv, t, expectedSchema, stripSchemaComments(conv.SpSchema))

	cartTableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "cart")
	assert.Equal(t, nil, err)
	testTableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "test")
	assert.Equal(t, nil, err)
	assert.Equal(t, len(conv.SchemaIssues[cartTableId].ColumnLevelIssues), 0)
	assert.Equal(t, len(conv.SchemaIssues[testTableId].ColumnLevelIssues), 15)
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
