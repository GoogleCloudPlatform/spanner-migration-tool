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
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	_ "github.com/go-sql-driver/mysql" // The driver should be used via the database/sql package.
	_ "github.com/lib/pq"
)

// MySQL specific implementation for InfoSchema
type MySQLInfoSchema struct {
	dbName string
}

func (mis MySQLInfoSchema) GetBaseDdl() common.BaseToDdl {
	return MySQLToSpannerDdl{}
}

func (mis MySQLInfoSchema) GetDbName() string {
	return mis.dbName
}

func (mis MySQLInfoSchema) GetIgnoredSchemas() map[string]bool {
	return make(map[string]bool)
}

// Building list of column names to support mysql spatial datatypes instead of
// using 'SELECT *' because spatial columns will be fetched using ST_AsText(colName).
func (mis MySQLInfoSchema) BuildColNameList(srcSchema schema.Table, srcColName []string) string {
	var srcColTypes []string
	var colList, colTmpName string
	for _, colName := range srcColName {
		// To handle cases where column name is reserved keyword or having space between words.
		colTmpName = "`" + colName + "`"
		srcColTypes = append(srcColTypes, srcSchema.ColDefs[colName].Type.Name)
		for _, spatial := range MysqlSpatialDataTypes {
			if strings.Contains(strings.ToLower(srcSchema.ColDefs[colName].Type.Name), spatial) {
				colTmpName = "ST_AsText" + "(" + colTmpName + ")" + colTmpName
				break
			}
		}
		colList = colList + colTmpName + ","
	}
	return colList[:len(colList)-1]
}

func (mis MySQLInfoSchema) GetTableName(dbName string, tableName string) string {
	return fmt.Sprintf("`%s`.`%s`", dbName, tableName)
}

func (mis MySQLInfoSchema) GetTablesQuery() string {
	return fmt.Sprintf("SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE' and table_schema=`%s`",
		mis.dbName)
}

func (mis MySQLInfoSchema) GetColumnsQuery() string {
	return `SELECT c.column_name, c.data_type, c.column_type, c.is_nullable, c.column_default, c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.extra
	FROM information_schema.COLUMNS c
	where table_schema = ? and table_name = ? ORDER BY c.ordinal_position;`
}

func (mis MySQLInfoSchema) GetConstraintsQuery() string {
	return `SELECT k.COLUMN_NAME, t.CONSTRAINT_TYPE
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
	  INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
		ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND t.TABLE_NAME=k.TABLE_NAME
	WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? ORDER BY k.ordinal_position;`
}

func (mis MySQLInfoSchema) GetForeignKeysQuery() string {
	return `SELECT k.TABLE_SCHEMA, k.REFERENCED_TABLE_NAME,k.COLUMN_NAME,k.REFERENCED_COLUMN_NAME,k.CONSTRAINT_NAME
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t 
	INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
		ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
		AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA 
		AND t.TABLE_NAME = k.TABLE_NAME 
		AND k.REFERENCED_TABLE_SCHEMA = k.TABLE_SCHEMA
	WHERE k.TABLE_SCHEMA = ? 
		AND k.TABLE_NAME = ? 
		AND t.CONSTRAINT_TYPE = "FOREIGN KEY" 
	ORDER BY
		k.REFERENCED_TABLE_NAME,
		k.COLUMN_NAME,
		k.ORDINAL_POSITION;`
}

func (mis MySQLInfoSchema) GetIndexesQuery() string {
	return `SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NOT NON_UNIQUE AS IS_UNIQUE
		FROM INFORMATION_SCHEMA.STATISTICS 
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
			AND INDEX_NAME != 'PRIMARY' 
		ORDER BY INDEX_NAME, SEQ_IN_INDEX;`
}

func (mis MySQLInfoSchema) ToType(dataType string, columnType string, colExtra sql.NullString, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	case dataType == "set":
		return schema.Type{Name: dataType, ArrayBounds: []int64{-1}}
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	case dataType == "decimal" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "decimal" && numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	default:
		return schema.Type{Name: dataType}
	}
}

func (mis MySQLInfoSchema) ProcessDataRows(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, rows *sql.Rows) {
	v, scanArgs := buildVals(len(srcCols))
	for rows.Next() {
		// get RawBytes from data.
		err := rows.Scan(scanArgs...)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't process sql data row: %s", err))
			// Scan failed, so we don't have any data to add to bad rows.
			conv.StatsAddBadRow(srcTable, conv.DataMode())
			continue
		}
		values := valsToStrings(v)
		ProcessDataRow(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, values)
	}
}

// buildVals constructs []sql.RawBytes value containers to scan row
// results into.  Returns both the underlying containers (as a slice)
// as well as an interface{} of pointers to containers to pass to
// rows.Scan.
func buildVals(n int) (v []sql.RawBytes, iv []interface{}) {
	v = make([]sql.RawBytes, n)
	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice.
	iv = make([]interface{}, len(v))
	for i := range v {
		iv[i] = &v[i]
	}
	return v, iv
}

func valsToStrings(vals []sql.RawBytes) []string {
	toString := func(val sql.RawBytes) string {
		if val == nil {
			return "NULL"
		}
		return string(val)
	}
	var s []string
	for _, v := range vals {
		s = append(s, toString(v))
	}
	return s
}
