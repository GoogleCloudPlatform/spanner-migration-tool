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
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	sp "cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

const (
	uuidType           string = "uniqueidentifier"
	geographyType      string = "geography"
	geometryType       string = "geometry"
	timeType           string = "time"
	hierarchyIdType    string = "hierarchyid"
	timestampType      string = "timestamp"
	dateTimeType       string = "datetime"
	dateTime2Type      string = "datetime2"
	dateTimeOffsetType string = "datetimeoffset"
	smallDateTimeType  string = "smalldatetime"
	dateType           string = "date"
)

type InfoSchemaImpl struct {
	DbName string
	Db     *sql.DB
}

// GetToDdl function below implement the common.InfoSchema interface.
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// We leave the 2 functions below empty to be able to pass this as an infoSchema interface. We don't need these for now.
func (isi InfoSchemaImpl) StartChangeDataCapture(ctx context.Context, conv *internal.Conv) (map[string]interface{}, error) {
	return nil, nil
}

func (isi InfoSchemaImpl) StartStreamingMigration(ctx context.Context, client *sp.Client, conv *internal.Conv, streamingInfo map[string]interface{}) error {
	return nil
}

// GetTableName returns table name.
func (isi InfoSchemaImpl) GetTableName(schema string, tableName string) string {
	if schema == "dbo" { // Drop 'dbo' prefix.
		return tableName
	}
	return fmt.Sprintf("%s.%s", schema, tableName)
}

// ProcessDataRows performs data conversion for source database
// 'db'. For each table, we extract data using a "SELECT *" query,
// convert the data to Spanner data (based on the source and Spanner
// schemas), and write it to Spanner.  If we can't get/process data
// for a table, we skip that table and process the remaining tables.
//
// Note that the database/sql library has a somewhat complex model for
// returning data from rows.Scan. Scalar values can be returned using
// the native value used by the underlying driver (by passing
// *interface{} to rows.Scan), or they can be converted to specific go
// types.
// We choose to do all type conversions explicitly ourselves so that
// we can generate more targeted error messages: hence we pass
// *interface{} parameters to row.Scan.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, srcTable string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable) error {
	rowsInterface, err := isi.GetRowsFromTable(conv, srcTable)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get data for table %s : err = %s", srcTable, err))
		return err
	}
	rows := rowsInterface.(*sql.Rows)
	defer rows.Close()
	srcCols, _ := rows.Columns()
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
	return nil
}

// GetRowsFromTable returns a sql Rows object for a table.
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error) {
	tbl := conv.SrcSchema[srcTable]
	//To get only the table name by removing the schema name prefix
	tblName := strings.Replace(srcTable, tbl.Schema+".", "", 1)

	q := getSelectQuery(isi.DbName, tbl.Schema, tblName, tbl.ColNames, tbl.ColDefs)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, err
	}
	return rows, err
}

func getSelectQuery(srcDb string, schemaName string, tableName string, colNames []string, colDefs map[string]schema.Column) string {
	var selects = make([]string, len(colNames))

	for i, cn := range colNames {
		var s string
		switch colDefs[cn].Type.Name {
		case geometryType, geographyType:
			s = fmt.Sprintf("[%s].STAsText() AS %s", cn, cn)
		case uuidType:
			s = fmt.Sprintf("CAST([%s] AS VARCHAR(36)) AS %s", cn, cn)
		case hierarchyIdType:
			s = fmt.Sprintf("CAST([%s] AS VARCHAR(4000)) AS %s", cn, cn)
		case timeType:
			s = fmt.Sprintf("CAST([%s] AS VARCHAR(12)) AS %s", cn, cn)
		case timestampType:
			s = fmt.Sprintf("CAST([%s] AS BIGINT) AS %s", cn, cn)
		case smallDateTimeType, dateTimeType, dateTime2Type, dateTimeOffsetType:
			s = fmt.Sprintf("CONVERT(VARCHAR(33), [%s], 126) AS %s", cn, cn)
		case dateType:
			s = fmt.Sprintf("CONVERT(VARCHAR(10), [%s], 23) AS %s", cn, cn)
		default:
			s = fmt.Sprintf("[%s]", cn)
		}
		selects[i] = s
	}

	return fmt.Sprintf("SELECT %s FROM [%s].[%s].[%s]", strings.Join(selects, ", "), srcDb, schemaName, tableName)
}

// buildVals contructs interface{} value containers to scan row
// results into.  Returns both the underlying containers (as a slice)
// as well as an interface{} of pointers to containers to pass to
// rows.Scan.
func buildVals(n int) (v []interface{}, iv []interface{}) {
	v = make([]interface{}, n)
	for i := range v {
		iv = append(iv, &v[i])
	}
	return v, iv
}

// GetRowCount with number of rows in each table.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	q := fmt.Sprintf(`SELECT COUNT(1) FROM [%s].[%s].[%s];`, isi.DbName, table.Schema, table.Name)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count int64
	if rows.Next() {
		err := rows.Scan(&count)
		return count, err
	}
	return 0, nil
}

// GetTables return list of tables in the selected database.
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	q := `
	SELECT 
		SCH.name AS table_schema, 
		TBL.name AS table_name
	FROM sys.tables AS TBL
	INNER JOIN sys.schemas AS SCH 
	ON SCH.schema_id = TBL.schema_id
	WHERE TBL.type = 'U' AND TBL.is_tracked_by_cdc = 0 AND TBL.is_ms_shipped = 0 AND TBL.name <> 'sysdiagrams'
	`
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}

	defer rows.Close()
	var tableSchema, tableName string
	var tables []common.SchemaAndName
	for rows.Next() {
		rows.Scan(&tableSchema, &tableName)
		tables = append(tables, common.SchemaAndName{Schema: tableSchema, Name: tableName})
	}
	return tables, nil
}

// GetColumns returns a list of Column objects and names
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	q := `
		SELECT 
			column_name, 
			data_type, 
			is_nullable, 
			column_default, 
			character_maximum_length, 
			numeric_precision, 
			numeric_scale
		FROM information_schema.COLUMNS 
		WHERE table_schema = @p1 and table_name = @p2 
		ORDER BY ordinal_position;
	`
	cols, err := isi.Db.Query(q, table.Schema, table.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colDefs := make(map[string]schema.Column)
	var colNames []string
	var colName, dataType string
	var isNullable string
	var colDefault sql.NullString
	// elementDataType
	var charMaxLen, numericPrecision, numericScale sql.NullInt64
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
			// c can be UNIQUE, PRIMARY KEY, FOREIGN KEY,
			// or CHECK (based on msql, sql server, postgres docs).
			// We've already filtered out PRIMARY KEY.
			switch c {
			case "CHECK":
				ignored.Check = true
			case "FOREIGN KEY", "PRIMARY KEY", "UNIQUE":
				// Nothing to do here -- these are handled elsewhere.
			}
		}
		ignored.Default = colDefault.Valid
		c := schema.Column{
			Name:    colName,
			Type:    toType(dataType, charMaxLen, numericPrecision, numericScale),
			NotNull: strings.ToUpper(isNullable) == "NO",
			Ignored: ignored,
		}
		colDefs[colName] = c
		colNames = append(colNames, colName)
	}
	return colDefs, colNames, nil
}

// GetConstraints returns a list of primary keys and by-column map of
// other constraints.  Note: we need to preserve ordinal order of
// columns in primary key constraints.
// Note that foreign key constraints are handled in getForeignKeys.
func (isi InfoSchemaImpl) GetConstraints(conv *internal.Conv, table common.SchemaAndName) ([]string, map[string][]string, error) {
	q := `
		SELECT 
			k.COLUMN_NAME, 
			t.CONSTRAINT_TYPE
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
		INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
			ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
		WHERE k.TABLE_SCHEMA = @p1 AND k.TABLE_NAME = @p2 ORDER BY k.ordinal_position;
	`
	rows, err := isi.Db.Query(q, table.Schema, table.Name)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var primaryKeys []string
	var col, constraint string
	m := make(map[string][]string)
	for rows.Next() {
		err := rows.Scan(&col, &constraint)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if col == "" || constraint == "" {
			conv.Unexpected("Got empty col or constraint")
			continue
		}
		switch constraint {
		case "PRIMARY KEY":
			primaryKeys = append(primaryKeys, col)
		default:
			m[col] = append(m[col], constraint)
		}
	}
	return primaryKeys, m, nil
}

// GetForeignKeys returns a list of all the foreign key constraints.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	q := `
	SELECT 
		OBJECT_SCHEMA_NAME (FK.referenced_object_id) AS [schema_name],
		OBJECT_NAME (FK.referenced_object_id) AS [referenced_table],
		COL_NAME(FKC.parent_object_id, FKC.parent_column_id) AS [column],  
		COL_NAME(FKC.referenced_object_id, FKC.referenced_column_id) AS [referenced_column],  
		FK.name AS [foreign_key_name]
	FROM sys.foreign_keys AS FK  
	INNER JOIN sys.foreign_key_columns AS FKC   
    ON FK.object_id = FKC.constraint_object_id  
	WHERE FK.parent_object_id = OBJECT_ID(@p1);
	`

	rows, err := isi.Db.Query(q, fmt.Sprintf("%s.%s", table.Schema, table.Name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refTable common.SchemaAndName
	var col, refCol, fKeyName string
	fKeys := make(map[string]common.FkConstraint)
	var keyNames []string
	for rows.Next() {
		err := rows.Scan(&refTable.Schema, &refTable.Name, &col, &refCol, &fKeyName)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		tableName := isi.GetTableName(refTable.Schema, refTable.Name)
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.Cols = append(fk.Cols, col)
			fk.Refcols = append(fk.Refcols, refCol)
			fKeys[fKeyName] = fk
			continue
		}
		fKeys[fKeyName] = common.FkConstraint{Name: fKeyName, Table: tableName, Refcols: []string{refCol}, Cols: []string{col}}
		keyNames = append(keyNames, fKeyName)
	}

	sort.Strings(keyNames)
	for _, k := range keyNames {
		foreignKeys = append(foreignKeys,
			schema.ForeignKey{
				Name:         fKeys[k].Name,
				Columns:      fKeys[k].Cols,
				ReferTable:   fKeys[k].Table,
				ReferColumns: fKeys[k].Refcols})
	}
	return foreignKeys, nil
}

// GetIndexes return a list of all indexes for the specified table.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName) ([]schema.Index, error) {
	q2 := `
		SELECT
			IX.name, 
			COL_NAME(IX.object_id, IXC.column_id) as [Column Name],
			IX.is_unique,
			IXC.is_descending_key 
		FROM sys.indexes IX 
		INNER JOIN sys.index_columns IXC 
			ON  IX.object_id = IXC.object_id AND IX.index_id = IXC.index_id
		INNER JOIN sys.tables TAB 
			ON IX.object_id = TAB.object_id 
		WHERE
			IX.is_primary_key = 0          
			AND IX.is_unique_constraint = 0 
			AND TAB.is_ms_shipped = 0   
			AND TAB.name=@p1
			AND TAB.schema_id = SCHEMA_ID(@p2)
			ORDER BY IX.name ;
	`
	rows, err := isi.Db.Query(q2, table.Name, table.Schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, column, isUnique, collation string
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	var indexes []schema.Index
	for rows.Next() {
		if err := rows.Scan(&name, &column, &isUnique, &collation); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}

		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{Name: name, Unique: (isUnique == "true")}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{Column: column, Desc: (collation == "DESC")})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		indexes = append(indexes, indexMap[k])
	}
	return indexes, nil
}

func toType(dataType string, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	case dataType == "numeric" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "numeric" && numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	case dataType == "decimal" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "decimal" && numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	default:
		return schema.Type{Name: dataType}
	}
}

func valsToStrings(vals []interface{}) []string {
	toString := func(val interface{}) string {
		if val == nil {
			return "NULL"
		}
		switch v := val.(type) {
		case []uint8:
			val = string([]byte(v))
		case *interface{}:
			val = *v
		}
		return fmt.Sprintf("%v", val)
	}
	var s []string
	for _, v := range vals {
		s = append(s, toString(v))
	}
	return s
}
