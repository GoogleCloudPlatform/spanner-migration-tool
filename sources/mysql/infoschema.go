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
	"regexp"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql" // The driver should be used via the database/sql package.
	_ "github.com/lib/pq"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

var collationRegex = regexp.MustCompile(constants.DB_COLLATION_REGEX)

// InfoSchemaImpl is MySQL specific implementation for InfoSchema.
type InfoSchemaImpl struct {
	DbName             string
	Db                 *sql.DB
	MigrationProjectId string
	SourceProfile      profiles.SourceProfile
	TargetProfile      profiles.TargetProfile
}

// GetToDdl implement the common.InfoSchema interface.
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// GetTableName returns table name.
func (isi InfoSchemaImpl) GetTableName(dbName string, tableName string) string {
	return tableName
}

// GetRowsFromTable returns a sql Rows object for a table.
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, tableId string) (interface{}, error) {
	srcSchema := conv.SrcSchema[tableId]
	srcCols := []string{}

	for _, srcColId := range srcSchema.ColIds {
		srcCols = append(srcCols, conv.SrcSchema[tableId].ColDefs[srcColId].Name)
	}
	if len(srcCols) == 0 {
		conv.Unexpected(fmt.Sprintf("Couldn't get source columns for table %s ", srcSchema.Name))
		return nil, nil
	}
	// MySQL schema and name can be arbitrary strings.
	// Ideally we would pass schema/name as a query parameter,
	// but MySQL doesn't support this. So we quote it instead.
	colNameList := buildColNameList(srcSchema, srcCols)
	q := fmt.Sprintf("SELECT %s FROM `%s`.`%s`;", colNameList, isi.DbName, srcSchema.Name)
	rows, err := isi.Db.Query(q)
	return rows, err
}

// Building list of column names to support mysql spatial datatypes instead of
// using 'SELECT *' because spatial columns will be fetched using ST_AsText(colName).
func buildColNameList(srcSchema schema.Table, srcColName []string) string {
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

// ProcessData performs data conversion for source database.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, tableId string, srcSchema schema.Table, commonColIds []string, spSchema ddl.CreateTable, additionalAttributes internal.AdditionalDataAttributes) error {
	srcTableName := conv.SrcSchema[tableId].Name
	rowsInterface, err := isi.GetRowsFromTable(conv, tableId)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get data for table %s : err = %s", srcTableName, err))
		return err
	}
	rows := rowsInterface.(*sql.Rows)
	defer rows.Close()
	srcCols, _ := rows.Columns()
	v, scanArgs := buildVals(len(srcCols))
	colNameIdMap := internal.GetSrcColNameIdMap(conv.SrcSchema[tableId])
	for rows.Next() {
		// get RawBytes from data.
		err := rows.Scan(scanArgs...)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't process sql data row: %s", err))
			// Scan failed, so we don't have any data to add to bad rows.
			conv.StatsAddBadRow(srcTableName, conv.DataMode())
			continue
		}
		values := valsToStrings(v)

		newValues, err := common.PrepareValues(conv, tableId, colNameIdMap, commonColIds, srcCols, values)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
			conv.StatsAddBadRow(srcTableName, conv.DataMode())
			conv.CollectBadRow(srcTableName, srcCols, values)
			continue
		}

		ProcessDataRow(conv, tableId, commonColIds, srcSchema, spSchema, newValues, additionalAttributes)
	}
	return nil
}

// GetRowCount with number of rows in each table.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	// MySQL schema and name can be arbitrary strings.
	// Ideally we would pass schema/name as a query parameter,
	// but MySQL doesn't support this. So we quote it instead.
	q := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`;", table.Schema, table.Name)
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
	return 0, nil // Check if 0 is ok to return
}

// GetTables return list of tables in the selected database.
// Note that sql.DB already effectively has the dbName
// embedded within it (dbName is part of the DSN passed to sql.Open),
// but unfortunately there is no way to extract it from sql.DB.
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	// In MySQL, schema is the same as database name.
	q := "SELECT table_name FROM information_schema.tables where table_type = 'BASE TABLE' and table_schema=?"
	rows, err := isi.Db.Query(q, isi.DbName)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}
	defer rows.Close()
	var tableName string
	var tables []common.SchemaAndName
	for rows.Next() {
		rows.Scan(&tableName)
		tables = append(tables, common.SchemaAndName{Schema: isi.DbName, Name: tableName})
	}
	return tables, nil
}






// GetColumnsBatch returns a list of Column objects and names for a batch of tables.
func (isi InfoSchemaImpl) GetColumnsBatch(conv *internal.Conv, tables []common.SchemaAndName) (map[string]map[string]schema.Column, map[string][]string, error) {
	if len(tables) == 0 {
		return nil, nil, nil
	}
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}

	placeholders := make([]string, len(tables))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	q := fmt.Sprintf(`SELECT c.table_name, c.column_name, c.data_type, c.column_type, c.is_nullable, c.column_default, c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.generation_expression, c.extra
              FROM information_schema.COLUMNS c
              where table_schema = ? and table_name IN (%s) ORDER BY c.table_name, c.ordinal_position;`, strings.Join(placeholders, ","))

	args := make([]interface{}, len(tables)+1)
	args[0] = isi.DbName
	for i, name := range tableNames {
		args[i+1] = name
	}

	cols, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get schema for tables: %s", err)
	}
	defer cols.Close()

	colDefs := make(map[string]map[string]schema.Column)
	colIds := make(map[string][]string)

	var tableName, colName, dataType, isNullable, columnType string
	var colDefault, colExtra, colGeneratedExpression sql.NullString
	var charMaxLen, numericPrecision, numericScale sql.NullInt64

	for cols.Next() {
		err := cols.Scan(&tableName, &colName, &dataType, &columnType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale, &colGeneratedExpression, &colExtra)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		colId := internal.GenerateColumnId()
		c := buildColumn(conv, colId, colName, dataType, columnType, isNullable, colDefault, colExtra, colGeneratedExpression, charMaxLen, numericPrecision, numericScale)

		if _, ok := colDefs[tableName]; !ok {
			colDefs[tableName] = make(map[string]schema.Column)
		}
		colDefs[tableName][colId] = c
		colIds[tableName] = append(colIds[tableName], colId)
	}
	return colDefs, colIds, nil
}

func buildColumn(conv *internal.Conv, colId, colName, dataType, columnType, isNullable string, colDefault, colExtra, colGeneratedExpression sql.NullString, charMaxLen, numericPrecision, numericScale sql.NullInt64) schema.Column {
	// It's required as empty string is considered as valid within Database SQL.
	if colGeneratedExpression.String == "" {
		colGeneratedExpression.Valid = false
	}
	ignored := schema.Ignored{}
	ignored.Default = colDefault.Valid
	
	var colAutoGen ddl.AutoGenCol
	if colExtra.String == "auto_increment" {
		colAutoGen = ddl.AutoGenCol{
			Name:           constants.AUTO_INCREMENT,
			GenerationType: constants.AUTO_INCREMENT,
		}
	} else {
		colAutoGen = ddl.AutoGenCol{}
	}

	defaultVal := ddl.DefaultValue{
		IsPresent: colDefault.Valid,
		Value:     ddl.Expression{},
	}
	if colDefault.Valid {
		ty := dataType
		if conv.SpDialect == constants.DIALECT_POSTGRESQL {
			ty = ddl.GetPGType(ddl.Type{Name: ty})
		}
		defaultVal.Value = ddl.Expression{
			ExpressionId: internal.GenerateExpressionId(),
			Statement:    common.SanitizeExpressionsValue(colDefault.String, ty, colExtra.String == constants.DEFAULT_GENERATED),
		}
	}

	generatedColumn := ddl.GeneratedColumn{
		IsPresent: colGeneratedExpression.Valid,
		Value:     ddl.Expression{},
	}
	if colGeneratedExpression.Valid {
		// Defaults to STORED type
		generatedColumn.Type = ddl.GeneratedColStored
		if strings.Contains(strings.ToUpper(colExtra.String), constants.VIRTUAL_GENERATED) {
			generatedColumn.Type = ddl.GeneratedColVirtual
		}
		generatedColumn.Value = ddl.Expression{
			ExpressionId: internal.GenerateExpressionId(),
			Statement:    common.SanitizeExpressionsValue(colGeneratedExpression.String, "", false),
		}
	}

	return schema.Column{
		Id:              colId,
		Name:            colName,
		Type:            toType(dataType, columnType, charMaxLen, numericPrecision, numericScale),
		NotNull:         common.ToNotNull(conv, isNullable),
		Ignored:         ignored,
		AutoGen:         colAutoGen,
		DefaultValue:    defaultVal,
		GeneratedColumn: generatedColumn,
	}
}

// GetConstraintsBatch returns a list of primary keys and by-column map of
// other constraints for a batch of tables. Note: we need to preserve ordinal order of
// columns in primary key constraints.
// Note that foreign key constraints are handled in GetForeignKeysBatch.
func (isi InfoSchemaImpl) GetConstraintsBatch(conv *internal.Conv, tables []common.SchemaAndName) (map[string][]string, map[string][]schema.CheckConstraint, map[string]map[string][]string, error) {
	if len(tables) == 0 {
		return nil, nil, nil, nil
	}
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}

	placeholders := make([]string, len(tables))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	var tableExistsCount int
	// check if CHECK_CONSTRAINTS table exists.
	checkQuery := `SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'information_schema' OR TABLE_SCHEMA = 'INFORMATION_SCHEMA') AND TABLE_NAME = 'CHECK_CONSTRAINTS';`
	err := isi.Db.QueryRow(checkQuery).Scan(&tableExistsCount)
	if err != nil {
		return nil, nil, nil, err
	}

	if tableExistsCount > 0 {
		return isi.getConstraintsWithCheck(conv, tables, placeholders, tableNames)
	} else {
		return isi.getConstraintsWithoutCheck(conv, tables, placeholders, tableNames)
	}
}

func (isi InfoSchemaImpl) getConstraintsWithCheck(conv *internal.Conv, tables []common.SchemaAndName, placeholders []string, tableNames []string) (map[string][]string, map[string][]schema.CheckConstraint, map[string]map[string][]string, error) {
	q := fmt.Sprintf(`SELECT DISTINCT t.TABLE_NAME, COALESCE(k.COLUMN_NAME,'') AS COLUMN_NAME, t.CONSTRAINT_NAME, t.CONSTRAINT_TYPE, COALESCE(c.CHECK_CLAUSE, '') AS CHECK_CLAUSE, COALESCE(k.ORDINAL_POSITION, 0) AS ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
            LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
            ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
            AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA 
            AND t.TABLE_NAME = k.TABLE_NAME
            LEFT JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS c
            ON t.CONSTRAINT_NAME = c.CONSTRAINT_NAME
	    AND t.TABLE_SCHEMA = c.CONSTRAINT_SCHEMA
            WHERE t.TABLE_SCHEMA = ? 
            AND t.TABLE_NAME IN (%s) 
            ORDER BY t.TABLE_NAME, COALESCE(k.ORDINAL_POSITION, 0);`, strings.Join(placeholders, ","))

	args := make([]interface{}, len(tables)+1)
	args[0] = isi.DbName
	for i, name := range tableNames {
		args[i+1] = name
	}

	rows, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	primaryKeys := make(map[string][]string)
	checkKeys := make(map[string][]schema.CheckConstraint)
	m := make(map[string]map[string][]string)

	var tableName, col, constraintType, checkClause, constraintName, ordinal_position string

	for rows.Next() {
		err = rows.Scan(&tableName, &col, &constraintName, &constraintType, &checkClause, &ordinal_position)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan constraints. error: %v", err))
			continue
		}

		if col == "" && constraintType == "" {
			conv.Unexpected("Got empty column or constraint type")
			continue
		}

		switch constraintType {
		case "PRIMARY KEY":
			primaryKeys[tableName] = append(primaryKeys[tableName], col)
		case "CHECK":
			checkClause = collationRegex.ReplaceAllString(checkClause, "")
			checkClause = checkAndAddParentheses(checkClause)
			checkKeys[tableName] = append(checkKeys[tableName], schema.CheckConstraint{Name: constraintName, Expr: checkClause, ExprId: internal.GenerateExpressionId(), Id: internal.GenerateCheckConstrainstId()})
		default:
			if _, ok := m[tableName]; !ok {
				m[tableName] = make(map[string][]string)
			}
			m[tableName][col] = append(m[tableName][col], constraintType)
		}
	}
	return primaryKeys, checkKeys, m, nil
}

func (isi InfoSchemaImpl) getConstraintsWithoutCheck(conv *internal.Conv, tables []common.SchemaAndName, placeholders []string, tableNames []string) (map[string][]string, map[string][]schema.CheckConstraint, map[string]map[string][]string, error) {
	q := fmt.Sprintf(`SELECT t.TABLE_NAME, k.COLUMN_NAME, t.CONSTRAINT_TYPE
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
            ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
            AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA 
            AND t.TABLE_NAME = k.TABLE_NAME
            WHERE t.TABLE_SCHEMA = ?
            AND t.TABLE_NAME IN (%s)
            ORDER BY t.TABLE_NAME, k.ORDINAL_POSITION;`, strings.Join(placeholders, ","))

	args := make([]interface{}, len(tables)+1)
	args[0] = isi.DbName
	for i, name := range tableNames {
		args[i+1] = name
	}

	rows, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	primaryKeys := make(map[string][]string)
	checkKeys := make(map[string][]schema.CheckConstraint)
	m := make(map[string]map[string][]string)

	var tableName, col, constraintType string

	for rows.Next() {
		err = rows.Scan(&tableName, &col, &constraintType)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan constraints. error: %v", err))
			continue
		}

		if col == "" && constraintType == "" {
			conv.Unexpected("Got empty column or constraint type")
			continue
		}

		switch constraintType {
		case "PRIMARY KEY":
			primaryKeys[tableName] = append(primaryKeys[tableName], col)
		default:
			if _, ok := m[tableName]; !ok {
				m[tableName] = make(map[string][]string)
			}
			m[tableName][col] = append(m[tableName][col], constraintType)
		}
	}
	return primaryKeys, checkKeys, m, nil
}

// checkAndAddParentheses this method will check parentheses  if found it will return same string
// or add the parentheses then return the string
func checkAndAddParentheses(checkClause string) string {
	if strings.HasPrefix(checkClause, "(") && strings.HasSuffix(checkClause, ")") {
		return checkClause
	} else {
		return `(` + checkClause + `)`
	}
}

// GetForeignKeysBatch returns list all the foreign keys constraints for a batch of tables.
// MySQL supports cross-database foreign key constraints. We ignore
// them because the Spanner migration tool works database at a time (a specific run
// of the Spanner migration tool focuses on a specific database) and so we can't handle
// them effectively.
func (isi InfoSchemaImpl) GetForeignKeysBatch(conv *internal.Conv, tables []common.SchemaAndName) (map[string][]schema.ForeignKey, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}

	placeholders := make([]string, len(tables))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	q := fmt.Sprintf(`SELECT k.TABLE_NAME,
			k.REFERENCED_TABLE_NAME,
			k.COLUMN_NAME,
			k.REFERENCED_COLUMN_NAME,
			k.CONSTRAINT_NAME,
			r.DELETE_RULE,
			r.UPDATE_RULE
		FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS r
		INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
			ON r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
			AND r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
			AND r.TABLE_NAME = k.TABLE_NAME
			AND r.REFERENCED_TABLE_NAME = k.REFERENCED_TABLE_NAME
			AND k.REFERENCED_TABLE_SCHEMA = k.TABLE_SCHEMA
		WHERE k.TABLE_SCHEMA = ?
			AND k.TABLE_NAME IN (%s)
		ORDER BY
			k.TABLE_NAME,
			k.REFERENCED_TABLE_NAME,
			k.ORDINAL_POSITION;`, strings.Join(placeholders, ","))

	args := make([]interface{}, len(tables)+1)
	args[0] = isi.DbName
	for i, name := range tableNames {
		args[i+1] = name
	}

	rows, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableName, col, refCol, refTable, fKeyName, OnDelete, OnUpdate string
	fKeys := make(map[string]map[string]common.FkConstraint)

	for rows.Next() {
		err := rows.Scan(&tableName, &refTable, &col, &refCol, &fKeyName, &OnDelete, &OnUpdate)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, ok := fKeys[tableName]; !ok {
			fKeys[tableName] = make(map[string]common.FkConstraint)
		}
		if _, found := fKeys[tableName][fKeyName]; found {
			fk := fKeys[tableName][fKeyName]
			fk.Cols = append(fk.Cols, col)
			fk.Refcols = append(fk.Refcols, refCol)
			fKeys[tableName][fKeyName] = fk
			continue
		}
		fKeys[tableName][fKeyName] = common.FkConstraint{Name: fKeyName, Table: refTable, Refcols: []string{refCol}, Cols: []string{col}, OnDelete: OnDelete, OnUpdate: OnUpdate}
	}

	return buildForeignKeys(fKeys), nil
}

func buildForeignKeys(fKeys map[string]map[string]common.FkConstraint) map[string][]schema.ForeignKey {
	foreignKeys := make(map[string][]schema.ForeignKey)
	for tName, keys := range fKeys {
		var keyNames []string
		for k := range keys {
			keyNames = append(keyNames, k)
		}
		sort.Strings(keyNames)
		for _, k := range keyNames {
			foreignKeys[tName] = append(foreignKeys[tName],
				schema.ForeignKey{
					Id:               internal.GenerateForeignkeyId(),
					Name:             keys[k].Name,
					ColumnNames:      keys[k].Cols,
					ReferTableName:   keys[k].Table,
					ReferColumnNames: keys[k].Refcols,
					OnDelete:         keys[k].OnDelete,
					OnUpdate:         keys[k].OnUpdate,
				})
		}
	}
	return foreignKeys
}

// GetIndexesBatch returns a list of all indexes for the specified batch of tables.
func (isi InfoSchemaImpl) GetIndexesBatch(conv *internal.Conv, tables []common.SchemaAndName, colNameIdMap map[string]map[string]string) (map[string][]schema.Index, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}

	placeholders := make([]string, len(tables))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	q := fmt.Sprintf(`SELECT DISTINCT TABLE_NAME, INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE
		FROM INFORMATION_SCHEMA.STATISTICS 
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME IN (%s)
			AND INDEX_NAME != 'PRIMARY' 
		ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;`, strings.Join(placeholders, ","))

	args := make([]interface{}, len(tables)+1)
	args[0] = isi.DbName
	for i, name := range tableNames {
		args[i+1] = name
	}

	rows, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableName, name, column, sequence, nonUnique string
	var collation sql.NullString
	indexMap := make(map[string]map[string]schema.Index)
	indexNames := make(map[string][]string)

	for rows.Next() {
		if err := rows.Scan(&tableName, &name, &column, &sequence, &collation, &nonUnique); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, ok := indexMap[tableName]; !ok {
			indexMap[tableName] = make(map[string]schema.Index)
		}
		if _, found := indexMap[tableName][name]; !found {
			indexNames[tableName] = append(indexNames[tableName], name)
			indexMap[tableName][name] = schema.Index{
				Id:     internal.GenerateIndexesId(),
				Name:   name,
				Unique: (nonUnique == "0"),
			}
		}
		index := indexMap[tableName][name]
		index.Keys = append(index.Keys, schema.Key{
			ColId: colNameIdMap[tableName][column],
			Desc:  (collation.Valid && collation.String == "D"),
		})
		indexMap[tableName][name] = index
	}

	return buildIndexes(indexNames, indexMap), nil
}

func buildIndexes(indexNames map[string][]string, indexMap map[string]map[string]schema.Index) map[string][]schema.Index {
	indexes := make(map[string][]schema.Index)
	for tName, names := range indexNames {
		for _, k := range names {
			indexes[tName] = append(indexes[tName], indexMap[tName][k])
		}
	}
	return indexes
}





func toType(dataType string, columnType string, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	case dataType == "set":
		return schema.Type{Name: dataType, ArrayBounds: []int64{-1}}
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	// We only want to parse the length for tinyints when it is present, in the form tinyint(12). columnType can also be just 'tinyint',
	// in which case we skip this parsing.
	case dataType == "tinyint" && len(columnType) > len("tinyint"):
		var length int64
		_, err := fmt.Sscanf(columnType, "tinyint(%d)", &length)
		if err != nil {
			return schema.Type{Name: dataType}
		}
		return schema.Type{Name: dataType, Mods: []int64{length}}
	case dataType == "bigint" && len(columnType) > len("bigint") && strings.Contains(strings.ToUpper(columnType), "UNSIGNED"):
		if numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0 {
			return schema.Type{Name: "bigint unsigned", Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
		} else if numericPrecision.Valid {
			return schema.Type{Name: "bigint unsigned", Mods: []int64{numericPrecision.Int64}}
		} else {
			return schema.Type{Name: "bigint unsigned"}
		}
	case numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	default:
		return schema.Type{Name: dataType}
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

func createSequence(conv *internal.Conv) ddl.Sequence {
	id := internal.GenerateSequenceId()
	sequenceName := "Sequence" + id[1:]
	sequence := ddl.Sequence{
		Id:           id,
		Name:         sequenceName,
		SequenceKind: "BIT REVERSED SEQUENCE",
	}
	conv.ConvLock.Lock()
	defer conv.ConvLock.Unlock()
	srcSequences := conv.SrcSequences
	srcSequences[id] = sequence
	return sequence
}

