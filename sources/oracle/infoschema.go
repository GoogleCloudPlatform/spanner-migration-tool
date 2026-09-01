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
	"regexp"
	"database/sql"
	"fmt"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

type InfoSchemaImpl struct {
	DbName             string
	Db                 *sql.DB
	MigrationProjectId string
	SourceProfile      profiles.SourceProfile
	TargetProfile      profiles.TargetProfile
}

// GetToDdl function below implement the common.InfoSchema interface.
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// GetTableName returns table name.
func (isi InfoSchemaImpl) GetTableName(dbName string, tableName string) string {
	return tableName
}

// GetRowsFromTable returns a sql Rows object for a table.
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, tableId string) (interface{}, error) {
	tbl := conv.SrcSchema[tableId]
	srcCols := tbl.ColIds
	if len(srcCols) == 0 {
		conv.Unexpected(fmt.Sprintf("Couldn't get source columns for table %s ", tbl.Name))
		return nil, nil
	}
	q := getSelectQuery(isi.DbName, tbl.Schema, tbl.Name, tbl.ColIds, tbl.ColDefs)
	rows, err := isi.Db.Query(q)
	return rows, err
}

func getSelectQuery(srcDb string, schemaName string, tableName string, colIds []string, colDefs map[string]schema.Column) string {
	var selects = make([]string, len(colIds))

	for i, colId := range colIds {
		cn := colDefs[colId].Name
		var s string
		if TimestampReg.MatchString(colDefs[colId].Type.Name) {
			s = fmt.Sprintf(`SYS_EXTRACT_UTC("%s") AS "%s"`, cn, cn)
		} else if len(colDefs[colId].Type.ArrayBounds) == 1 {
			s = fmt.Sprintf(`(SELECT JSON_ARRAYAGG(COLUMN_VALUE RETURNING VARCHAR2(4000))
				FROM TABLE ("%s"."%s")) AS "%s"`, tableName, cn, cn)
		} else {
			switch colDefs[colId].Type.Name {
			case "NUMBER":
				s = fmt.Sprintf(`TO_CHAR("%s") AS "%s"`, cn, cn)
			case "XMLTYPE":
				s = fmt.Sprintf(`CAST(XMLTYPE.getStringVal("%s") AS VARCHAR2(4000)) AS "%s"`, cn, cn)
			case "SDO_GEOMETRY":
				s = fmt.Sprintf(`SDO_UTIL.TO_WKTGEOMETRY("%s") AS "%s"`, cn, cn)
			case "OBJECT":
				s = fmt.Sprintf(`
				(
					CASE WHEN "%s" IS NULL THEN ''
					ELSE
						XMLTYPE("%s").getStringVal()
					END
				) AS "%s"
				`, cn, cn, cn)
			default:
				s = fmt.Sprintf(`"%s"`, cn)
			}
		}
		selects[i] = s
	}

	return fmt.Sprintf(`SELECT %s FROM "%s"."%s"`, strings.Join(selects, ", "), schemaName, tableName)
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
		ProcessDataRow(conv, tableId, commonColIds, srcSchema, spSchema, newValues)
	}
	return nil
}

func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	q := fmt.Sprintf("SELECT COUNT(*) FROM \"%s\".\"%s\"", table.Schema, table.Name)
	var count int64
	err := isi.Db.QueryRow(q).Scan(&count)
	return count, err
}

func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	q := "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = UPPER(:1)"
	rows, err := isi.Db.Query(q, isi.DbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []common.SchemaAndName
	var tableName string
	for rows.Next() {
		err := rows.Scan(&tableName)
		if err == nil {
			tables = append(tables, common.SchemaAndName{Schema: isi.DbName, Name: tableName})
		}
	}
	return tables, nil
}

func (isi InfoSchemaImpl) GetColumnsBatch(conv *internal.Conv, tables []common.SchemaAndName) (map[string]common.TableColumns, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}
	
	owner := strings.ToUpper(isi.DbName)
	
	// Create numbered placeholders :2, :3, etc.
	placeholders := make([]string, len(tables))
	args := make([]interface{}, len(tables)+1)
	args[0] = owner
	for i, name := range tableNames {
		placeholders[i] = fmt.Sprintf(":%d", i+2)
		args[i+1] = name
	}
	
	q := fmt.Sprintf(`SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, IDENTITY_COLUMN 
		FROM ALL_TAB_COLS 
		WHERE OWNER = :1 AND TABLE_NAME IN (%s) 
		ORDER BY TABLE_NAME, COLUMN_ID`, strings.Join(placeholders, ","))

	rows, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colDefs := make(map[string]map[string]schema.Column)
	colIds := make(map[string][]string)

	for rows.Next() {
		var tableName, colName, dataType, isNullable, identityCol string
		var colDefault sql.NullString
		var charMaxLen, numericPrecision, numericScale sql.NullInt64

		err := rows.Scan(&tableName, &colName, &dataType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale, &identityCol)
		if err != nil {
			continue
		}

		colId := internal.GenerateColumnId()
		
		var colAutoGen ddl.AutoGenCol
		if identityCol == "YES" {
			colAutoGen = ddl.AutoGenCol{
				Name:           constants.AUTO_INCREMENT,
				GenerationType: constants.AUTO_INCREMENT,
			}
		}

		c := schema.Column{
			Id:      colId,
			Name:    colName,
			Type:    toType(dataType, charMaxLen, numericPrecision, numericScale),
			NotNull: isNullable == "N",
			AutoGen: colAutoGen,
		}

		if _, ok := colDefs[tableName]; !ok {
			colDefs[tableName] = make(map[string]schema.Column)
		}
		colDefs[tableName][colId] = c
		colIds[tableName] = append(colIds[tableName], colId)
	}

	result := make(map[string]common.TableColumns)
	for _, t := range tables {
		result[t.Name] = common.TableColumns{
			ColDefs: colDefs[t.Name],
			ColIds:  colIds[t.Name],
		}
	}
	return result, nil
}

func toType(dataType string, charLen sql.NullInt64, numericPrecision sql.NullInt64, numericScale sql.NullInt64) schema.Type {
	// Simple mapping for now
	if numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0 {
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	} else if numericPrecision.Valid {
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	} else if charLen.Valid && strings.Contains(dataType, "CHAR") {
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	}
	return schema.Type{Name: dataType}
}

func (isi InfoSchemaImpl) GetConstraintsBatch(conv *internal.Conv, tables []common.SchemaAndName) (map[string]common.TableConstraints, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}
	
	owner := strings.ToUpper(isi.DbName)
	placeholders := make([]string, len(tables))
	args := make([]interface{}, len(tables)+1)
	args[0] = owner
	for i, name := range tableNames {
		placeholders[i] = fmt.Sprintf(":%d", i+2)
		args[i+1] = name
	}

	q := fmt.Sprintf(`SELECT 
			k.table_name,
			k.column_name,
			t.constraint_type,
			t.search_condition,
			t.constraint_name
		FROM all_constraints t
		INNER JOIN all_cons_columns k
		ON (k.constraint_name = t.constraint_name AND k.owner = t.owner AND k.table_name = t.table_name)
		WHERE t.owner = :1 AND k.table_name IN (%s)
		ORDER BY k.table_name, k.position`, strings.Join(placeholders, ","))

	rows, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	primaryKeys := make(map[string][]string)
	m := make(map[string]map[string][]string)
	checkKeys := make(map[string]map[string]schema.CheckConstraint)

	var tableName, col, constraint, constraintName string
	var condition sql.NullString

	for rows.Next() {
		err := rows.Scan(&tableName, &col, &constraint, &condition, &constraintName)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if col == "" || constraint == "" {
			continue
		}
		
		switch constraint {
		case "P":
			primaryKeys[tableName] = append(primaryKeys[tableName], col)
		case "C":
			if _, ok := m[tableName]; !ok {
				m[tableName] = make(map[string][]string)
			}
			if condition.Valid && strings.Contains(condition.String, "IS JSON") {
				m[tableName][col] = append(m[tableName][col], "J")
			}
			m[tableName][col] = append(m[tableName][col], constraint)

			if condition.Valid && !strings.Contains(condition.String, "IS NOT NULL") {
				if _, ok := checkKeys[tableName]; !ok {
					checkKeys[tableName] = make(map[string]schema.CheckConstraint)
				}
				if _, exists := checkKeys[tableName][constraintName]; !exists {
						checkKeys[tableName][constraintName] = schema.CheckConstraint{
							Id:   internal.GenerateCheckConstrainstId(),
							Name: constraintName,
							Expr: "(" + condition.String + ")",
							ExprId: internal.GenerateExpressionId(),
						}
				}
			}

		default:
			if _, ok := m[tableName]; !ok {
				m[tableName] = make(map[string][]string)
			}
			m[tableName][col] = append(m[tableName][col], constraint)
		}
	}

	result := make(map[string]common.TableConstraints)
	for _, t := range tables {
		var checks []schema.CheckConstraint
		if ckMap, ok := checkKeys[t.Name]; ok {
			for _, chk := range ckMap {
				checks = append(checks, chk)
			}
		}

		result[t.Name] = common.TableConstraints{
			PrimaryKeys:       primaryKeys[t.Name],
			ColumnConstraints: m[t.Name],
			CheckConstraints:  checks,
		}
	}
	return result, nil
}

func (isi InfoSchemaImpl) GetForeignKeysBatch(conv *internal.Conv, tables []common.SchemaAndName) (map[string]common.TableForeignKeys, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}
	
	owner := strings.ToUpper(isi.DbName)
	placeholders := make([]string, len(tables))
	args := make([]interface{}, len(tables)+1)
	args[0] = owner
	for i, name := range tableNames {
		placeholders[i] = fmt.Sprintf(":%d", i+2)
		args[i+1] = name
	}

	q := fmt.Sprintf(`SELECT 
			A.table_name,
			B.table_name AS ref_table, 
			A.column_name AS col_name,
			B.column_name AS ref_col_name,
			A.constraint_name AS name
		FROM all_cons_columns A 
		JOIN all_constraints C ON A.owner = C.owner AND A.constraint_name = C.constraint_name
		JOIN all_cons_columns B ON B.owner = C.owner AND B.constraint_name = C.r_constraint_name
		WHERE A.owner = :1 AND A.table_name IN (%s)
		ORDER BY A.table_name, A.position`, strings.Join(placeholders, ","))

	rows, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableName, col, refCol, refTable, fKeyName string
	fKeys := make(map[string]map[string]common.FkConstraint)

	for rows.Next() {
		err := rows.Scan(&tableName, &refTable, &col, &refCol, &fKeyName)
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
		fKeys[tableName][fKeyName] = common.FkConstraint{Name: fKeyName, Table: refTable, Refcols: []string{refCol}, Cols: []string{col}}
	}

	result := make(map[string]common.TableForeignKeys)
	for _, t := range tables {
		var fks []schema.ForeignKey
		if tableFkeys, ok := fKeys[t.Name]; ok {
			for fkName, fkData := range tableFkeys {
				fks = append(fks, schema.ForeignKey{
					Id:               internal.GenerateForeignkeyId(),
					Name:             fkName,
					ColumnNames:      fkData.Cols,
					ReferTableName:   fkData.Table,
					ReferColumnNames: fkData.Refcols,
				})
			}
		}
		result[t.Name] = common.TableForeignKeys{ForeignKeys: fks}
	}
	return result, nil
}

func (isi InfoSchemaImpl) GetIndexesBatch(conv *internal.Conv, tables []common.SchemaAndName, colDefs map[string]common.TableColumns) (map[string][]schema.Index, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}
	
	owner := strings.ToUpper(isi.DbName)
	placeholders := make([]string, len(tables))
	args := make([]interface{}, len(tables)+1)
	args[0] = owner
	for i, name := range tableNames {
		placeholders[i] = fmt.Sprintf(":%d", i+2)
		args[i+1] = name
	}

	q := fmt.Sprintf(`SELECT 
			IC.table_name,
			IC.index_name,
			IC.column_name,
			IC.descend,
			I.uniqueness,
			IE.column_expression,
			I.index_type
		FROM all_ind_columns IC 
		LEFT JOIN all_ind_expressions IE ON IC.index_name = IE.index_name AND IC.column_position = IE.column_position AND IC.index_owner = IE.index_owner
		LEFT JOIN all_indexes I ON IC.index_name = I.index_name AND I.table_owner = IC.index_owner
		WHERE IC.index_owner = :1 AND IC.table_name IN (%s)
		ORDER BY IC.table_name, IC.index_name, IC.column_position`, strings.Join(placeholders, ","))

	rows, err := isi.Db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colNameIdMap := make(map[string]map[string]string)
	for tableName, tableCols := range colDefs {
		colNameIdMap[tableName] = make(map[string]string)
		for colId, col := range tableCols.ColDefs {
			colNameIdMap[tableName][col.Name] = colId
		}
	}

	indexMap := make(map[string]map[string]schema.Index)
	ignoredIndexes := make(map[string]map[string]bool)

	var tableName, name, column, unique, indexType string
	var collation, colexpression sql.NullString

	for rows.Next() {
		if err := rows.Scan(&tableName, &name, &column, &collation, &unique, &colexpression, &indexType); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}

		if _, ok := indexMap[tableName]; !ok {
			indexMap[tableName] = make(map[string]schema.Index)
			ignoredIndexes[tableName] = make(map[string]bool)
		}

		if indexType != "NORMAL" && strings.Contains(colexpression.String, "(") && strings.Contains(colexpression.String, ")") {
			ignoredIndexes[tableName][name] = true
		}

		if colexpression.Valid && !strings.Contains(colexpression.String, "(") && !strings.Contains(colexpression.String, ")") {
			column = colexpression.String[1 : len(colexpression.String)-1]
		}

		if _, found := indexMap[tableName][name]; !found {
			indexMap[tableName][name] = schema.Index{
				Id:     internal.GenerateIndexesId(),
				Name:   name,
				Unique: (unique == "UNIQUE"),
			}
		}

		idx := indexMap[tableName][name]
		idx.Keys = append(idx.Keys, schema.Key{
			ColId: colNameIdMap[tableName][column],
			Desc:  (collation.Valid && collation.String == "DESC"),
		})
		indexMap[tableName][name] = idx
	}

	result := make(map[string][]schema.Index)
	for _, t := range tables {
		var indexes []schema.Index
		if tableIdxs, ok := indexMap[t.Name]; ok {
			for idxName, idxData := range tableIdxs {
				if !ignoredIndexes[t.Name][idxName] {
					indexes = append(indexes, idxData)
				}
			}
		}
		result[t.Name] = indexes
	}
	return result, nil
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

var TimestampReg = regexp.MustCompile(`^TIMESTAMP(\(\d+\))?(\s+WITH(\s+LOCAL)?\s+TIME\s+ZONE)?$`)
var IntervalReg = regexp.MustCompile(`^INTERVAL\s+(YEAR(\(\d+\))?\s+TO\s+MONTH|DAY(\(\d+\))?\s+TO\s+SECOND(\(\d+\))?)$`)
