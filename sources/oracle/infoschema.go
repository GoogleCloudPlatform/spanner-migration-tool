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
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	sp "cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/streaming"
)

type InfoSchemaImpl struct {
	DbName        string
	Db            *sql.DB
	SourceProfile profiles.SourceProfile
	TargetProfile profiles.TargetProfile
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
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error) {
	tbl := conv.SrcSchema[srcTable]
	srcCols := tbl.ColNames
	if len(srcCols) == 0 {
		conv.Unexpected(fmt.Sprintf("Couldn't get source columns for table %s ", srcTable))
		return nil, nil
	}
	q := getSelectQuery(isi.DbName, tbl.Schema, tbl.Name, tbl.ColNames, tbl.ColDefs)
	rows, err := isi.Db.Query(q)
	return rows, err
}

func getSelectQuery(srcDb string, schemaName string, tableName string, colNames []string, colDefs map[string]schema.Column) string {
	var selects = make([]string, len(colNames))

	for i, cn := range colNames {
		var s string
		if TimestampReg.MatchString(colDefs[cn].Type.Name) {
			s = fmt.Sprintf(`SYS_EXTRACT_UTC("%s") AS "%s"`, cn, cn)
		} else if len(colDefs[cn].Type.ArrayBounds) == 1 {
			s = fmt.Sprintf(`(SELECT JSON_ARRAYAGG(COLUMN_VALUE RETURNING VARCHAR2(4000)) 
				FROM TABLE ("%s"."%s")) AS "%s"`, tableName, cn, cn)
		} else {
			switch colDefs[cn].Type.Name {
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

// GetRowCount with number of rows in each table.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	q := fmt.Sprintf(`SELECT count(*) FROM "%s"`, table.Name)
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

func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	q := fmt.Sprintf("SELECT table_name FROM all_tables WHERE owner = '%s'", isi.DbName)
	rows, err := isi.Db.Query(q)
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

// GetColumns returns a list of Column objects and names
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	q := fmt.Sprintf(`
						SELECT 
						column_name, 
						data_type, 
						nullable, 
						data_default, 
						data_length, 
						data_precision, 
						data_scale,
						at.typecode,
						act.elem_type_name,
						act.length,
						act.precision,
						act.scale
					FROM all_tab_columns atc
					LEFT JOIN all_types at ON atc.data_type=at.type_name AND atc.owner = at.owner
					LEFT JOIN all_coll_types act ON atc.data_type=act.type_name AND atc.owner = at.owner
					WHERE atc.owner = '%s' AND atc.table_name = '%s'
					`, table.Schema, table.Name)
	cols, err := isi.Db.Query(q)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colDefs := make(map[string]schema.Column)
	var colNames []string
	var colName, dataType string
	var isNullable string
	var colDefault, typecode, elementDataType sql.NullString
	var charMaxLen, numericPrecision, numericScale, elementCharMaxLen, elementNumericPrecision, elementNumericScale sql.NullInt64
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale, &typecode, &elementDataType, &elementCharMaxLen, &elementNumericPrecision, &elementNumericScale)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
			// Type of constraint definition in oracle C (check constraint on a table)
			// P (primary key), U (unique key) ,R (referential integrity), V (with check option, on a view)
			// O (with read only, on a view).
			// We've already filtered out PRIMARY KEY.
			switch c {
			case "C":
				ignored.Check = true
			// Oracle 21c introduces a JSON datatype, before that we used to store json as VARCHAR2, CLOB, and BLOB.
			// If column has check constraints IS JSON(check for J in constraints array as per GetConstraints function) then update src datatype to JSON
			// so toSpannerTypeInternal function map this datatype to spanner JSON.
			case "J":
				dataType = "JSON"
				charMaxLen.Valid = false
			}
		}
		if typecode.Valid && typecode.String == "OBJECT" {
			dataType = "OBJECT"
			charMaxLen.Valid = false
		}

		ignored.Default = colDefault.Valid
		c := schema.Column{
			Name:    colName,
			Type:    toType(dataType, typecode, elementDataType, charMaxLen, numericPrecision, numericScale, elementCharMaxLen, elementNumericPrecision, elementNumericScale),
			NotNull: strings.ToUpper(isNullable) == "N",
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
	q := fmt.Sprintf(`
					SELECT 
						k.column_name,
						t.constraint_type,
						t.search_condition
	   				FROM all_constraints t
       				INNER JOIN all_cons_columns k
       				ON (k.constraint_name = t.constraint_name) 
					WHERE t.owner = '%s' AND k.table_name = '%s'
					`, table.Schema, table.Name)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var primaryKeys []string
	var col, constraint string
	var condition sql.NullString
	m := make(map[string][]string)
	for rows.Next() {
		err := rows.Scan(&col, &constraint, &condition)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if col == "" || constraint == "" {
			conv.Unexpected("Got empty col or constraint")
			continue
		}
		// P (primary key) constraint in oracle
		switch constraint {
		case "P":
			primaryKeys = append(primaryKeys, col)
		case "C":
			// If column has IS JSON check constraints then add extra string "J" in constraints array.
			// condition value example `column_name IS JSON`,null etc.
			if condition.Valid && strings.Contains(condition.String, "IS JSON") {
				m[col] = append(m[col], "J")
			}
			m[col] = append(m[col], constraint)
		default:
			m[col] = append(m[col], constraint)
		}
	}
	return primaryKeys, m, nil
}

// GetForeignKeys return list all the foreign keys constraints.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	q := fmt.Sprintf(`
						SELECT 
							B.table_name AS ref_table, 
							A.column_name AS col_name,
							B.column_name AS ref_col_name,
							A.constraint_name AS name
						FROM all_cons_columns A 
						JOIN all_constraints C ON A.owner = C.owner AND A.constraint_name = C.constraint_name
						JOIN all_cons_columns B ON B.owner = C.owner AND B.constraint_name = C.r_constraint_name
						WHERE A.table_name='%s' AND A.owner='%s'
					`, table.Name, isi.DbName)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var col, refCol, refTable, fKeyName string
	fKeys := make(map[string]common.FkConstraint)
	var keyNames []string

	for rows.Next() {
		err := rows.Scan(&refTable, &col, &refCol, &fKeyName)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.Cols = append(fk.Cols, col)
			fk.Refcols = append(fk.Refcols, refCol)
			fKeys[fKeyName] = fk
			continue
		}
		fKeys[fKeyName] = common.FkConstraint{Name: fKeyName, Table: refTable, Refcols: []string{refCol}, Cols: []string{col}}
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
// Oracle db support several types of index:
// 1. Normal indexes. (By default, Oracle Database creates B-tree indexes.)
// 2.Bitmap indexes
// 3.Partitioned indexes
// 4. Function-based indexes
// 5.Domain indexes,
// we are only considering normal index as of now.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName) ([]schema.Index, error) {
	q := fmt.Sprintf(`
					SELECT 
						IC.index_name,
						IC.column_name,
						IC.column_position, 
						IC.descend,
						I.uniqueness, 
						IE.column_expression, 
						I.index_type 
                	FROM  all_ind_columns IC 
					LEFT JOIN all_ind_expressions IE ON IC.index_name = IE.index_name 
						AND IC.column_position=IE.column_position
						AND IC.index_owner = IE.index_owner
                	LEFT JOIN all_indexes I ON IC.index_name = I.index_name
						 AND I.table_owner = IC.index_owner
                	WHERE IC.index_owner='%s' AND IC.table_name='%s'
            		ORDER BY IC.index_name, IC.column_position
				`, table.Schema, table.Name)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, column, sequence, Unique, indexType string
	var collation, colexpression sql.NullString
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	ignoredIndex := make(map[string]bool)
	var indexes []schema.Index
	for rows.Next() {
		if err := rows.Scan(&name, &column, &sequence, &collation, &Unique, &colexpression, &indexType); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		// ingnore all index except normal
		// UPPER("EMAIL") check for the function call with "(",")"
		if indexType != "NORMAL" && strings.Contains(colexpression.String, "(") && strings.Contains(colexpression.String, ")") {
			ignoredIndex[name] = true
		}

		//INDEX1_LAST	SYS_NC00009$	1	DESC	NONUNIQUE	"LAST_NAME"	FUNCTION-BASED NORMAL
		// DESC column make index functional index but as special case we included that
		// and update column name with column expression
		if colexpression.Valid && !strings.Contains(colexpression.String, "(") && !strings.Contains(colexpression.String, ")") {
			column = colexpression.String[1 : len(colexpression.String)-1]
		}

		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{Name: name, Unique: (Unique == "UNIQUE")}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{Column: column, Desc: (collation.Valid && collation.String == "DESC")})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		// only add noraml index
		if _, found := ignoredIndex[k]; !found {
			indexes = append(indexes, indexMap[k])
		}
	}
	return indexes, nil
}

// StartChangeDataCapture is used for automatic triggering of Datastream job when
// performing a streaming migration.
func (isi InfoSchemaImpl) StartChangeDataCapture(ctx context.Context, conv *internal.Conv) (map[string]interface{}, error) {
	if isi.SourceProfile.Ty == profiles.SourceProfileTypeConnection && isi.SourceProfile.Conn.Streaming {
		mp := make(map[string]interface{})
		streamingCfg, err := streaming.StartDatastream(ctx, isi.SourceProfile, isi.TargetProfile)
		if err != nil {
			err = fmt.Errorf("error starting datastream: %v", err)
			return nil, err
		}
		mp["streamingCfg"] = streamingCfg
		return mp, err
	}
	return nil, nil
}

// StartStreamingMigration is used for automatic triggering of Dataflow job when
// performing a streaming migration.
func (isi InfoSchemaImpl) StartStreamingMigration(ctx context.Context, client *sp.Client, conv *internal.Conv, streamingInfo map[string]interface{}) error {
	if isi.SourceProfile.Ty == profiles.SourceProfileTypeConnection && isi.SourceProfile.Conn.Streaming {
		streamingCfg, _ := streamingInfo["streamingCfg"].(streaming.StreamingCfg)
		err := streaming.StartDataflow(ctx, isi.SourceProfile, isi.TargetProfile, streamingCfg)
		if err != nil {
			err = fmt.Errorf("error starting dataflow: %v", err)
			return err
		}
	}
	return nil
}

func toType(dataType string, typecode, elementDataType sql.NullString, charLen sql.NullInt64, numericPrecision, numericScale, elementCharMaxLen, elementNumericPrecision, elementNumericScale sql.NullInt64) schema.Type {
	switch {
	case typecode.Valid && typecode.String == "COLLECTION":
		return modifyType(elementDataType.String, elementCharMaxLen, elementNumericPrecision, elementNumericScale, true)
	default:
		return modifyType(dataType, charLen, numericPrecision, numericScale, false)
	}
}

func modifyType(dataType string, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64, isArray bool) schema.Type {
	var t schema.Type
	switch {
	case dataType == "NUMBER" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		t = schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "NUMBER" && numericPrecision.Valid:
		t = schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	// Oracle get column query return data length for the Number type.
	case dataType != "NUMBER" && charLen.Valid:
		t = schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	default:
		t = schema.Type{Name: dataType}
	}
	if isArray {
		t.ArrayBounds = []int64{-1}
		return t
	}
	return t
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
