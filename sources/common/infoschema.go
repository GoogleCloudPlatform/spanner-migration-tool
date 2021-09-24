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

package common

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	_ "github.com/go-sql-driver/mysql" // The driver should be used via the database/sql package.
	_ "github.com/lib/pq"
)

type BaseInfoSchema interface {
	GetBaseDdl() BaseToDdl
	GetDbName() string
	GetIgnoredSchemas() map[string]bool
	GetTableName(dbName string, tableName string, withQuotes bool) string
	GetTablesQuery() string
	GetColumnsQuery() string
	ProcessColumns(conv *internal.Conv, cols *sql.Rows, constraints map[string][]string) (map[string]schema.Column, []string)
	BuildColNameList(srcSchema schema.Table, srcColName []string) string
	GetConstraintsQuery() string
	GetForeignKeysQuery() string
	GetIndexesQuery() string
	ProcessDataRows(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, rows *sql.Rows)
}

// ProcessInfoSchema performs schema conversion for source database
// 'db'. Information schema tables are a broadly supported ANSI standard,
// and we use them to obtain source database's schema information.
func ProcessInfoSchema(conv *internal.Conv, db *sql.DB, baseInfoSchema BaseInfoSchema) error {
	tables, err := getTables(db, baseInfoSchema)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := processTable(conv, db, t, baseInfoSchema); err != nil {
			return err
		}
	}
	SchemaToSpannerDDL(conv, baseInfoSchema.GetBaseDdl())
	conv.AddPrimaryKeys()
	return nil
}

// ProcessSQLData performs data conversion for source database
// 'db'. For each table, we extract data using a "SELECT (colNamesList)" query,
// convert the data to Spanner data (based on the source and Spanner
// schemas), and write it to Spanner.  If we can't get/process data
// for a table, we skip that table and process the remaining tables.
//
// Using database/sql library we pass *sql.RawBytes to rows.scan.
// RawBytes is a byte slice and values can be easily converted to string.
func ProcessSQLData(conv *internal.Conv, db *sql.DB, baseInfoSchema BaseInfoSchema) {
	// TODO: refactor to use the set of tables computed by
	// ProcessInfoSchema instead of computing them again.
	tables, err := getTables(db, baseInfoSchema)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		srcTable := baseInfoSchema.GetTableName(baseInfoSchema.GetDbName(), t.name, false)
		srcSchema, ok := conv.SrcSchema[srcTable]
		if !ok {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get schemas for table %s", srcTable))
			continue
		}
		srcCols := srcSchema.ColNames
		if len(srcCols) == 0 {
			conv.Unexpected(fmt.Sprintf("Couldn't get source columns for table %s ", t.name))
			continue
		}
		colNameList := baseInfoSchema.BuildColNameList(srcSchema, srcCols)
		// MySQL schema and name can be arbitrary strings.
		// Ideally we would pass schema/name as a query parameter,
		// but MySQL doesn't support this. So we quote it instead.
		q := fmt.Sprintf("SELECT %s FROM %s;", colNameList, baseInfoSchema.GetTableName(baseInfoSchema.GetDbName(), t.name, true))
		rows, err := db.Query(q)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get data for table %s : err = %s", t.name, err))
			continue
		}
		defer rows.Close()
		srcCols, _ = rows.Columns()
		spTable, err := internal.GetSpannerTable(conv, srcTable)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get spanner table : %s", err))
			continue
		}
		spCols, err := internal.GetSpannerCols(conv, srcTable, srcCols)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get spanner columns for table %s : err = %s", t.name, err))
			continue
		}
		spSchema, ok := conv.SpSchema[spTable]
		if !ok {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get schemas for table %s", srcTable))
			continue
		}
		baseInfoSchema.ProcessDataRows(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, rows)
	}
}

// SetRowStats populates conv with the number of rows in each table.
func SetRowStats(conv *internal.Conv, db *sql.DB, baseInfoSchema BaseInfoSchema) {
	tables, err := getTables(db, baseInfoSchema)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		q := fmt.Sprintf("SELECT COUNT(*) FROM %s;", baseInfoSchema.GetTableName(t.schema, t.name, true))
		tableName := t.name
		rows, err := db.Query(q)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get number of rows for table %s", tableName))
			continue
		}
		defer rows.Close()
		var count int64
		if rows.Next() {
			err := rows.Scan(&count)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Can't get row count: %s", err))
				continue
			}
			conv.Stats.Rows[tableName] += count
		}
	}
}

type schemaAndName struct {
	schema string
	name   string
}

// getTables return list of tables in the selected database.
// Note that sql.DB already effectively has the dbName
// embedded within it (dbName is part of the DSN passed to sql.Open),
// but unfortunately there is no way to extract it from sql.DB.
func getTables(db *sql.DB, baseInfoSchema BaseInfoSchema) ([]schemaAndName, error) {
	rows, err := db.Query(baseInfoSchema.GetTablesQuery())
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}
	defer rows.Close()
	var tableSchema, tableName string
	var tables []schemaAndName
	for rows.Next() {
		rows.Scan(&tableSchema, &tableName)
		if !baseInfoSchema.GetIgnoredSchemas()[tableSchema] {
			tables = append(tables, schemaAndName{schema: tableSchema, name: tableName})
		}
	}
	return tables, nil
}

func processTable(conv *internal.Conv, db *sql.DB, table schemaAndName, baseInfoSchema BaseInfoSchema) error {
	cols, err := db.Query(baseInfoSchema.GetColumnsQuery(), table.schema, table.name)
	if err != nil {
		return fmt.Errorf("couldn't get schema for table %s.%s: %s", table.schema, table.name, err)
	}
	defer cols.Close()
	primaryKeys, constraints, err := getConstraints(conv, db, table, baseInfoSchema)
	if err != nil {
		return fmt.Errorf("couldn't get constraints for table %s.%s: %s", table.schema, table.name, err)
	}
	foreignKeys, err := getForeignKeys(conv, db, table, baseInfoSchema)
	if err != nil {
		return fmt.Errorf("couldn't get foreign key constraints for table %s.%s: %s", table.schema, table.name, err)
	}
	indexes, err := getIndexes(conv, db, table, baseInfoSchema)
	if err != nil {
		return fmt.Errorf("couldn't get indexes for table %s.%s: %s", table.schema, table.name, err)
	}
	colDefs, colNames := baseInfoSchema.ProcessColumns(conv, cols, constraints)
	name := baseInfoSchema.GetTableName(table.schema, table.name, false)
	var schemaPKeys []schema.Key
	for _, k := range primaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{Column: k})
	}
	conv.SrcSchema[name] = schema.Table{
		Name:        name,
		ColNames:    colNames,
		ColDefs:     colDefs,
		PrimaryKeys: schemaPKeys,
		Indexes:     indexes,
		ForeignKeys: foreignKeys}
	return nil
}

// getConstraints returns a list of primary keys and by-column map of
// other constraints.  Note: we need to preserve ordinal order of
// columns in primary key constraints.
// Note that foreign key constraints are handled in getForeignKeys.
func getConstraints(conv *internal.Conv, db *sql.DB, table schemaAndName, baseInfoSchema BaseInfoSchema) ([]string, map[string][]string, error) {
	rows, err := db.Query(baseInfoSchema.GetConstraintsQuery(), table.schema, table.name)
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

type fkConstraint struct {
	name    string
	table   string
	refcols []string
	cols    []string
}

// getForeignKeys return list all the foreign keys constraints.
// MySQL supports cross-database foreign key constraints. We ignore
// them because HarbourBridge works database at a time (a specific run
// of HarbourBridge focuses on a specific database) and so we can't handle
// them effectively.
func getForeignKeys(conv *internal.Conv, db *sql.DB, table schemaAndName, baseInfoSchema BaseInfoSchema) (foreignKeys []schema.ForeignKey, err error) {
	rows, err := db.Query(baseInfoSchema.GetForeignKeysQuery(), table.schema, table.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var col, refSchema, refCol, refTable, fKeyName string
	fKeys := make(map[string]fkConstraint)
	var keyNames []string

	for rows.Next() {
		err := rows.Scan(&refSchema, &refTable, &col, &refCol, &fKeyName)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		tableName := baseInfoSchema.GetTableName(refSchema, refTable, false)
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.cols = append(fk.cols, col)
			fk.refcols = append(fk.refcols, refCol)
			fKeys[fKeyName] = fk
			continue
		}
		fKeys[fKeyName] = fkConstraint{name: fKeyName, table: tableName, refcols: []string{refCol}, cols: []string{col}}
		keyNames = append(keyNames, fKeyName)
	}
	sort.Strings(keyNames)
	for _, k := range keyNames {
		foreignKeys = append(foreignKeys,
			schema.ForeignKey{
				Name:         fKeys[k].name,
				Columns:      fKeys[k].cols,
				ReferTable:   fKeys[k].table,
				ReferColumns: fKeys[k].refcols})
	}
	return foreignKeys, nil
}

// getIndexes return a list of all indexes for the specified table.
func getIndexes(conv *internal.Conv, db *sql.DB, table schemaAndName, baseInfoSchema BaseInfoSchema) ([]schema.Index, error) {
	rows, err := db.Query(baseInfoSchema.GetIndexesQuery(), table.schema, table.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, column, sequence, order, isUnique string
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	var indexes []schema.Index
	for rows.Next() {
		if err := rows.Scan(&name, &column, &sequence, &order, &isUnique); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{Name: name, Unique: (isUnique == "1")}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{Column: column, Desc: (order == "D")})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		indexes = append(indexes, indexMap[k])
	}
	return indexes, nil
}
