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

package internal

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/cloudspannerecosystem/harbourbridge/schema"
)

// ProcessInfoSchema performs schema conversion for source database
// 'db'. We assume that the source database supports information
// schema tables. These tables are a broadly supported ANSI standard,
// and we use them to obtain source database's schema information.
func ProcessInfoSchema(conv *Conv, db *sql.DB) error {
	// TODO: All of the queries to get tables and table data
	// should be in a single transaction to ensure we obtain a
	// consistent snapshot of schema information across tables
	// (pg_dump does something similar). When we add SELECT
	// queries to get data, we should wrap those in the same
	// transaction to ensure consistency of schema and data.
	tables, err := getTables(db)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := processTable(conv, db, t); err != nil {
			return err
		}
	}
	schemaToDDL(conv)
	conv.AddPrimaryKeys()
	return nil
}

type schemaAndName struct {
	schema string // PostgreSQL schema (aka namespace for PostgreSQL objects).
	name   string
}

func getTables(db *sql.DB) ([]schemaAndName, error) {
	ignored := make(map[string]bool)
	// Ignore all system tables: we just want to convert user tables.
	for _, s := range []string{"information_schema", "postgres", "pg_catalog", "pg_temp_1", "pg_toast", "pg_toast_temp_1"} {
		ignored[s] = true
	}
	q := "SELECT table_schema, table_name FROM information_schema.tables where table_type = 'BASE TABLE'"
	rows, err := db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("Couldn't get tables: %w\n", err)
	}
	defer rows.Close()
	var tableSchema, tableName string
	var tables []schemaAndName
	for rows.Next() {
		rows.Scan(&tableSchema, &tableName)
		if !ignored[tableSchema] {
			tables = append(tables, schemaAndName{schema: tableSchema, name: tableName})
		}
	}
	return tables, nil
}

func processTable(conv *Conv, db *sql.DB, table schemaAndName) error {
	cols, err := getColumns(table, db)
	if err != nil {
		return fmt.Errorf("Couldn't get schema for table %s.%s: %s\n", table.schema, table.name, err)
	}
	defer cols.Close()
	primaryKeys, constraints, err := getConstraints(conv, db, table)
	if err != nil {
		return fmt.Errorf("Couldn't get constraints for table %s.%s: %s\n", table.schema, table.name, err)
	}
	colDefs, colNames := processColumns(conv, cols, constraints)
	name := fmt.Sprintf("%s.%s", table.schema, table.name)
	if table.schema == "public" { // Drop 'public' prefix.
		name = table.name
	}
	var schemaPKeys []schema.Key
	for _, k := range primaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{Column: k})
	}
	conv.srcSchema[name] = schema.Table{
		Name:        name,
		ColNames:    colNames,
		ColDefs:     colDefs,
		PrimaryKeys: schemaPKeys}
	return nil
}

func getColumns(table schemaAndName, db *sql.DB) (*sql.Rows, error) {
	q := `SELECT c.column_name, c.data_type, e.data_type, c.is_nullable, c.column_default, c.character_maximum_length, c.numeric_precision, c.numeric_scale
              FROM information_schema.COLUMNS c LEFT JOIN information_schema.element_types e
                 ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
                     = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
              where table_schema = $1 and table_name = $2 ORDER BY c.ordinal_position;`
	return db.Query(q, table.schema, table.name)
}

func processColumns(conv *Conv, cols *sql.Rows, constraints map[string][]string) (map[string]schema.Column, []string) {
	colDefs := make(map[string]schema.Column)
	var colNames []string
	var colName, dataType, isNullable string
	var colDefault, elementDataType sql.NullString
	var charMaxLen, numericPrecision, numericScale sql.NullInt64
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &elementDataType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale)
		if err != nil {
			fmt.Printf("Can't scan: %v\n", err)
			continue
		}
		unique := false
		ignored := schema.Ignored{}
		for _, c := range constraints[colName] {
			// c can be UNIQUE, PRIMARY KEY, FOREIGN KEY,
			// or CHECK (based on msql, sql server, postgres docs).
			// We've already filtered out PRIMARY KEY.
			switch c {
			case "UNIQUE":
				unique = true
			case "FOREIGN KEY":
				ignored.ForeignKey = true
			case "CHECK":
				ignored.Check = true
			}
		}
		ignored.Default = colDefault.Valid
		c := schema.Column{
			Name:    colName,
			Type:    toType(dataType, elementDataType, charMaxLen, numericPrecision, numericScale),
			NotNull: toNotNull(conv, isNullable),
			Unique:  unique,
			Ignored: ignored,
		}
		colDefs[colName] = c
		colNames = append(colNames, colName)
	}
	return colDefs, colNames
}

// getConstraints returns a list of primary keys and by-column map of
// other constraints.  Note: we need to preserve ordinal order of
// columns in primary key constraints.
func getConstraints(conv *Conv, db *sql.DB, table schemaAndName) ([]string, map[string][]string, error) {
	q := `SELECT k.COLUMN_NAME, t.CONSTRAINT_TYPE
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
                  ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
              WHERE k.TABLE_SCHEMA = $1 AND k.TABLE_NAME = $2 ORDER BY k.ordinal_position;`
	rows, err := db.Query(q, table.schema, table.name)
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
			fmt.Printf("Can't scan: %v\n", err)
			continue
		}
		if col == "" || constraint == "" {
			fmt.Printf("Got empty col or constraint\n")
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

func toType(dataType string, elementDataType sql.NullString, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	case dataType == "ARRAY" && elementDataType.Valid:
		return schema.Type{Name: elementDataType.String, ArrayBounds: []int64{-1}}
		// TODO: handle error cases.
		// TODO: handle case of multiple array bounds.
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	case dataType == "numeric" && numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case dataType == "numeric" && numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	default:
		return schema.Type{Name: dataType}
	}
}

func toNotNull(conv *Conv, isNullable string) bool {
	switch isNullable {
	case "YES":
		return false
	case "NO":
		return true
	}
	conv.unexpected(fmt.Sprintf("isNullable column has unknown value: %s", isNullable))
	return false
}
