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

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type InfoSchema interface {
	GetToDdl() ToDdl
	GetTableName(schema string, tableName string) string
	GetTables(db *sql.DB) ([]SchemaAndName, error)
	GetColumns(table SchemaAndName, db *sql.DB) (*sql.Rows, error) //TODO - merge this method and ProcessColumns for cleaner interface
	ProcessColumns(conv *internal.Conv, cols *sql.Rows, constraints map[string][]string) (map[string]schema.Column, []string)
	GetRowsFromTable(conv *internal.Conv, db *sql.DB, table SchemaAndName) (*sql.Rows, error)
	GetRowCount(db *sql.DB, table SchemaAndName) (int64, error)
	GetConstraints(conv *internal.Conv, db *sql.DB, table SchemaAndName) ([]string, map[string][]string, error)
	GetForeignKeys(conv *internal.Conv, db *sql.DB, table SchemaAndName) (foreignKeys []schema.ForeignKey, err error)
	GetIndexes(conv *internal.Conv, db *sql.DB, table SchemaAndName) ([]schema.Index, error)
	ProcessDataRows(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, rows *sql.Rows)
}

type SchemaAndName struct {
	Schema string
	Name   string
}

type FkConstraint struct {
	Name    string
	Table   string
	Refcols []string
	Cols    []string
}

// ProcessInfoSchema performs schema conversion for source database
// 'db'. Information schema tables are a broadly supported ANSI standard,
// and we use them to obtain source database's schema information.
func ProcessInfoSchema(conv *internal.Conv, db *sql.DB, infoSchema InfoSchema) error {
	tables, err := infoSchema.GetTables(db)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := processTable(conv, db, t, infoSchema); err != nil {
			return err
		}
	}
	SchemaToSpannerDDL(conv, infoSchema.GetToDdl())
	conv.AddPrimaryKeys()
	return nil
}

// ProcessSQLData performs data conversion for source database
// 'db'. For each table, we extract and convert the data to Spanner data
// (based on the source and Spanner schemas), and write it to Spanner.
// If we can't get/process data for a table, we skip that table and process
// the remaining tables.
//
// Using database/sql library we pass *sql.RawBytes to rows.scan.
// RawBytes is a byte slice and values can be easily converted to string.
func ProcessSQLData(conv *internal.Conv, db *sql.DB, infoSchema InfoSchema) {
	// TODO: refactor to use the set of tables computed by
	// ProcessInfoSchema instead of computing them again.
	tables, err := infoSchema.GetTables(db)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		srcTable := infoSchema.GetTableName(t.Schema, t.Name)
		srcSchema, ok := conv.SrcSchema[srcTable]
		if !ok {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get schemas for table %s", srcTable))
			continue
		}
		rows, err := infoSchema.GetRowsFromTable(conv, db, t)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get data for table %s : err = %s", t.Name, err))
			continue
		}
		defer rows.Close()
		srcCols, _ := rows.Columns()
		spTable, err := internal.GetSpannerTable(conv, srcTable)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get spanner table : %s", err))
			continue
		}
		spCols, err := internal.GetSpannerCols(conv, srcTable, srcCols)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get spanner columns for table %s : err = %s", t.Name, err))
			continue
		}
		spSchema, ok := conv.SpSchema[spTable]
		if !ok {
			//TODO - check why Bad rows are not being added in above conditions
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get schemas for table %s", srcTable))
			continue
		}
		infoSchema.ProcessDataRows(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, rows)
	}
}

// SetRowStats populates conv with the number of rows in each table.
func SetRowStats(conv *internal.Conv, db *sql.DB, infoSchema InfoSchema) {
	tables, err := infoSchema.GetTables(db)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		tableName := infoSchema.GetTableName(t.Schema, t.Name)
		count, err := infoSchema.GetRowCount(db, t)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get number of rows for table %s", tableName))
			continue
		}
		conv.Stats.Rows[tableName] += count
	}
}

func processTable(conv *internal.Conv, db *sql.DB, table SchemaAndName, infoSchema InfoSchema) error {
	cols, err := infoSchema.GetColumns(table, db)
	if err != nil {
		return fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	defer cols.Close()

	primaryKeys, constraints, err := infoSchema.GetConstraints(conv, db, table)
	if err != nil {
		return fmt.Errorf("couldn't get constraints for table %s.%s: %s", table.Schema, table.Name, err)
	}
	foreignKeys, err := infoSchema.GetForeignKeys(conv, db, table)
	if err != nil {
		return fmt.Errorf("couldn't get foreign key constraints for table %s.%s: %s", table.Schema, table.Name, err)
	}
	indexes, err := infoSchema.GetIndexes(conv, db, table)
	if err != nil {
		return fmt.Errorf("couldn't get indexes for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colDefs, colNames := infoSchema.ProcessColumns(conv, cols, constraints)
	if err != nil {
		return fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	name := infoSchema.GetTableName(table.Schema, table.Name)
	var schemaPKeys []schema.Key
	if len(primaryKeys) == 0 {
		for col, constraintList := range constraints {
			for _, constraint := range constraintList {
				if constraint == "UNIQUE" {
					schemaPKeys = append(schemaPKeys, schema.Key{Column: col})
				}
			}
		}
	} else {
		for _, k := range primaryKeys {
			schemaPKeys = append(schemaPKeys, schema.Key{Column: k})
		}
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
