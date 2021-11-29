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
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// InfoSchema contains database information.
type InfoSchema interface {
	GetToDdl() ToDdl
	GetTableName(schema string, tableName string) string
	GetTables() ([]SchemaAndName, error)
	GetColumns(conv *internal.Conv, table SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error)
	GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error)
	GetRowCount(table SchemaAndName) (int64, error)
	GetConstraints(conv *internal.Conv, table SchemaAndName) ([]string, map[string][]string, error)
	GetForeignKeys(conv *internal.Conv, table SchemaAndName) (foreignKeys []schema.ForeignKey, err error)
	GetIndexes(conv *internal.Conv, table SchemaAndName) ([]schema.Index, error)
	ProcessData(conv *internal.Conv, srcTable string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable) error
}

// SchemaAndName contains the schema and name for a table
type SchemaAndName struct {
	Schema string
	Name   string
}

// FkConstraint contains foreign key constraints
type FkConstraint struct {
	Name    string
	Table   string
	Refcols []string
	Cols    []string
}

// ProcessSchema performs schema conversion for source database
// 'db'. Information schema tables are a broadly supported ANSI standard,
// and we use them to obtain source database's schema information.
func ProcessSchema(conv *internal.Conv, infoSchema InfoSchema) error {
	tables, err := infoSchema.GetTables()
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := processTable(conv, t, infoSchema); err != nil {
			return err
		}
	}
	SchemaToSpannerDDL(conv, infoSchema.GetToDdl())
	conv.AddPrimaryKeys()
	return nil
}

// ProcessData performs data conversion for source database
// 'db'. For each table, we extract and convert the data to Spanner data
// (based on the source and Spanner schemas), and write it to Spanner.
// If we can't get/process data for a table, we skip that table and process
// the remaining tables.
func ProcessData(conv *internal.Conv, infoSchema InfoSchema, writer *spanner.BatchWriter) {
	// Tables are ordered in alphabetical order with one exception: interleaved
	// tables appear after the population of their parent table.
	orderTableNames := ddl.OrderTables(conv.SpSchema)

	for _, spannerTable := range orderTableNames {
		srcTable, _ := internal.GetSourceTable(conv, spannerTable)
		srcSchema := conv.SrcSchema[srcTable]
		spTable, err1 := internal.GetSpannerTable(conv, srcTable)
		spCols, err2 := internal.GetSpannerCols(conv, srcTable, srcSchema.ColNames)
		spSchema, ok := conv.SpSchema[spTable]
		if err1 != nil || err2 != nil || !ok {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get cols and schemas for table %s: err1=%s, err2=%s, ok=%t",
				srcTable, err1, err2, ok))
			continue
		}
		err := infoSchema.ProcessData(conv, srcTable, srcSchema, spTable, spCols, spSchema)
		if err != nil {
			writer.Flush()
			return
		}
	}
}

// SetRowStats populates conv with the number of rows in each table.
func SetRowStats(conv *internal.Conv, infoSchema InfoSchema) {
	tables, err := infoSchema.GetTables()
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		tableName := infoSchema.GetTableName(t.Schema, t.Name)
		count, err := infoSchema.GetRowCount(t)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get number of rows for table %s", tableName))
			continue
		}
		conv.Stats.Rows[tableName] += count
	}
}

func processTable(conv *internal.Conv, table SchemaAndName, infoSchema InfoSchema) error {
	primaryKeys, constraints, err := infoSchema.GetConstraints(conv, table)
	if err != nil {
		return fmt.Errorf("couldn't get constraints for table %s.%s: %s", table.Schema, table.Name, err)
	}
	foreignKeys, err := infoSchema.GetForeignKeys(conv, table)
	if err != nil {
		return fmt.Errorf("couldn't get foreign key constraints for table %s.%s: %s", table.Schema, table.Name, err)
	}
	indexes, err := infoSchema.GetIndexes(conv, table)
	if err != nil {
		return fmt.Errorf("couldn't get indexes for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colDefs, colNames, err := infoSchema.GetColumns(conv, table, constraints, primaryKeys)
	if err != nil {
		return fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	name := infoSchema.GetTableName(table.Schema, table.Name)
	var schemaPKeys []schema.Key
	for _, k := range primaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{Column: k})
	}
	conv.SrcSchema[name] = schema.Table{
		Name:        name,
		Schema:      table.Schema,
		ColNames:    colNames,
		ColDefs:     colDefs,
		PrimaryKeys: schemaPKeys,
		Indexes:     indexes,
		ForeignKeys: foreignKeys}
	return nil
}
