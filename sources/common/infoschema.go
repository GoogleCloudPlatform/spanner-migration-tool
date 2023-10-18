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
	"context"
	"fmt"
	"sync"

	sp "cloud.google.com/go/spanner"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

const DefaultWorkers = 20 // Default to 20 - observed diminishing returns above this value

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
	GetIndexes(conv *internal.Conv, table SchemaAndName, colNameIdMp map[string]string) ([]schema.Index, error)
	ProcessData(conv *internal.Conv, tableId string, srcSchema schema.Table, spCols []string, spSchema ddl.CreateTable, additionalAttributes internal.AdditionalDataAttributes) error
	StartChangeDataCapture(ctx context.Context, conv *internal.Conv) (map[string]interface{}, error)
	StartStreamingMigration(ctx context.Context, client *sp.Client, conv *internal.Conv, streamInfo map[string]interface{}) (internal.DataflowOutput, error)
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
func ProcessSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int, attributes internal.AdditionalSchemaAttributes) error {

	tableCount, err := GenerateSrcSchema(conv, infoSchema, numWorkers)
	if err != nil {
		return err
	}
	initPrimaryKeyOrder(conv)
	initIndexOrder(conv)
	SchemaToSpannerDDL(conv, infoSchema.GetToDdl())
	if tableCount != len(conv.SpSchema) {
		fmt.Printf("Failed to load all the source tables, source table count: %v, processed tables:%v. Please retry connecting to the source database to load tables.\n", tableCount, len(conv.SpSchema))
		return fmt.Errorf("failed to load all the source tables, source table count: %v, processed tables:%v. Please retry connecting to the source database to load tables.", tableCount, len(conv.SpSchema))
	}
	conv.AddPrimaryKeys()
	if attributes.IsSharded {
		conv.AddShardIdColumn()
	}
	fmt.Println("loaded schema")
	return nil
}

func GenerateSrcSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int) (int, error) {
	tables, err := infoSchema.GetTables()
	fmt.Println("fetched tables", tables)
	if err != nil {
		return 0, err
	}

	if numWorkers < 1 {
		numWorkers = DefaultWorkers
	}

	asyncProcessTable := func(t SchemaAndName, mutex *sync.Mutex) TaskResult[SchemaAndName] {
		table, e := processTable(conv, t, infoSchema)
		mutex.Lock()
		conv.SrcSchema[table.Id] = table
		mutex.Unlock()
		res := TaskResult[SchemaAndName]{t, e}
		return res
	}

	res, e := RunParallelTasks(tables, numWorkers, asyncProcessTable, true)
	if e != nil {
		fmt.Printf("exiting due to error: %s , while processing schema for table %s\n", e, res)
		return 0, e
	}

	internal.ResolveForeignKeyIds(conv.SrcSchema)
	return len(tables), nil
}

// ProcessData performs data conversion for source database
// 'db'. For each table, we extract and convert the data to Spanner data
// (based on the source and Spanner schemas), and write it to Spanner.
// If we can't get/process data for a table, we skip that table and process
// the remaining tables.
func ProcessData(conv *internal.Conv, infoSchema InfoSchema, additionalAttributes internal.AdditionalDataAttributes) {
	// Tables are ordered in alphabetical order with one exception: interleaved
	// tables appear after the population of their parent table.
	tableIds := ddl.GetSortedTableIdsBySpName(conv.SpSchema)

	for _, tableId := range tableIds {
		srcSchema := conv.SrcSchema[tableId]
		spSchema, ok := conv.SpSchema[tableId]
		if !ok {
			conv.Stats.BadRows[srcSchema.Name] += conv.Stats.Rows[srcSchema.Name]
			conv.Unexpected(fmt.Sprintf("Can't get cols and schemas for table %s:ok=%t",
				srcSchema.Name, ok))
			continue
		}
		// Extract common spColds. We get column ids common to both source and
		// spanner table so that we can read these records from source
		colIds := GetCommonColumnIds(conv, tableId, spSchema.ColIds)
		err := infoSchema.ProcessData(conv, tableId, srcSchema, colIds, spSchema, additionalAttributes)
		if err != nil {
			return
		}
		if conv.DataFlush != nil {
			conv.DataFlush()
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

func processTable(conv *internal.Conv, table SchemaAndName, infoSchema InfoSchema) (schema.Table, error) {
	var t schema.Table
	fmt.Println("processing schema for table", table)
	tblId := internal.GenerateTableId()
	primaryKeys, constraints, err := infoSchema.GetConstraints(conv, table)
	if err != nil {
		return t, fmt.Errorf("couldn't get constraints for table %s.%s: %s", table.Schema, table.Name, err)
	}
	foreignKeys, err := infoSchema.GetForeignKeys(conv, table)
	if err != nil {
		return t, fmt.Errorf("couldn't get foreign key constraints for table %s.%s: %s", table.Schema, table.Name, err)
	}

	colDefs, colIds, err := infoSchema.GetColumns(conv, table, constraints, primaryKeys)
	if err != nil {
		return t, fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	colNameIdMap := make(map[string]string)
	for k, v := range colDefs {
		colNameIdMap[v.Name] = k
	}

	indexes, err := infoSchema.GetIndexes(conv, table, colNameIdMap)
	if err != nil {
		return t, fmt.Errorf("couldn't get indexes for table %s.%s: %s", table.Schema, table.Name, err)
	}

	name := infoSchema.GetTableName(table.Schema, table.Name)
	var schemaPKeys []schema.Key
	for _, k := range primaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{ColId: colNameIdMap[k]})
	}
	t = schema.Table{
		Id:           tblId,
		Name:         name,
		Schema:       table.Schema,
		ColIds:       colIds,
		ColNameIdMap: colNameIdMap,
		ColDefs:      colDefs,
		PrimaryKeys:  schemaPKeys,
		Indexes:      indexes,
		ForeignKeys:  foreignKeys}
	return t, nil
}

// getIncludedSrcTablesFromConv fetches the list of tables
// from the source database that need to be migrated.
func GetIncludedSrcTablesFromConv(conv *internal.Conv) (tableList []string, err error) {
	for spTable := range conv.SpSchema {
		//lookup the spanner table in the source tables via ID
		srcTable, ok := conv.SrcSchema[spTable]
		if !ok {
			err := fmt.Errorf("id for spanner and source tables do not match, this is most likely a bug")
			return nil, err
		}
		tableList = append(tableList, srcTable.Name)
	}
	return tableList, nil
}
