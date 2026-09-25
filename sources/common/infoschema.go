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
	"strings"
	"sync"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/task"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

const DefaultWorkers = 20 // Default to 20 - observed diminishing returns above this value

// InfoSchema contains database information.
type InfoSchema interface {
	GetToDdl() ToDdl
	GetTableName(schema string, tableName string) string
	GetTables() ([]SchemaAndName, error)
	GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error)
	GetRowCount(table SchemaAndName) (int64, error)
	ProcessData(conv *internal.Conv, tableId string, srcSchema schema.Table, spCols []string, spSchema ddl.CreateTable, additionalAttributes internal.AdditionalDataAttributes) error

}

// StandardInfoSchema supports per-table fetching of metadata.
type StandardInfoSchema interface {
	InfoSchema
	// GetColumns fetches column definitions for a single table.
	// Returns a map of column ID to schema.Column, a list of column IDs in ordinal order, and any error.
	GetColumns(conv *internal.Conv, table SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error)
	// GetConstraints fetches constraints (primary keys, check constraints, etc.) for a single table.
	// Returns a list of primary key column names, a list of check constraints, a map of column name to list of constraint types, and any error.
	GetConstraints(conv *internal.Conv, table SchemaAndName) ([]string, []schema.CheckConstraint, map[string][]string, error)
	// GetForeignKeys fetches foreign key constraints for a single table.
	// Returns a list of schema.ForeignKey and any error.
	GetForeignKeys(conv *internal.Conv, table SchemaAndName) (foreignKeys []schema.ForeignKey, err error)
	// GetIndexes fetches indexes for a single table.
	// Returns a list of schema.Index and any error.
	GetIndexes(conv *internal.Conv, table SchemaAndName, colNameIdMp map[string]string) ([]schema.Index, error)
}

// TableColumns contains column definitions and IDs for a table.
type TableColumns struct {
	ColDefs map[string]schema.Column
	ColIds  []string
}

// TableConstraints contains constraints for a table.
type TableConstraints struct {
	PrimaryKeys       []string
	CheckConstraints  []schema.CheckConstraint
	ColumnConstraints map[string][]string // colName -> constraintTypes
}

// TableForeignKeys contains foreign keys for a table.
type TableForeignKeys struct {
	ForeignKeys []schema.ForeignKey
}

// BatchedInfoSchema supports bulk fetching of metadata for multiple tables.
type BatchedInfoSchema interface {
	InfoSchema
	// GetColumnsBatch fetches column definitions for a batch of tables.
	GetColumnsBatch(conv *internal.Conv, tables []SchemaAndName) (map[string]TableColumns, error)
	// GetConstraintsBatch fetches constraints for a batch of tables.
	GetConstraintsBatch(conv *internal.Conv, tables []SchemaAndName) (map[string]TableConstraints, error)
	// GetForeignKeysBatch fetches foreign key constraints for a batch of tables.
	GetForeignKeysBatch(conv *internal.Conv, tables []SchemaAndName) (map[string]TableForeignKeys, error)
	// GetIndexesBatch fetches indexes for a batch of tables.
	GetIndexesBatch(conv *internal.Conv, tables []SchemaAndName, colDefs map[string]TableColumns) (map[string][]schema.Index, error)
}

// PartitionInfoSchema is an optional interface implemented by sources that can
// distinguish a child partition from an ordinary table. It is discovered via a
// type assertion, so sources without partitioning support need no changes.
type PartitionInfoSchema interface {
	PartitionParent(schema string, table string) (parentSchema string, parentTable string, ok bool)
}

// TableInheritanceProvider is an optional interface for sources that support table inheritance.
type TableInheritanceProvider interface {
	GetInheritedTables() (map[string][]string, error)
}

// SchemaAndName contains the schema and name for a table
type SchemaAndName struct {
	Schema string
	Name   string
	Id     string
}

// FkConstraint contains foreign key constraints
type FkConstraint struct {
	Name     string
	Table    string
	Refcols  []string
	Cols     []string
	OnDelete string
	OnUpdate string
}

type InfoSchemaInterface interface {
	GenerateSrcSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int) (int, error)
	ProcessData(conv *internal.Conv, infoSchema InfoSchema, additionalAttributes internal.AdditionalDataAttributes)
	SetRowStats(conv *internal.Conv, infoSchema InfoSchema)
	ProcessTable(conv *internal.Conv, table SchemaAndName, infoSchema StandardInfoSchema) (schema.Table, error)
	GetIncludedSrcTablesFromConv(conv *internal.Conv) (schemaToTablesMap map[string]internal.SchemaDetails, err error)
}
type InfoSchemaImpl struct{}

type ProcessSchemaInterface interface {
	ProcessSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int, attributes internal.AdditionalSchemaAttributes, s SchemaToSpannerInterface, uo UtilsOrderInterface, is InfoSchemaInterface) error
}

type ProcessSchemaImpl struct{}

// ProcessSchema performs schema conversion for source database
// 'db'. Information schema tables are a broadly supported ANSI standard,
// and we use them to obtain source database's schema information.
func (ps *ProcessSchemaImpl) ProcessSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int, attributes internal.AdditionalSchemaAttributes, s SchemaToSpannerInterface, uo UtilsOrderInterface, is InfoSchemaInterface) error {

	tableCount, err := is.GenerateSrcSchema(conv, infoSchema, numWorkers)
	if err != nil {
		return err
	}
	uo.initPrimaryKeyOrder(conv)
	uo.initIndexOrder(conv)
	err = s.SchemaToSpannerDDL(conv, infoSchema.GetToDdl(), attributes)
	if err != nil {
		return err
	}
	skipped := countSkippedPartitions(conv)
	if tableCount-skipped != len(conv.SpSchema) {
		logger.Log.Info(fmt.Sprintf("Failed to load all the source tables, source table count: %v, processed tables:%v, intentionally skipped partitions:%v. Please retry connecting to the source database to load tables.\n", tableCount, len(conv.SpSchema), skipped))
		return fmt.Errorf("failed to load all the source tables, source table count: %v, processed tables:%v, intentionally skipped partitions:%v. Please retry connecting to the source database to load tables", tableCount, len(conv.SpSchema), skipped)
	}
	if skipped > 0 {
		logger.Log.Info(fmt.Sprintf("Ignored %v partitioned table(s); their data is covered by the corresponding parent tables. Use the Restore option in the UI to migrate them individually.", skipped))
	}
	logger.Log.Info(fmt.Sprint("loaded schema"))
	return nil
}

// isSkippedPartition reports whether tableId is a child partition that was left
// out of the Spanner schema.
func isSkippedPartition(conv *internal.Conv, tableId string) bool {
	srcTable, ok := conv.SrcSchema[tableId]
	if !ok || srcTable.PartitionParent == "" {
		return false
	}
	_, converted := conv.SpSchema[tableId]
	return !converted
}

// countSkippedPartitions returns the number of child partitions left out of the
// Spanner schema.
func countSkippedPartitions(conv *internal.Conv) int {
	skipped := 0
	for tableId := range conv.SrcSchema {
		if isSkippedPartition(conv, tableId) {
			skipped++
		}
	}
	return skipped
}

// partitionParentName returns the name SrcSchema uses for the partitioned table
// that tableSchema.tableName belongs to, empty if it is not a partition or the
// source does not report partitions.
func partitionParentName(is InfoSchema, tableSchema, tableName string) string {
	pi, ok := is.(PartitionInfoSchema)
	if !ok {
		return ""
	}
	parentSchema, parentTable, isPartition := pi.PartitionParent(tableSchema, tableName)
	if !isPartition {
		return ""
	}
	return is.GetTableName(parentSchema, parentTable)
}

func (is *InfoSchemaImpl) GenerateSrcSchema(conv *internal.Conv, infoSchema InfoSchema, numWorkers int) (int, error) {
	tables, err := infoSchema.GetTables()
	logger.Log.Info(fmt.Sprint("fetched tables", tables))
	if err != nil {
		return 0, err
	}

	if numWorkers < 1 {
		numWorkers = DefaultWorkers
	}

	var tableCount int
	var e error

	if bis, ok := infoSchema.(BatchedInfoSchema); ok {
		tableCount, e = is.generateSrcSchemaBatched(conv, bis, tables, numWorkers)
	} else if sis, ok := infoSchema.(StandardInfoSchema); ok {
		tableCount, e = is.generateSrcSchemaStandard(conv, sis, tables, numWorkers)
	} else {
		return 0, fmt.Errorf("source does not support schema fetching")
	}

	if e != nil {
		return 0, e
	}

	annotateInheritedTables(conv, infoSchema)
	internal.ResolveForeignKeyIds(conv.SrcSchema)
	return tableCount, nil
}

func annotateInheritedTables(conv *internal.Conv, infoSchema InfoSchema) {
	provider, ok := infoSchema.(TableInheritanceProvider)
	if !ok {
		return
	}
	inherited, err := provider.GetInheritedTables()
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Couldn't get table inheritance information: %s", err))
		return
	}
	for tableId, table := range conv.SrcSchema {
		if parents, ok := inherited[table.Name]; ok && len(parents) > 0 {
			table.InheritedFrom = parents
			conv.SrcSchema[tableId] = table
		}
	}
}

func (is *InfoSchemaImpl) generateSrcSchemaBatched(conv *internal.Conv, bis BatchedInfoSchema, tables []SchemaAndName, numWorkers int) (int, error) {
	batchSize := 50
	var batches [][]SchemaAndName
	for i := 0; i < len(tables); i += batchSize {
		end := i + batchSize
		if end > len(tables) {
			end = len(tables)
		}
		batches = append(batches, tables[i:end])
	}

	asyncProcessBatch := func(batch []SchemaAndName, mutex *sync.Mutex) task.TaskResult[[]SchemaAndName] {
		tableCols, err := bis.GetColumnsBatch(conv, batch)
		if err != nil {
			return task.TaskResult[[]SchemaAndName]{Result: batch, Err: err}
		}
		tableConstraints, err := bis.GetConstraintsBatch(conv, batch)
		if err != nil {
			return task.TaskResult[[]SchemaAndName]{Result: batch, Err: err}
		}
		tableForeignKeys, err := bis.GetForeignKeysBatch(conv, batch)
		if err != nil {
			return task.TaskResult[[]SchemaAndName]{Result: batch, Err: err}
		}
		indexes, err := bis.GetIndexesBatch(conv, batch, tableCols)
		if err != nil {
			return task.TaskResult[[]SchemaAndName]{Result: batch, Err: err}
		}
		partitionParent := make(map[string]string, len(batch))
		for _, t := range batch {
			partitionParent[t.Name] = partitionParentName(bis, t.Schema, t.Name)
		}

		mutex.Lock()
		for _, t := range batch {
			name := bis.GetTableName(t.Schema, t.Name)
			cols := tableCols[t.Name]
			constraints := tableConstraints[t.Name]
			fks := tableForeignKeys[t.Name]

			tableObj := BuildSchemaTable(t, name, cols.ColDefs, cols.ColIds, constraints.PrimaryKeys, constraints.CheckConstraints, indexes[t.Name], fks.ForeignKeys, partitionParent[t.Name])
			conv.SrcSchema[tableObj.Id] = tableObj
		}
		mutex.Unlock()

		return task.TaskResult[[]SchemaAndName]{Result: batch, Err: nil}
	}

	r := task.RunParallelTasksImpl[[]SchemaAndName, []SchemaAndName]{}
	_, e := r.RunParallelTasks(batches, numWorkers, asyncProcessBatch, true)

	if e != nil {
		return 0, e
	}
	return len(tables), nil
}

func (is *InfoSchemaImpl) generateSrcSchemaStandard(conv *internal.Conv, sis StandardInfoSchema, tables []SchemaAndName, numWorkers int) (int, error) {
	asyncProcessTable := func(t SchemaAndName, mutex *sync.Mutex) task.TaskResult[SchemaAndName] {
		table, e := is.ProcessTable(conv, t, sis)
		mutex.Lock()
		conv.SrcSchema[table.Id] = table
		mutex.Unlock()
		res := task.TaskResult[SchemaAndName]{Result: t, Err: e}
		return res
	}

	r := task.RunParallelTasksImpl[SchemaAndName, SchemaAndName]{}
	_, e := r.RunParallelTasks(tables, numWorkers, asyncProcessTable, false)
	if e != nil {
		return 0, e
	}
	return len(tables), nil
}

// ProcessData performs data conversion for source database
// 'db'. For each table, we extract and convert the data to Spanner data
// (based on the source and Spanner schemas), and write it to Spanner.
// If we can't get/process data for a table, we skip that table and process
// the remaining tables.
func (is *InfoSchemaImpl) ProcessData(conv *internal.Conv, infoSchema InfoSchema, additionalAttributes internal.AdditionalDataAttributes) {
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
func (is *InfoSchemaImpl) SetRowStats(conv *internal.Conv, infoSchema InfoSchema) {
	tables, err := infoSchema.GetTables()
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	for _, t := range tables {
		tableName := infoSchema.GetTableName(t.Schema, t.Name)
		if tableId, err := internal.GetTableIdFromSrcName(conv.SrcSchema, tableName); err == nil && isSkippedPartition(conv, tableId) {
			continue
		}
		count, err := infoSchema.GetRowCount(t)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't get number of rows for table %s", tableName))
			continue
		}
		conv.Stats.Rows[tableName] += count
	}
}

func (is *InfoSchemaImpl) ProcessTable(conv *internal.Conv, table SchemaAndName, infoSchema StandardInfoSchema) (schema.Table, error) {
	var t schema.Table
	logger.Log.Info(fmt.Sprintf("processing schema for table %s", table))
	primaryKeys, checkConstraints, constraints, err := infoSchema.GetConstraints(conv, table)
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

	partitionParent := partitionParentName(infoSchema, table.Schema, table.Name)
	name := infoSchema.GetTableName(table.Schema, table.Name)
	return BuildSchemaTable(table, name, colDefs, colIds, primaryKeys, checkConstraints, indexes, foreignKeys, partitionParent), nil
}

// BuildSchemaTable constructs a schema.Table struct from fetched metadata.
// partitionParent is empty unless the table is a child partition.
func BuildSchemaTable(table SchemaAndName, name string, colDefs map[string]schema.Column, colIds []string, primaryKeys []string, checkConstraints []schema.CheckConstraint, indexes []schema.Index, foreignKeys []schema.ForeignKey, partitionParent string) schema.Table {
	tblId := internal.GenerateTableId()
	colNameIdMap := make(map[string]string)
	for k, v := range colDefs {
		colNameIdMap[v.Name] = k
	}
	var schemaPKeys []schema.Key
	for _, k := range primaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{ColId: colNameIdMap[k]})
	}
	return schema.Table{
		Id:               tblId,
		Name:             name,
		Schema:           table.Schema,
		ColIds:           colIds,
		ColNameIdMap:     colNameIdMap,
		ColDefs:          colDefs,
		PrimaryKeys:      schemaPKeys,
		CheckConstraints: checkConstraints,
		Indexes:          indexes,
		ForeignKeys:      foreignKeys,
		PartitionParent:  partitionParent}
}

// getIncludedSrcTablesFromConv fetches the list of tables
// from the source database that need to be migrated.
func (is *InfoSchemaImpl) GetIncludedSrcTablesFromConv(conv *internal.Conv) (schemaToTablesMap map[string]internal.SchemaDetails, err error) {
	schemaToTablesMap = make(map[string]internal.SchemaDetails)
	for spTable := range conv.SpSchema {
		//lookup the spanner table in the source tables via ID
		srcTable, ok := conv.SrcSchema[spTable]
		if !ok {
			err := fmt.Errorf("id for spanner and source tables do not match, this is most likely a bug")
			return nil, err
		}
		if _, exists := schemaToTablesMap[srcTable.Schema]; !exists {
			schemaToTablesMap[srcTable.Schema] = internal.SchemaDetails{
				TableDetails: []internal.TableDetails{},
			}
		}
		schemaDetails := schemaToTablesMap[srcTable.Schema]
		schemaDetails.TableDetails = append(
			schemaDetails.TableDetails,
			internal.TableDetails{TableName: srcTable.Name},
		)
		schemaToTablesMap[srcTable.Schema] = schemaDetails
	}
	return schemaToTablesMap, nil
}

// SanitizeExpressionsValue removes extra characters added to Default Value in information schema in MySQL.
func SanitizeExpressionsValue(expressionValue string, ty string, generated bool) string {
	types := []string{"char", "varchar", "text", "varbinary", "tinyblob", "tinytext", "text",
		"blob", "mediumtext", "mediumblob", "longtext", "longblob", "STRING"}
	// Check if ty exists in the types array
	stringType := false
	for _, t := range types {
		if t == ty {
			stringType = true
			break
		}
	}
	expressionValue = strings.ReplaceAll(expressionValue, "_utf8mb4", "")
	expressionValue = strings.ReplaceAll(expressionValue, "\\\\", "\\")
	expressionValue = strings.ReplaceAll(expressionValue, "\\'", "'")
	if !generated && stringType && !strings.HasPrefix(expressionValue, "'") && !strings.HasSuffix(expressionValue, "'") {
		expressionValue = "'" + expressionValue + "'"
	}
	return expressionValue
}
