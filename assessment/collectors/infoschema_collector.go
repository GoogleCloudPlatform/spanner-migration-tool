/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package assessment

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	collectorCommon "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/common"
	common "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

type InfoSchemaCollector struct {
	tables           map[string]utils.TableAssessmentInfo
	indexes          []utils.IndexAssessmentInfo
	triggers         []utils.TriggerAssessmentInfo
	storedProcedures []utils.StoredProcedureAssessmentInfo
	functions        []utils.FunctionAssessmentInfo
	views            []utils.ViewAssessmentInfo
	conv             *internal.Conv
}

func (c InfoSchemaCollector) IsEmpty() bool {
	if c.conv == nil && c.indexes == nil && c.tables == nil && c.storedProcedures == nil && c.triggers == nil {
		return true
	}
	return false
}

func GetDefaultInfoSchemaCollector(conv *internal.Conv, sourceProfile profiles.SourceProfile) (InfoSchemaCollector, error) {
	return GetInfoSchemaCollector(conv, sourceProfile, collectorCommon.SQLDBConnector{}, collectorCommon.DefaultConnectionConfigProvider{}, getInfoSchema)
}

func GetInfoSchemaCollector(conv *internal.Conv, sourceProfile profiles.SourceProfile, dbConnector collectorCommon.DBConnector, configProvider collectorCommon.ConnectionConfigProvider, infoSchemaProvider func(*sql.DB, profiles.SourceProfile) (common.InfoSchema, error)) (InfoSchemaCollector, error) {
	logger.Log.Info("initializing infoschema collector")
	var errString string
	connectionConfig, err := configProvider.GetConnectionConfig(sourceProfile)
	if err != nil {
		return InfoSchemaCollector{}, err
	}

	db, err := dbConnector.Connect(sourceProfile.Driver, connectionConfig)
	if err != nil {
		return InfoSchemaCollector{}, err
	}

	infoSchema, err := infoSchemaProvider(db, sourceProfile)
	if err != nil {
		return InfoSchemaCollector{}, fmt.Errorf("error getting info schema: %v", err)
	}

	tb, err := infoSchema.GetTableInfo(conv)
	if err != nil {
		errString = errString + fmt.Sprintf("\nError while scanning tables: %v", err)
	}
	indCollector, err := getIndexes(infoSchema, conv)
	if err != nil {
		errString = errString + fmt.Sprintf("\nError while scanning indexes: %v", err)
	}

	triggers, err := infoSchema.GetTriggerInfo()
	if err != nil {
		errString = errString + fmt.Sprintf("\nError while scanning triggers: %v", err)
	}
	sps, err := infoSchema.GetStoredProcedureInfo()
	if err != nil {
		errString = errString + fmt.Sprintf("\nError while scanning stored procedures: %v", err)
	}
	functions, err := infoSchema.GetFunctionInfo()
	if err != nil {
		errString = errString + fmt.Sprintf("\nError while scanning functions: %v", err)
	}
	views, err := infoSchema.GetViewInfo()
	if err != nil {
		errString = errString + fmt.Sprintf("\nError while scanning views: %v", err)
	}
	err = nil
	if errString != "" {
		err = fmt.Errorf(errString, "")
	}
	return InfoSchemaCollector{
		tables:           tb,
		indexes:          indCollector,
		triggers:         triggers,
		conv:             conv,
		storedProcedures: sps,
		functions:        functions,
		views:            views,
	}, err
}

func getIndexes(infoSchema common.InfoSchema, conv *internal.Conv) ([]utils.IndexAssessmentInfo, error) {
	indCollector := []utils.IndexAssessmentInfo{}
	for _, table := range conv.SrcSchema {
		for id := range table.Indexes {
			index, err := infoSchema.GetIndexInfo(table.Name, table.Indexes[id])
			if err != nil {
				return nil, err
			}
			index.TableId = table.Id
			indCollector = append(indCollector, index)
		}
	}
	return indCollector, nil
}

func getInfoSchema(db *sql.DB, sourceProfile profiles.SourceProfile) (common.InfoSchema, error) {
	driver := sourceProfile.Driver
	switch driver {
	case constants.MYSQL:
		return mysql.InfoSchemaImpl{
			Db:     db,
			DbName: sourceProfile.Conn.Mysql.Db,
		}, nil
	default:
		return nil, fmt.Errorf("driver %s not supported", driver)
	}
}

func (c InfoSchemaCollector) ListTables() (map[string]utils.SrcTableDetails, map[string]utils.SpTableDetails) {
	srcTable := make(map[string]utils.SrcTableDetails)
	spTable := make(map[string]utils.SpTableDetails)

	for tableId := range c.tables {
		srcCheckConstraints := make(map[string]schema.CheckConstraint)
		for _, ck := range c.conv.SrcSchema[tableId].CheckConstraints {
			srcCheckConstraints[ck.Id] = ck
		}
		spCheckConstraints := make(map[string]ddl.CheckConstraint)
		for _, ck := range c.conv.SpSchema[tableId].CheckConstraints {
			spCheckConstraints[ck.Id] = ck
		}
		srcFks := make(map[string]utils.SourceForeignKey)
		for _, fk := range c.conv.SrcSchema[tableId].ForeignKeys {
			srcFks[fk.Id] = utils.SourceForeignKey{
				Definition: fk,
				// TODO : Move all print methods to source specific classes
				Ddl: utils.PrintForeignKeyAlterTable(fk, tableId, c.conv.SrcSchema),
			}
		}
		spFks := make(map[string]utils.SpannerForeignKey)
		for _, fk := range c.conv.SpSchema[tableId].ForeignKeys {
			spFks[fk.Id] = utils.SpannerForeignKey{
				Definition:      fk,
				IsInterleavable: canInterleaveWithFK(c.conv.SpSchema, tableId, fk.ReferTableId, &fk),
				ParentTableName: c.conv.SpSchema[fk.ReferTableId].Name,
				Ddl:             fk.PrintForeignKeyAlterTable(c.conv.SpSchema, ddl.Config{}, tableId),
			}
		}
		srcTable[tableId] = utils.SrcTableDetails{
			Id:               tableId,
			Name:             c.conv.SrcSchema[tableId].Name,
			Charset:          c.tables[tableId].Charset,
			Collation:        c.tables[tableId].Collation,
			CheckConstraints: srcCheckConstraints,
			SourceForeignKey: srcFks,
		}
		spTable[tableId] = utils.SpTableDetails{
			Id:                tableId,
			Name:              c.conv.SpSchema[tableId].Name,
			CheckConstraints:  spCheckConstraints,
			SpannerForeignKey: spFks,
		}
	}
	return srcTable, spTable
}

// TODO: move this method to assessment_engine
func canInterleaveWithFK(tableSchemaMap map[string]ddl.CreateTable, childTableId string, parentTableId string, fk *ddl.Foreignkey) bool {
	// Check if both tables exist
	childTable, childOk := tableSchemaMap[childTableId]
	parentTable, parentOk := tableSchemaMap[parentTableId]
	if !childOk || !parentOk {
		return false
	}

	// Check if parent key is a prefix of child key
	if len(parentTable.PrimaryKeys) > len(childTable.PrimaryKeys) {
		return false
	}

	sortedParentKeys := sortPrimaryKeys(parentTable.PrimaryKeys)
	sortedChildKeys := sortPrimaryKeys(childTable.PrimaryKeys)

	for i, parentKey := range sortedParentKeys {
		childKey := childTable.PrimaryKeys[i]
		if tableSchemaMap[parentTableId].ColDefs[parentKey.ColId].Name != tableSchemaMap[childTableId].ColDefs[childKey.ColId].Name {
			return false // Parent key columns must be a prefix of child key columns in the same order
		}
	}

	// Check if foreign key columns match the parent key
	if len(parentTable.PrimaryKeys) != len(fk.ReferColumnIds) {
		return false // Foreign key columns must match the parent primary key columns in number.
	}
	for i, parentKeyCol := range sortedParentKeys {
		if parentKeyCol.ColId != fk.ReferColumnIds[i] {
			return false // Foreign key columns must match the parent primary key columns in order.
		}
		if fk.ColIds[i] != sortedChildKeys[i].ColId {
			return false // Foreign key columns must match child primary key prefix in order.
		}
	}

	return true
}

func sortPrimaryKeys(keys []ddl.IndexKey) []ddl.IndexKey {
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Order < keys[j].Order
	})
	return keys
}

func (c InfoSchemaCollector) ListIndexes() (map[string]utils.SrcIndexDetails, map[string]utils.SpIndexDetails) {
	srcIndexes := make(map[string]utils.SrcIndexDetails)
	spIndexes := make(map[string]utils.SpIndexDetails)

	for i := range c.indexes {
		srcIndexes[c.indexes[i].IndexDef.Id] = utils.SrcIndexDetails{
			Id:        c.indexes[i].IndexDef.Id,
			Name:      c.indexes[i].IndexDef.Name,
			TableId:   c.indexes[i].TableId,
			Type:      c.indexes[i].Ty,
			TableName: c.conv.SrcSchema[c.indexes[i].TableId].Name,
			IsUnique:  c.indexes[i].IndexDef.Unique,
			Ddl:       utils.PrintCreateIndex(c.indexes[i].IndexDef, c.conv.SrcSchema[c.indexes[i].TableId]),
		}
		if _, ok := c.conv.SpSchema[c.indexes[i].TableId]; ok {
			spIndexes[c.indexes[i].IndexDef.Id] = utils.SpIndexDetails{
				Id:        c.indexes[i].IndexDef.Id,
				Name:      internal.ToSpannerIndexName(c.conv, c.indexes[i].IndexDef.Name),
				TableId:   c.indexes[i].TableId,
				IsUnique:  c.indexes[i].IndexDef.Unique,
				TableName: c.conv.SpSchema[c.indexes[i].TableId].Name,
				Ddl:       getSpannerIndex(c.indexes[i].IndexDef.Id, c.conv.SpSchema[c.indexes[i].TableId]).PrintCreateIndex(c.conv.SpSchema[c.indexes[i].TableId], ddl.Config{}),
			}
		}
	}
	return srcIndexes, spIndexes
}

func getSpannerIndex(indexId string, spSchema ddl.CreateTable) ddl.CreateIndex {
	for id := range spSchema.Indexes {
		if spSchema.Indexes[id].Id == indexId {
			return spSchema.Indexes[id]
		}
	}
	return ddl.CreateIndex{}
}

func (c InfoSchemaCollector) ListTriggers() map[string]utils.TriggerAssessment {
	triggersAssessmentOutput := make(map[string]utils.TriggerAssessment)
	for _, trigger := range c.triggers {
		tableId, _ := internal.GetTableIdFromSrcName(c.conv.SrcSchema, trigger.TargetTable)
		triggerId := internal.GenerateTriggerId()
		triggersAssessmentOutput[triggerId] = utils.TriggerAssessment{
			Id:            triggerId,
			Name:          trigger.Name,
			Operation:     trigger.Operation,
			TargetTable:   trigger.TargetTable,
			TargetTableId: tableId,
		}
	}
	return triggersAssessmentOutput
}

func (c InfoSchemaCollector) ListFunctions() map[string]utils.FunctionAssessment {
	functionAssessmentOutput := make(map[string]utils.FunctionAssessment)
	for _, function := range c.functions {
		fnId := internal.GenerateFunctionId()
		functionAssessmentOutput[fnId] = utils.FunctionAssessment{
			Id:          fnId,
			Name:        function.Name,
			Definition:  function.Definition,
			LinesOfCode: strings.Count(function.Definition, ";"),
		}
	}
	return functionAssessmentOutput
}

func (c InfoSchemaCollector) ListViews() map[string]utils.ViewAssessment {
	viewAssessmentOutput := make(map[string]utils.ViewAssessment)
	for _, view := range c.views {
		viewId := internal.GenerateViewId()
		viewAssessmentOutput[viewId] = utils.ViewAssessment{
			Id:            viewId,
			SrcName:       view.Name,
			SrcDefinition: view.Definition,
			SrcViewType:   "NON-MATERIALIZED", // Views are always non-materialized in MySQL
			SpName:        internal.GetSpannerValidName(c.conv, view.Name),
		}
	}
	return viewAssessmentOutput
}

func (c InfoSchemaCollector) ListColumnDefinitions() (map[string]utils.SrcColumnDetails, map[string]utils.SpColumnDetails) {
	srcColumnDetails := make(map[string]utils.SrcColumnDetails)
	spColumnDetails := make(map[string]utils.SpColumnDetails)
	for _, table := range c.conv.SrcSchema {
		for _, column := range table.ColDefs {
			pkOrder := -1
			for _, pk := range table.PrimaryKeys {
				if pk.ColId == column.Id {
					pkOrder = pk.Order
					break
				}
			}
			var foreignKeys []string
			for _, fk := range table.ForeignKeys {
				for _, col := range fk.ColIds {
					if col == column.Id {
						foreignKeys = append(foreignKeys, fk.Name)
						break
					}
				}
			}
			onInsertTimestampSet := false
			if column.DefaultValue.IsPresent && column.DefaultValue.Value.Statement == "CURRENT_TIMESTAMP" {
				onInsertTimestampSet = true
			}
			var isUnsigned, isOnUpdateTimestampSet bool
			var generatedColumn utils.GeneratedColumnInfo
			var maxColumnSize int64
			if table, ok := c.tables[table.Id]; ok {
				if columnAssessment, ok := table.ColumnAssessmentInfos[column.Id]; ok {
					isUnsigned = columnAssessment.IsUnsigned
					isOnUpdateTimestampSet = columnAssessment.IsOnUpdateTimestampSet
					generatedColumn = columnAssessment.GeneratedColumn
					maxColumnSize = columnAssessment.MaxColumnSize
				}
			}

			srcColumnDetails[column.Id] = utils.SrcColumnDetails{
				Id:                     column.Id,
				Name:                   column.Name,
				TableId:                table.Id,
				TableName:              table.Name,
				Datatype:               column.Type.Name,
				NotNull:                column.NotNull,
				Mods:                   column.Type.Mods,
				ArrayBounds:            column.Type.ArrayBounds,
				AutoGen:                column.AutoGen,
				DefaultValue:           column.DefaultValue,
				PrimaryKeyOrder:        pkOrder,
				ForeignKey:             foreignKeys,
				IsUnsigned:             isUnsigned,
				MaxColumnSize:          maxColumnSize,
				GeneratedColumn:        generatedColumn,
				IsOnUpdateTimestampSet: isOnUpdateTimestampSet,
				IsOnInsertTimestampSet: onInsertTimestampSet,
			}
		}
	}
	for _, table := range c.conv.SpSchema {
		for _, column := range table.ColDefs {
			pkOrder := -1
			for _, pk := range table.PrimaryKeys {
				if pk.ColId == column.Id {
					pkOrder = pk.Order
					break
				}
			}
			var foreignKeys []string
			for _, fk := range table.ForeignKeys {
				for _, col := range fk.ColIds {
					if col == column.Id {
						foreignKeys = append(foreignKeys, fk.Name)
						break
					}
				}
			}
			spColumnDetails[column.Id] = utils.SpColumnDetails{
				Id:              column.Id,
				Name:            column.Name,
				TableId:         table.Id,
				TableName:       table.Name,
				Datatype:        column.T.Name,
				NotNull:         column.NotNull,
				Len:             column.T.Len,
				IsArray:         column.T.IsArray,
				AutoGen:         column.AutoGen,
				DefaultValue:    column.DefaultValue,
				PrimaryKeyOrder: pkOrder,
				ForeignKey:      foreignKeys,
			}
		}
	}
	return srcColumnDetails, spColumnDetails
}

func (c InfoSchemaCollector) ListStoredProcedures() map[string]utils.StoredProcedureAssessment {
	storedProcedureAssessmentOutput := make(map[string]utils.StoredProcedureAssessment)
	for _, storedProcedure := range c.storedProcedures {
		spId := internal.GenerateStoredProcedureId()
		storedProcedureAssessmentOutput[spId] = utils.StoredProcedureAssessment{
			Id:          spId,
			Name:        storedProcedure.Name,
			Definition:  storedProcedure.Definition,
			LinesOfCode: strings.Count(storedProcedure.Definition, ";"),
		}
	}
	return storedProcedureAssessmentOutput
}

func (c InfoSchemaCollector) ListSpannerSequences() map[string]ddl.Sequence {
	return c.conv.SpSequences
}
