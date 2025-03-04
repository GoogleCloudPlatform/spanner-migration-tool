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

	common "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

type InfoSchemaCollector struct {
	tables           []utils.TableAssessment
	indexes          []utils.IndexAssessment
	triggers         []utils.TriggerAssessment
	storedProcedures []utils.StoredProcedureAssessment
	conv             *internal.Conv
}

func (c InfoSchemaCollector) IsEmpty() bool {
	if c.conv == nil && c.indexes == nil && c.tables == nil && c.storedProcedures == nil && c.triggers == nil {
		return true
	}
	return false
}

func CreateInfoSchemaCollector(conv *internal.Conv, sourceProfile profiles.SourceProfile) (InfoSchemaCollector, error) {
	logger.Log.Info("initializing infoschema collector")
	var errString string
	infoSchema, err := getInfoSchema(sourceProfile)
	if err != nil {
		return InfoSchemaCollector{}, err
	}
	tb := infoSchema.GetTableInfo(conv)
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
	}, err
}

func getIndexes(infoSchema common.InfoSchema, conv *internal.Conv) ([]utils.IndexAssessment, error) {
	indCollector := []utils.IndexAssessment{}
	for _, table := range conv.SrcSchema {
		index, err := infoSchema.GetIndexInfo(table.Name, conv)
		if err != nil {
			return nil, err
		}
		indCollector = append(indCollector, index...)
	}
	return indCollector, nil
}

func getInfoSchema(sourceProfile profiles.SourceProfile) (common.InfoSchema, error) {
	connectionConfig, err := conversion.ConnectionConfig(sourceProfile)
	if err != nil {
		return nil, err
	}
	driver := sourceProfile.Driver
	switch driver {
	case constants.MYSQL:
		db, err := sql.Open(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return mysql.InfoSchemaImpl{
			Db:     db,
			DbName: sourceProfile.Conn.Mysql.Db,
		}, nil
	default:
		return nil, fmt.Errorf("driver %s not supported", driver)
	}
}

func (c InfoSchemaCollector) ListTables() (map[string]utils.TableDetails, map[string]utils.TableDetails) {
	srcTable := make(map[string]utils.TableDetails)
	spTable := make(map[string]utils.TableDetails)

	for i := range c.tables {
		tableId := c.tables[i].TableDef.Id
		srcTable[tableId] = utils.TableDetails{
			Id:   tableId,
			Name: c.conv.SrcSchema[tableId].Name,
		}
		spTable[tableId] = utils.TableDetails{
			Id:   tableId,
			Name: c.conv.SpSchema[tableId].Name,
		}
	}
	return srcTable, spTable
}

func (c InfoSchemaCollector) ListColumns() map[string][]string {
	columnNames := make(map[string][]string)
	for i := range c.tables {
		var columnArray []string
		for j := range c.tables[i].ColumnAssessments {
			columnArray = append(columnArray, c.tables[i].ColumnAssessments[j].Name)
		}
		columnNames[c.tables[i].Name] = columnArray
	}
	return columnNames
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
		}
		spIndexes[c.indexes[i].IndexDef.Id] = utils.SpIndexDetails{
			Id:        c.indexes[i].IndexDef.Id,
			Name:      internal.ToSpannerIndexName(c.conv, c.indexes[i].IndexDef.Name),
			TableId:   c.indexes[i].TableId,
			IsUnique:  c.indexes[i].IndexDef.Unique,
			TableName: c.conv.SpSchema[c.indexes[i].TableId].Name,
		}
	}
	return srcIndexes, spIndexes
}

func (c InfoSchemaCollector) ListTriggers() map[string]utils.TriggerAssessmentOutput {
	triggersAssessmentOutput := make(map[string]utils.TriggerAssessmentOutput)
	for _, trigger := range c.triggers {
		tableId, _ := internal.GetTableIdFromSrcName(c.conv.SrcSchema, trigger.TargetTable)
		triggerId := internal.GenerateTriggerId()
		triggersAssessmentOutput[triggerId] = utils.TriggerAssessmentOutput{
			Id:            triggerId,
			Name:          trigger.Name,
			Operation:     trigger.Operation,
			TargetTable:   trigger.TargetTable,
			TargetTableId: tableId,
		}
	}
	return triggersAssessmentOutput
}

func (c InfoSchemaCollector) ListColumnDetails() (map[string]utils.SrcColumnDetails, map[string]utils.SpColumnDetails) {
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
			srcColumnDetails[column.Id] = utils.SrcColumnDetails{
				TableName:       table.Name,
				Datatype:        column.Type.Name,
				IsNull:          !column.NotNull,
				Mods:            column.Type.Mods,
				ArrayBounds:     column.Type.ArrayBounds,
				AutoGen:         column.AutoGen,
				DefaultValue:    column.DefaultValue,
				PrimaryKeyOrder: pkOrder,
				ForeignKey:      foreignKeys,
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
				TableName:       table.Name,
				Datatype:        column.T.Name,
				IsNull:          !column.NotNull,
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

func (c InfoSchemaCollector) ListStoredProcedures() map[string]utils.StoredProcedureAssessmentOutput {
	storedProcedureAssessmentOutput := make(map[string]utils.StoredProcedureAssessmentOutput)
	for _, storedProcedure := range c.storedProcedures {
		spId := internal.GenerateStoredProcedureId()
		storedProcedureAssessmentOutput[spId] = utils.StoredProcedureAssessmentOutput{
			Id:         spId,
			Name:       storedProcedure.Name,
			Definition: storedProcedure.Definition,
		}
	}
	return storedProcedureAssessmentOutput
}
