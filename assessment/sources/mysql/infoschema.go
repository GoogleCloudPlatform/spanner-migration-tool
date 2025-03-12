// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
)

type InfoSchemaImpl struct {
	Db     *sql.DB
	DbName string
}

type SourceSpecificComparisonImpl struct{}

func (isi InfoSchemaImpl) GetTableInfo(conv *internal.Conv) (map[string]utils.TableAssessmentInfo, error) {
	tb := make(map[string]utils.TableAssessmentInfo)
	dbIdentifier := utils.DbIdentifier{
		DatabaseName: isi.DbName,
	}
	for _, table := range conv.SrcSchema {
		columnAssessments := make(map[string]utils.ColumnAssessmentInfo[any])
		for _, column := range table.ColDefs {
			q := `SELECT c.column_type
              FROM information_schema.COLUMNS c
              where table_schema = ? and table_name = ? and column_name = ? ORDER BY c.ordinal_position;`
			var columnType string
			err := isi.Db.QueryRow(q, isi.DbName, table.Name, column.Name).Scan(&columnType)
			if err != nil {
				return nil, fmt.Errorf("couldn't get schema for column %s.%s: %s", table.Name, column.Name, err)
			}
			columnAssessments[column.Id] = utils.ColumnAssessmentInfo[any]{
				Db: utils.DbIdentifier{
					DatabaseName: isi.DbName,
				},
				Name:          column.Name,
				TableName:     table.Name,
				ColumnDef:     column,
				IsUnsigned:    strings.Contains(strings.ToLower(columnType), " unsigned"),
				MaxColumnSize: getColumnMaxSize(column.Type.Name, column.Type.Mods),
			}
		}
		tb[table.Id] = utils.TableAssessmentInfo{Name: table.Name, TableDef: table, ColumnAssessmentInfos: columnAssessments, Db: dbIdentifier}
	}
	return tb, nil
}

// GetIndexes return a list of all indexes for the specified table.
func (isi InfoSchemaImpl) GetIndexInfo(table string, conv *internal.Conv) ([]utils.IndexAssessmentInfo, error) {
	q := `SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE
		FROM INFORMATION_SCHEMA.STATISTICS 
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX;`
	rows, err := isi.Db.Query(q, isi.DbName, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, column, sequence, nonUnique, indexType string
	var collation sql.NullString
	indexMap := make(map[string]utils.IndexAssessmentInfo)
	var indexNames []string
	var indexes []utils.IndexAssessmentInfo
	var errString string
	for rows.Next() {
		if err := rows.Scan(&name, &column, &sequence, &collation, &nonUnique, &indexType); err != nil {
			errString = errString + fmt.Sprintf("Can't scan: %v", err)
			continue
		}
		if _, found := indexMap[name]; !found {
			tableId, _ := internal.GetTableIdFromSrcName(conv.SrcSchema, table)
			indexNames = append(indexNames, name)
			indexMap[name] = utils.IndexAssessmentInfo{
				Ty:      indexType,
				Name:    name,
				TableId: tableId,
				Db: utils.DbIdentifier{
					DatabaseName: isi.DbName,
				},
				IndexDef: schema.Index{
					Id:     internal.GenerateIndexesId(),
					Name:   name,
					Unique: (nonUnique == "0"),
				},
			}

		}
		index := indexMap[name]
		index.IndexDef.Keys = append(index.IndexDef.Keys, schema.Key{
			ColId: column,
			Desc:  (collation.Valid && collation.String == "D"),
		})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		indexes = append(indexes, indexMap[k])
	}
	return indexes, nil
}

func (isi InfoSchemaImpl) GetTriggerInfo() ([]utils.TriggerAssessmentInfo, error) {
	q := `SELECT DISTINCT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT,ACTION_TIMING,EVENT_MANIPULATION
	FROM INFORMATION_SCHEMA.TRIGGERS 
	WHERE EVENT_OBJECT_SCHEMA = ?`
	rows, err := isi.Db.Query(q, isi.DbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, table, actionStmt, actionTiming, eventManipulation string
	var triggers []utils.TriggerAssessmentInfo
	var errString string
	for rows.Next() {
		if err := rows.Scan(&name, &table, &actionStmt, &actionTiming, &eventManipulation); err != nil {
			errString = errString + fmt.Sprintf("Can't scan: %v", err)
			continue
		}
		triggers = append(triggers, utils.TriggerAssessmentInfo{
			Name:              name,
			Operation:         actionStmt,
			TargetTable:       table,
			ActionTiming:      actionTiming,
			EventManipulation: eventManipulation,
			Db: utils.DbIdentifier{
				DatabaseName: isi.DbName,
			},
		})
	}
	return triggers, nil
}

func (isi InfoSchemaImpl) GetStoredProcedureInfo() ([]utils.StoredProcedureAssessmentInfo, error) {
	q := `SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC
	FROM INFORMATION_SCHEMA.ROUTINES 
	WHERE ROUTINE_TYPE='PROCEDURE' AND ROUTINE_SCHEMA = ?`
	rows, err := isi.Db.Query(q, isi.DbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, defintion, isDeterministic string
	var storedProcedures []utils.StoredProcedureAssessmentInfo
	var errString string
	for rows.Next() {
		if err := rows.Scan(&name, &defintion, &isDeterministic); err != nil {
			errString = errString + fmt.Sprintf("Can't scan: %v", err)
			continue
		}
		storedProcedures = append(storedProcedures, utils.StoredProcedureAssessmentInfo{
			Name:            name,
			Definition:      defintion,
			IsDeterministic: isDeterministic == "YES",
			Db: utils.DbIdentifier{
				DatabaseName: isi.DbName,
			},
		})
	}
	return storedProcedures, nil
}

// TODO - also account for charsets
func getColumnMaxSize(dataType string, mods []int64) int64 {
	switch strings.ToLower(dataType) {
	case "date":
		return 4
	case "timestamp":
		return 4
	case "bit":
		return int64(math.Ceil(float64(mods[0]+7) / 8))
	case "int":
		return 4
	case "integer":
		return 4
	case "float":
		return 4 // Add precision pspecific handling
	case "text":
		return 2 ^ 16 //TODO Check for actual storage used and update here
	case "mediumtext":
		return 2 ^ 24
	case "longtext":
		return 2 ^ 32
	default:
		//TODO - add all types
		return 4
	}
}

func (ssa SourceSpecificComparisonImpl) IsDataTypeCodeCompatible(srcColumnDef utils.SrcColumnDetails, spColumnDef utils.SpColumnDetails) bool {

	switch strings.ToUpper(spColumnDef.Datatype) {
	case "BOOL":
		switch srcColumnDef.Datatype {
		case "tinyint":
			return true
		case "bit":
			return true
		default:
			return false
		}
	case "BYTES":
		switch srcColumnDef.Datatype {
		case "binary":
			return true
		case "varbinary":
			return true
		case "blob":
			return true
		default:
			return false
		}
	case "DATE":
		switch srcColumnDef.Datatype {
		case "date":
			return true
		default:
			return false
		}
	case "FLOAT32":
		switch srcColumnDef.Datatype {
		case "float":
			return true
		case "double":
			return true
		default:
			return false
		}
	case "FLOAT64":
		switch srcColumnDef.Datatype {
		case "float":
			return true
		case "double":
			return true
		default:
			return false
		}
	case "INT64":
		switch srcColumnDef.Datatype {
		case "int":
			return true
		case "bigint":
			return true
		default:
			return false
		}
	case "JSON":
		switch srcColumnDef.Datatype {
		case "json":
			return true
		case "varchar":
			return true
		default:
			return false
		}
	case "NUMERIC":
		switch srcColumnDef.Datatype {
		case "float":
			return true
		case "double":
			return true
		default:
			return false
		}
	case "STRING":
		switch srcColumnDef.Datatype {
		case "varchar":
			return true
		case "text":
			return true
		case "mediumtext":
			return true
		case "longtext":
			return true
		default:
			return false
		}
	case "TIMESTAMP":
		switch srcColumnDef.Datatype {
		case "timestamp":
			return true
		case "datetime":
			return true
		default:
			return false
		}
	default:
		//TODO - add all types
		return false
	}

}
