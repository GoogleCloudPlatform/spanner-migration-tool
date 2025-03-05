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
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
)

type InfoSchemaImpl struct {
	Db     *sql.DB
	DbName string
}

func (isi InfoSchemaImpl) GetTableInfo(conv *internal.Conv) ([]utils.TableAssessment, error) {
	tb := []utils.TableAssessment{}
	dbIdentifier := utils.DbIdentifier{
		DatabaseName: isi.DbName,
	}
	for _, table := range conv.SrcSchema {
		columnAssessments := []utils.ColumnAssessment[any]{}
		for _, column := range table.ColDefs {
			q := `SELECT c.column_type
              FROM information_schema.COLUMNS c
              where table_schema = ? and table_name = ? and column_name = ? ORDER BY c.ordinal_position;`
			cols, err := isi.Db.Query(q, isi.DbName, table.Name, column.Name)
			if err != nil {
				return nil, fmt.Errorf("couldn't get schema for column %s.%s: %s", table.Name, column.Name, err)
			}
			defer cols.Close()
			var columnType string
			for cols.Next() {
				err := cols.Scan(&columnType)
				if err != nil {
					conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
					continue
				}
			}
			columnAssessments = append(columnAssessments, utils.ColumnAssessment[any]{
				Db: utils.DbIdentifier{
					DatabaseName: isi.DbName,
				},
				Name:       column.Name,
				TableName:  table.Name,
				ColumnDef:  column,
				IsUnsigned: strings.Contains(strings.ToLower(columnType), " unsigned"),
			})
		}
		tb = append(tb, utils.TableAssessment{Name: table.Name, TableDef: table, ColumnAssessments: columnAssessments, Db: dbIdentifier})
	}
	return tb, nil
}

// GetIndexes return a list of all indexes for the specified table.
func (isi InfoSchemaImpl) GetIndexInfo(table string) ([]utils.IndexAssessment, error) {
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
	indexMap := make(map[string]utils.IndexAssessment)
	var indexNames []string
	var indexes []utils.IndexAssessment
	var errString string
	for rows.Next() {
		if err := rows.Scan(&name, &column, &sequence, &collation, &nonUnique, &indexType); err != nil {
			errString = errString + fmt.Sprintf("Can't scan: %v", err)
			continue
		}
		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = utils.IndexAssessment{
				Ty:        indexType,
				Name:      name,
				TableName: table,
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

func (isi InfoSchemaImpl) GetTriggerInfo() ([]utils.TriggerAssessment, error) {
	q := `SELECT DISTINCT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT,ACTION_TIMING,EVENT_MANIPULATION
	FROM INFORMATION_SCHEMA.TRIGGERS 
	WHERE EVENT_OBJECT_SCHEMA = ?`
	rows, err := isi.Db.Query(q, isi.DbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, table, actionStmt, actionTiming, eventManipulation string
	var triggers []utils.TriggerAssessment
	var errString string
	for rows.Next() {
		if err := rows.Scan(&name, &table, &actionStmt, &actionTiming, &eventManipulation); err != nil {
			errString = errString + fmt.Sprintf("Can't scan: %v", err)
			continue
		}
		triggers = append(triggers, utils.TriggerAssessment{
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

func (isi InfoSchemaImpl) GetStoredProcedureInfo() ([]utils.StoredProcedureAssessment, error) {
	q := `SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC
	FROM INFORMATION_SCHEMA.ROUTINES 
	WHERE ROUTINE_TYPE='PROCEDURE' AND ROUTINE_SCHEMA = ?`
	rows, err := isi.Db.Query(q, isi.DbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, defintion, isDeterministic string
	var storedProcedures []utils.StoredProcedureAssessment
	var errString string
	for rows.Next() {
		if err := rows.Scan(&name, &defintion, &isDeterministic); err != nil {
			errString = errString + fmt.Sprintf("Can't scan: %v", err)
			continue
		}
		storedProcedures = append(storedProcedures, utils.StoredProcedureAssessment{
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
