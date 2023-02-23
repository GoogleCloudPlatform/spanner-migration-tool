// Copyright 2022 Google LLC
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

package table

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

const (
	NotNullAdded   string = "ADDED"
	NotNullRemoved string = "REMOVED"
)

// IsColumnPresentInColNames check column is present in colnames.
func IsColumnPresentInColNames(colNames []string, columnName string) bool {

	for _, column := range colNames {
		if column == columnName {
			return true
		}
	}

	return false
}

// GetSpannerTableDDL return Spanner Table DDL as string.
func GetSpannerTableDDL(spannerTable ddl.CreateTable, spDialect string) string {

	c := ddl.Config{Comments: true, ProtectIds: false, SpDialect: spDialect}

	ddl := spannerTable.PrintCreateTable(c)

	return ddl
}

func renameInterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, newName string) []InterleaveTableSchema {

	tableIndex := isTablePresent(interleaveTableSchema, table)

	interleaveTableSchema = createInterleaveTableSchema(interleaveTableSchema, table, tableIndex)

	interleaveTableSchema = renameInterleaveColumn(interleaveTableSchema, table, columnId, colName, newName)

	return interleaveTableSchema
}

func isTablePresent(interleaveTableSchema []InterleaveTableSchema, table string) int {

	for i := 0; i < len(interleaveTableSchema); i++ {
		if interleaveTableSchema[i].Table == table {
			return i
		}
	}

	return -1
}

func createInterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, tableIndex int) []InterleaveTableSchema {

	if tableIndex == -1 {
		itc := InterleaveTableSchema{}
		itc.Table = table
		itc.InterleaveColumnChanges = []InterleaveColumn{}

		interleaveTableSchema = append(interleaveTableSchema, itc)
	}

	return interleaveTableSchema
}

func renameInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, table, columnId, colName, newName string) []InterleaveTableSchema {

	tableIndex := isTablePresent(interleaveTableSchema, table)

	colIndex := isColumnPresent(interleaveTableSchema[tableIndex].InterleaveColumnChanges, columnId)

	interleaveTableSchema = createInterleaveColumn(interleaveTableSchema, tableIndex, colIndex, columnId, colName, newName)

	if tableIndex != -1 && colIndex != -1 {
		interleaveTableSchema[tableIndex].InterleaveColumnChanges[colIndex].ColumnId = columnId
		interleaveTableSchema[tableIndex].InterleaveColumnChanges[colIndex].ColumnName = colName
		interleaveTableSchema[tableIndex].InterleaveColumnChanges[colIndex].UpdateColumnName = newName

	}

	return interleaveTableSchema

}

func createInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, tableIndex int, colIndex int, columnId string, colName string, newName string) []InterleaveTableSchema {

	if colIndex == -1 {

		if columnId != "" {

			interleaveColumn := InterleaveColumn{}
			interleaveColumn.ColumnId = columnId
			interleaveColumn.ColumnName = colName
			interleaveColumn.UpdateColumnName = newName

			interleaveTableSchema[tableIndex].InterleaveColumnChanges = append(interleaveTableSchema[tableIndex].InterleaveColumnChanges, interleaveColumn)
		}
	}

	return interleaveTableSchema
}

func isColumnPresent(interleaveColumn []InterleaveColumn, columnId string) int {

	for i := 0; i < len(interleaveColumn); i++ {
		if interleaveColumn[i].ColumnId == columnId {
			return i
		}
	}

	return -1
}

func updateTypeOfInterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, previousType string, updateType string) []InterleaveTableSchema {

	tableIndex := isTablePresent(interleaveTableSchema, table)

	interleaveTableSchema = createInterleaveTableSchema(interleaveTableSchema, table, tableIndex)

	interleaveTableSchema = updateTypeOfInterleaveColumn(interleaveTableSchema, table, columnId, colName, previousType, updateType)
	return interleaveTableSchema
}

func updateTypeOfInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, table, columnId, colName, previousType, updateType string) []InterleaveTableSchema {

	tableIndex := isTablePresent(interleaveTableSchema, table)
	colIndex := isColumnPresent(interleaveTableSchema[tableIndex].InterleaveColumnChanges, columnId)
	interleaveTableSchema = createInterleaveColumnType(interleaveTableSchema, tableIndex, colIndex, columnId, colName, previousType, updateType)

	if tableIndex != -1 && colIndex != -1 {
		interleaveTableSchema[tableIndex].InterleaveColumnChanges[colIndex].ColumnId = columnId
		interleaveTableSchema[tableIndex].InterleaveColumnChanges[colIndex].ColumnName = colName
		interleaveTableSchema[tableIndex].InterleaveColumnChanges[colIndex].Type = previousType
		interleaveTableSchema[tableIndex].InterleaveColumnChanges[colIndex].UpdateType = updateType
	}

	return interleaveTableSchema
}

func createInterleaveColumnType(interleaveTableSchema []InterleaveTableSchema, tableIndex int, colIndex int, columnId string, colName string, previousType string, updateType string) []InterleaveTableSchema {

	if colIndex == -1 {
		if columnId != "" {
			interleaveColumn := InterleaveColumn{}
			interleaveColumn.ColumnId = columnId
			interleaveColumn.ColumnName = colName
			interleaveColumn.Type = previousType
			interleaveColumn.UpdateType = updateType
			interleaveTableSchema[tableIndex].InterleaveColumnChanges = append(interleaveTableSchema[tableIndex].InterleaveColumnChanges, interleaveColumn)
		}
	}

	return interleaveTableSchema
}

func trimRedundantInterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema) []InterleaveTableSchema {
	updatedInterleaveTableSchema := []InterleaveTableSchema{}
	for _, v := range interleaveTableSchema {
		if len(v.InterleaveColumnChanges) > 0 {
			updatedInterleaveTableSchema = append(updatedInterleaveTableSchema, v)
		}
	}
	return updatedInterleaveTableSchema
}

func updateInterleaveTableSchema(conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema) []InterleaveTableSchema {
	for k, v := range interleaveTableSchema {
		table := v.Table
		for ind, col := range v.InterleaveColumnChanges {
			if col.UpdateColumnName == "" {
				interleaveTableSchema[k].InterleaveColumnChanges[ind].UpdateColumnName = col.ColumnName
			}
			if col.Type == "" {
				interleaveTableSchema[k].InterleaveColumnChanges[ind].Type = conv.SpSchema[table].ColDefs[col.UpdateColumnName].T.Name
			}
			if col.UpdateType == "" {
				interleaveTableSchema[k].InterleaveColumnChanges[ind].UpdateType = conv.SpSchema[table].ColDefs[col.UpdateColumnName].T.Name
			}
		}
	}
	return interleaveTableSchema
}

func UpdateNotNull(notNullChange, table, colName string, conv *internal.Conv) {

	sp := conv.SpSchema[table]

	switch notNullChange {
	case NotNullAdded:
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = true
		sp.ColDefs[colName] = spColDef
	case NotNullRemoved:
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = false
		sp.ColDefs[colName] = spColDef
	}
}

func IsParent(table string) (bool, string) {
	sessionState := session.GetSessionState()

	for _, spSchema := range sessionState.Conv.SpSchema {
		if spSchema.Parent == table {
			return true, spSchema.Name
		}
	}
	return false, ""
}
