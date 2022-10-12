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
func GetSpannerTableDDL(spannerTable ddl.CreateTable) string {

	c := ddl.Config{Comments: true, ProtectIds: false}

	ddl := spannerTable.PrintCreateTable(c)

	return ddl
}

func renameinterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, newName string) []InterleaveTableSchema {

	tindex := isTablePresent(interleaveTableSchema, table)

	interleaveTableSchema = createinterleaveTableSchema(interleaveTableSchema, table, tindex)

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

func createinterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, tindex int) []InterleaveTableSchema {

	if tindex == -1 {
		itc := InterleaveTableSchema{}
		itc.Table = table
		itc.InterleaveColumnChanges = []InterleaveColumn{}

		interleaveTableSchema = append(interleaveTableSchema, itc)
	}

	return interleaveTableSchema
}

func renameInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, table, columnId, colName, newName string) []InterleaveTableSchema {

	tindex := isTablePresent(interleaveTableSchema, table)

	cindex := isColumnPresent(interleaveTableSchema[tindex].InterleaveColumnChanges, columnId)

	interleaveTableSchema = createInterleaveColumn(interleaveTableSchema, tindex, cindex, columnId, colName, newName)

	if tindex != -1 && cindex != -1 {
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].ColumnId = columnId
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].ColumnName = colName
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].UpdateColumnName = newName

	}

	return interleaveTableSchema

}

func createInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, tindex int, cindex int, columnId string, colName string, newName string) []InterleaveTableSchema {

	if cindex == -1 {

		if columnId != "" {

			ic := InterleaveColumn{}
			ic.ColumnId = columnId
			ic.ColumnName = colName
			ic.UpdateColumnName = newName

			interleaveTableSchema[tindex].InterleaveColumnChanges = append(interleaveTableSchema[tindex].InterleaveColumnChanges, ic)
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

func typeinterleaveTableSchema(interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, previoustype string, updateType string) []InterleaveTableSchema {

	tindex := isTablePresent(interleaveTableSchema, table)

	interleaveTableSchema = createinterleaveTableSchema(interleaveTableSchema, table, tindex)

	interleaveTableSchema = typeInterleaveColumn(interleaveTableSchema, table, columnId, colName, previoustype, updateType)
	return interleaveTableSchema
}

func typeInterleaveColumn(interleaveTableSchema []InterleaveTableSchema, table, columnId, colName, previoustype, updateType string) []InterleaveTableSchema {

	tindex := isTablePresent(interleaveTableSchema, table)
	cindex := isColumnPresent(interleaveTableSchema[tindex].InterleaveColumnChanges, columnId)
	interleaveTableSchema = createInterleaveColumntype(interleaveTableSchema, tindex, cindex, columnId, colName, previoustype, updateType)

	if tindex != -1 && cindex != -1 {
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].ColumnId = columnId
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].ColumnName = colName
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].Type = previoustype
		interleaveTableSchema[tindex].InterleaveColumnChanges[cindex].UpdateType = updateType

	}

	return interleaveTableSchema
}

func createInterleaveColumntype(interleaveTableSchema []InterleaveTableSchema, tindex int, cindex int, columnId string, colName string, previoustype string, updateType string) []InterleaveTableSchema {

	if cindex == -1 {
		if columnId != "" {
			ic := InterleaveColumn{}
			ic.ColumnId = columnId
			ic.ColumnName = colName
			ic.Type = previoustype
			ic.UpdateType = updateType
			interleaveTableSchema[tindex].InterleaveColumnChanges = append(interleaveTableSchema[tindex].InterleaveColumnChanges, ic)
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

func updatedInterleaveTableSchema(conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema) []InterleaveTableSchema {
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
	case "ADDED":
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = true
		sp.ColDefs[colName] = spColDef
	case "REMOVED":
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