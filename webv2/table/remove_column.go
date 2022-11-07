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
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// removeColumn remove given column from schema.
func removeColumn(tableId string, colId string, conv *internal.Conv) {

	sp := conv.SpSchema[tableId]

	removeColumnFromTableSchema(conv, tableId, colId)

	// update foreignKey relationship Table column names.
	for _, fk := range sp.ForeignKeys {
		fkReferColPosition := getFkColumnPosition(fk.ColIds, colId)
		if fkReferColPosition == -1 {
			continue
		}

		removeColumnFromTableSchema(conv, fk.ReferTableId, fk.ReferColumnIds[fkReferColPosition])

	}

	for _, sp := range conv.SpSchema {

		for j := 0; j < len(sp.ForeignKeys); j++ {
			if sp.ForeignKeys[j].ReferTableId == tableId {
				fkColPosition := getFkColumnPosition(sp.ForeignKeys[j].ReferColumnIds, colId)
				if fkColPosition == -1 {
					continue
				}
				removeColumnFromTableSchema(conv, sp.Id, sp.ForeignKeys[j].ColIds[fkColPosition])
			}

		}

	}

	isParent, childTableId := IsParent(tableId)

	if isParent {
		childColId, err := getColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			removeColumnFromTableSchema(conv, childTableId, childColId)
		}
	}

	if conv.SpSchema[tableId].ParentId != "" {
		parentTableId := conv.SpSchema[tableId].ParentId
		parentColId, err := getColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			removeColumnFromTableSchema(conv, parentTableId, parentColId)
		}

	}
}

// removeColumnFromCurrentTableSchema remove given column from table schema.
func removeColumnFromTableSchema(conv *internal.Conv, tableId string, colId string) {
	sp := conv.SpSchema[tableId]

	sp = removeColumnFromSpannerColDefs(sp, colId)

	sp = removeColumnFromSpannerPK(sp, colId)

	sp = removeColumnFromSpannerSecondaryIndex(sp, colId)

	sp = removeColumnFromSpannerForeignkeyColumns(sp, colId)

	sp = removeColumnFromSpannerForeignkeyReferColumns(sp, colId)

	sp = removeColumnFromSpannerColNames(sp, colId)

	removeSpannerSchemaIssue(tableId, colId, conv)

	removeColumnFromToSpannerToSource(tableId, colId, conv)

	conv.SpSchema[tableId] = sp
}

// removeColumnFromSpannerColNames remove given column from ColNames.
func removeColumnFromSpannerColNames(sp ddl.CreateTable, colId string) ddl.CreateTable {

	for i, col := range sp.ColIds {
		if col == colId {
			sp.ColIds = utilities.Remove(sp.ColIds, i)
			break
		}
	}
	delete(sp.ColDefs, colId)
	return sp
}

// removeColumnFromSpannerPK remove given column from Primary Key List.
func removeColumnFromSpannerPK(sp ddl.CreateTable, colId string) ddl.CreateTable {

	for i, pk := range sp.PrimaryKeys {
		if pk.ColId == colId {
			sp.PrimaryKeys = utilities.RemovePk(sp.PrimaryKeys, i)
			break
		}
	}
	return sp
}

// removeColumnFromSpannerColDefs remove given column from Spanner ColDefs List.
func removeColumnFromSpannerColDefs(sp ddl.CreateTable, colId string) ddl.CreateTable {
	delete(sp.ColDefs, colId)
	return sp
}

// removeColumnFromSpannerSecondaryIndex remove given column from Spanner SecondaryIndex List.
func removeColumnFromSpannerSecondaryIndex(sp ddl.CreateTable, colId string) ddl.CreateTable {

	for i, index := range sp.Indexes {
		for j, key := range index.Keys {
			if key.ColId == colId {
				sp.Indexes[i].Keys = utilities.RemoveColumnFromSecondaryIndexKey(sp.Indexes[i].Keys, j)
				break
			}
		}
	}
	return sp
}

// removeColumnFromSecondaryIndexKey remove given column from Spanner Secondary Schema Issue List.
func removeSpannerSchemaIssue(tableId string, colId string, conv *internal.Conv) {
	if conv.SchemaIssues != nil {
		if conv.SchemaIssues[tableId] != nil && conv.SchemaIssues[tableId][colId] != nil {
			delete(conv.SchemaIssues[tableId], colId)
		}
	}
}

// removeColumnFromToSpannerToSource remove given column from ToSpanner and ToSource List.
func removeColumnFromToSpannerToSource(tableId string, colId string, conv *internal.Conv) {
	srcTableName := conv.SrcSchema[tableId].Name
	spTableName := conv.SpSchema[tableId].Name
	srcColName := conv.SrcSchema[tableId].ColDefs[colId].Name
	spColName := conv.SpSchema[tableId].ColDefs[colId].Name

	delete(conv.ToSource[spTableName].Cols, spColName)
	delete(conv.ToSpanner[srcTableName].Cols, srcColName)
}

// removeColumnFromSpannerForeignkeyColumns remove given column from Spanner Foreignkey Columns List.
func removeColumnFromSpannerForeignkeyColumns(sp ddl.CreateTable, colId string) ddl.CreateTable {

	for i, fk := range sp.ForeignKeys {
		j := 0
		for _, id := range fk.ColIds {
			if id == colId {
				sp.ForeignKeys[i].ColIds = utilities.RemoveFkColumn(fk.ColIds, j)
			} else {
				j = j + 1
			}
		}
	}

	// drop foreing key if the foreign key doesn't have any column left after the update.
	i := 0
	for _, fk := range sp.ForeignKeys {
		if len(fk.ColIds) <= 0 {
			sp.ForeignKeys = append(sp.ForeignKeys[:i], sp.ForeignKeys[i+1:]...)
		}
	}
	return sp
}

// removeColumnFromSpannerForeignkeyReferColumns remove given column from Spanner Foreignkey Refer Columns List.
func removeColumnFromSpannerForeignkeyReferColumns(sp ddl.CreateTable, colId string) ddl.CreateTable {

	for i, fk := range sp.ForeignKeys {
		j := 0
		for _, id := range fk.ReferColumnIds {
			if id == colId {
				sp.ForeignKeys[i].ReferColumnIds = utilities.RemoveFkReferColumns(sp.ForeignKeys[i].ReferColumnIds, j)
			} else {
				j = j + 1
			}
		}
	}
	// drop foreign key if the foreign key doesn't have any refer-column left after the update.
	i := 0
	for _, fk := range sp.ForeignKeys {
		if len(fk.ReferColumnIds) <= 0 {
			sp.ForeignKeys = append(sp.ForeignKeys[:i], sp.ForeignKeys[i+1:]...)
		}
	}
	return sp
}
