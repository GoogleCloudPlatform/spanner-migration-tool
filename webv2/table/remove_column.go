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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	utilities "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

// removeColumn remove given column from schema.
func RemoveColumn(tableId string, colId string, conv *internal.Conv) {

	sp := conv.SpSchema[tableId]

	// remove interleaving if the column to be removed is used in interleaving.
	isParent, childTableId := utilities.IsParent(tableId)
	if isParent {
		if isColFistOderPk(conv.SpSchema[tableId].PrimaryKeys, colId) {
			childSp := conv.SpSchema[childTableId]
			childSp.ParentId = ""
			conv.SpSchema[childTableId] = childSp
		}
	}

	if conv.SpSchema[tableId].ParentId != "" {
		if isColFistOderPk(conv.SpSchema[tableId].PrimaryKeys, colId) {
			sp.ParentId = ""
			conv.SpSchema[tableId] = sp
		}
	}

	// remove foreign keys from refer tables.
	for id, sp := range conv.SpSchema {
		var updatedFks []ddl.Foreignkey
		for j := 0; j < len(sp.ForeignKeys); j++ {
			if sp.ForeignKeys[j].ReferTableId == tableId {
				fkColPosition := getFkColumnPosition(sp.ForeignKeys[j].ReferColumnIds, colId)
				if fkColPosition == -1 {
					updatedFks = append(updatedFks, sp.ForeignKeys[j])
				} else {
					delete(conv.UsedNames, sp.ForeignKeys[j].Name)
				}
			} else {
				updatedFks = append(updatedFks, sp.ForeignKeys[j])
			}
		}

		sp.ForeignKeys = updatedFks
		conv.SpSchema[id] = sp
	}

	//remove column from the table.
	removeColumnFromTableSchema(conv, tableId, colId)

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
	conv.SchemaIssuesLock.Lock()
	defer conv.SchemaIssuesLock.Unlock()
	if conv.SchemaIssues != nil {
		if conv.SchemaIssues[tableId].ColumnLevelIssues != nil && conv.SchemaIssues[tableId].ColumnLevelIssues[colId] != nil {
			delete(conv.SchemaIssues[tableId].ColumnLevelIssues, colId)
		}
	}
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
