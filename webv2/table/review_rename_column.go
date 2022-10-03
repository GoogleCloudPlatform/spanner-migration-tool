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
)

// reviewRenameColumn review  renaming of Columnname in schmema.
func reviewRenameColumn(newName, table, colName string, Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema) []InterleaveTableSchema {

	sp := Conv.SpSchema[table]

	columnId := sp.ColDefs[colName].Id

	// update column name for refer tables
	for _, fk := range sp.Fks {
		reviewRenameColumnNameTableSchema(Conv, fk.ReferTable, colName, newName)
	}

	// update column name for table which are referring to the current table
	for _, sp := range Conv.SpSchema {
		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				reviewRenameColumnNameTableSchema(Conv, sp.Name, colName, newName)
			}
		}
	}

	// review column name update for interleaved child
	isParent, childTableName := IsParent(table)

	if isParent {
		reviewRenameColumnNameTableSchema(Conv, childTableName, colName, newName)
		if _, ok := Conv.SpSchema[childTableName].ColDefs[colName]; ok {
			childColumnId := Conv.SpSchema[childTableName].ColDefs[colName].Id
			interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, childTableName, childColumnId, colName, newName)
		}
	}

	// review column name update for interleaved parent
	parentTableName := Conv.SpSchema[table].Parent

	if parentTableName != "" {
		reviewRenameColumnNameTableSchema(Conv, parentTableName, colName, newName)
		if _, ok := Conv.SpSchema[parentTableName].ColDefs[colName]; ok {
			parentColumnId := Conv.SpSchema[parentTableName].ColDefs[colName].Id
			interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, parentTableName, parentColumnId, colName, newName)
		}
	}

	reviewRenameColumnNameTableSchema(Conv, table, colName, newName)
	if childTableName != "" || parentTableName != "" {
		interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, table, columnId, colName, newName)
	}

	return interleaveTableSchema
}

// reviewRenameColumnNameTableSchema review  renaming of column-name in Table Schema.
func reviewRenameColumnNameTableSchema(Conv *internal.Conv, tableName, colName, newName string) {
	sp := Conv.SpSchema[tableName]

	_, ok := sp.ColDefs[colName]

	if ok {
		{
			sp = renameColumnNameInSpannerColNames(sp, colName, newName)
			sp = renameColumnNameInSpannerColDefs(sp, colName, newName)
			sp = renameColumnNameInSpannerPK(sp, colName, newName)
			sp = renameColumnNameInSpannerSecondaryIndex(sp, colName, newName)
			sp = renameColumnNameInSpannerForeignkeyColumns(sp, colName, newName)
			sp = renameColumnNameInSpannerForeignkeyReferColumns(sp, colName, newName)
			sp = renameColumnNameInSpannerColNames(sp, colName, newName)
			renameColumnNameInToSpannerToSource(tableName, colName, newName, Conv)
			renameColumnNameInSpannerSchemaIssue(tableName, colName, newName, Conv)

			Conv.SpSchema[tableName] = sp
		}
	}
}
