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
func reviewRenameColumn(newName, table, colName string, conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema) []InterleaveTableSchema {

	sp := conv.SpSchema[table]

	columnId := sp.ColDefs[colName].Id

	// update column name for refer tables.
	for _, fk := range sp.Fks {
		reviewRenameColumnNameTableSchema(conv, fk.ReferTable, colName, newName)
	}

	// update column name for table which are referring to the current table.
	for _, sp := range conv.SpSchema {
		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				reviewRenameColumnNameTableSchema(conv, sp.Name, colName, newName)
			}
		}
	}

	// review column name update for interleaved child.
	isParent, childTableName := IsParent(table)

	if isParent {
		reviewRenameColumnNameTableSchema(conv, childTableName, colName, newName)
		if _, ok := conv.SpSchema[childTableName].ColDefs[colName]; ok {
			childColumnId := conv.SpSchema[childTableName].ColDefs[colName].Id
			interleaveTableSchema = renameInterleaveTableSchema(interleaveTableSchema, childTableName, childColumnId, colName, newName)
		}
	}

	// review column name update for interleaved parent.
	parentTableName := conv.SpSchema[table].Parent

	if parentTableName != "" {
		reviewRenameColumnNameTableSchema(conv, parentTableName, colName, newName)
		if _, ok := conv.SpSchema[parentTableName].ColDefs[newName]; ok {
			parentColumnId := conv.SpSchema[parentTableName].ColDefs[newName].Id
			interleaveTableSchema = renameInterleaveTableSchema(interleaveTableSchema, parentTableName, parentColumnId, colName, newName)
		}
	}

	reviewRenameColumnNameTableSchema(conv, table, colName, newName)
	if childTableName != "" || parentTableName != "" {
		interleaveTableSchema = renameInterleaveTableSchema(interleaveTableSchema, table, columnId, colName, newName)
	}

	return interleaveTableSchema
}

// reviewRenameColumnNameTableSchema review  renaming of column-name in Table Schema.
func reviewRenameColumnNameTableSchema(conv *internal.Conv, tableName, colName, newName string) {
	sp := conv.SpSchema[tableName]

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
			renameColumnNameInToSpannerToSource(tableName, colName, newName, conv)
			renameColumnNameInSpannerSchemaIssue(tableName, colName, newName, conv)

			conv.SpSchema[tableName] = sp
		}
	}
}
