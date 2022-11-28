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

// renameColumn renames given column to newname and update in schema.
func renameColumn(newName, tableId, colId string, conv *internal.Conv) {

	sp := conv.SpSchema[tableId]

	// update interleave table relation.
	isParent, childTableId := IsParent(tableId)

	if isParent {
		childColId, err := getColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			renameColumnNameTableSchema(conv, childTableId, childColId, newName)
		}
	}

	if conv.SpSchema[tableId].ParentId != "" {
		parentTableId := conv.SpSchema[tableId].ParentId
		parentColId, err := getColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			renameColumnNameTableSchema(conv, parentTableId, parentColId, newName)
		}
	}
	renameColumnNameTableSchema(conv, tableId, colId, newName)
}

// renameColumnNameInCurrentTableSchema renames given column in Table Schema.
func renameColumnNameTableSchema(conv *internal.Conv, tableId string, colId string, newName string) {
	sp := conv.SpSchema[tableId]

	column, ok := sp.ColDefs[colId]

	if ok {

		column.Name = newName

		sp.ColDefs[colId] = column
		conv.SpSchema[tableId] = sp

	}
}
