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
)

// renameColumn renames given column to newname and update in schema.
func renameColumn(newName, tableId, colId string, conv *internal.Conv) {
	spTable := conv.SpSchema[tableId]

	spColumn, ok := spTable.ColDefs[colId]

	if ok {

		spColumn.Name = newName

		spTable.ColDefs[colId] = spColumn
		conv.SpSchema[tableId] = spTable

		// Update ToSpanner mapping to reflect the column rename
		if conv.ToSpanner != nil {
			srcTable := conv.SrcSchema[tableId]
			srcColumn, ok := srcTable.ColDefs[colId]
			if ok {
				conv.ToSpanner[srcTable.Name].Cols[srcColumn.Name] = newName
			}
		}

	}
}