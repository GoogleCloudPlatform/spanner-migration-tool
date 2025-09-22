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

// reviewRenameColumn review  renaming of Columnname in schmema.
func reviewRenameColumn(newName, tableId, colId string, conv *internal.Conv) {
	reviewRenameColumnNameTableSchema(conv, tableId, colId, newName)
}

// reviewRenameColumnNameTableSchema review  renaming of column-name in Table Schema.
func reviewRenameColumnNameTableSchema(conv *internal.Conv, tableId, colId, newName string) {
	sp := conv.SpSchema[tableId]

	column, ok := sp.ColDefs[colId]

	if ok {
		column.Name = newName

		sp.ColDefs[colId] = column
		conv.SpSchema[tableId] = sp

	}
}
