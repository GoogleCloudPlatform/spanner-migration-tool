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
)

// addColumn add given column into spannerTable.
func addColumn(tableId string, colId string, conv *internal.Conv) {

	sp := conv.SpSchema[tableId]

	spColName, _ := internal.GetSpannerCol(conv, tableId, colId, conv.SpSchema[tableId].ColDefs, false)

	sp.ColDefs[colId] = ddl.ColumnDef{
		Id:   colId,
		Name: spColName,
	}

	if !IsColumnPresentInColNames(sp.ColIds, colId) {

		sp.ColIds = append(sp.ColIds, colId)

	}

	conv.SpSchema[tableId] = sp
}
