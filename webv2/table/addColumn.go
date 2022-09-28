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
func addColumn(table string, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]
	src := Conv.SrcSchema[table]

	srcColumnId := src.ColDefs[colName].Id

	sp.ColDefs[colName] = ddl.ColumnDef{
		Id:   srcColumnId,
		Name: colName,
	}

	if IsColumnPresentInColNames(sp.ColNames, colName) == false {

		sp.ColNames = append(sp.ColNames, colName)

	}

	Conv.SpSchema[table] = sp

	srcTableName := Conv.ToSource[table].Name
	srcColName := src.ColDefs[colName].Name

	Conv.ToSpanner[srcTableName].Cols[srcColName] = colName
	Conv.ToSource[table].Cols[colName] = srcColName
}
