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
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

//UpdateColumnType updates type of given column to newType.
func UpdateColumnType(newType, table, colName string, Conv *internal.Conv, w http.ResponseWriter) {
	sp := Conv.SpSchema[table]

	//update column type for current table
	err := UpdateColumnTypeChangeTableSchema(Conv, table, colName, newType, w)
	if err != nil {
		return
	}

	//update column type for refer tables
	for _, fk := range sp.Fks {
		err = UpdateColumnTypeChangeTableSchema(Conv, fk.ReferTable, colName, newType, w)
		if err != nil {
			return
		}
	}

	//update column type for tables referring to the current table
	for _, sp := range Conv.SpSchema {
		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				UpdateColumnTypeChangeTableSchema(Conv, sp.Name, colName, newType, w)
			}
		}
	}

	// update column type of child table
	isParent, childTableName := IsParent(table)
	if isParent {
		err = UpdateColumnTypeChangeTableSchema(Conv, childTableName, colName, newType, w)
		if err != nil {
			return
		}
	}

	// update column type of parent table
	parentTableName := Conv.SpSchema[table].Parent
	if parentTableName != "" {
		err = UpdateColumnTypeChangeTableSchema(Conv, parentTableName, colName, newType, w)
		if err != nil {
			return
		}
	}
}

//UpdateColumnTypeTableSchema updates column type to newtype for a column of a table.
func UpdateColumnTypeChangeTableSchema(Conv *internal.Conv, table string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := Conv.ToSource[table].Name
	sp, ty, err := utilities.GetType(Conv, newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	colDef := sp.ColDefs[colName]
	colDef.T = ty
	sp.ColDefs[colName] = colDef
	Conv.SpSchema[table] = sp

	return nil
}
