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

// UpdateColumnType updates type of given column to newType.
func UpdateColumnType(newType, table, colName string, conv *internal.Conv, w http.ResponseWriter) {
	sp := conv.SpSchema[table]

	// update column type for current table.
	err := UpdateColumnTypeChangeTableSchema(conv, table, colName, newType, w)
	if err != nil {
		return
	}

	// update column type for refer tables.
	for _, fk := range sp.Fks {
		err = UpdateColumnTypeChangeTableSchema(conv, fk.ReferTable, colName, newType, w)
		if err != nil {
			return
		}
	}

	// update column type for tables referring to the current table.
	for _, sp := range conv.SpSchema {
		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				UpdateColumnTypeChangeTableSchema(conv, sp.Name, colName, newType, w)
			}
		}
	}

	// update column type of child table.
	isParent, childTableName := IsParent(table)
	if isParent {
		err = UpdateColumnTypeChangeTableSchema(conv, childTableName, colName, newType, w)
		if err != nil {
			return
		}
	}

	// update column type of parent table.
	parentTableName := conv.SpSchema[table].Parent
	if parentTableName != "" {
		err = UpdateColumnTypeChangeTableSchema(conv, parentTableName, colName, newType, w)
		if err != nil {
			return
		}
	}
}

// UpdateColumnTypeTableSchema updates column type to newtype for a column of a table.
func UpdateColumnTypeChangeTableSchema(conv *internal.Conv, table string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := conv.ToSource[table].Name
	sp, ty, err := utilities.GetType(conv, newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	colDef := sp.ColDefs[colName]
	colDef.T = ty
	sp.ColDefs[colName] = colDef
	conv.SpSchema[table] = sp

	return nil
}
