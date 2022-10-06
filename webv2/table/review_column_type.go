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

// ReviewColumnNameType review update of colum type to given newType.
func ReviewColumnNameType(newType, table, colName string, conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, w http.ResponseWriter) (_ []InterleaveTableSchema, err error) {
	sp := conv.SpSchema[table]

	//review update of column type for refer table
	for _, fk := range sp.Fks {
		err := reviewColumnTypeChangeTableSchema(conv, fk.ReferTable, colName, newType)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
	}

	//review update of column type for table referring to the current table
	for _, sp := range conv.SpSchema {
		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {

				err = reviewColumnTypeChangeTableSchema(conv, sp.Name, colName, newType)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return interleaveTableSchema, err
				}
			}
		}
	}

	// review update of column type for child talbe
	isParent, childTableName := IsParent(table)
	if isParent {
		columnId := conv.SpSchema[childTableName].ColDefs[colName].Id

		previoustype := conv.SpSchema[childTableName].ColDefs[colName].T.Name
		err = reviewColumnTypeChangeTableSchema(conv, childTableName, colName, newType)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, childTableName, columnId, colName, previoustype, newType)
	}

	//review update of column type for parent table
	parentTableName := conv.SpSchema[table].Parent
	if parentTableName != "" {
		columnId := conv.SpSchema[parentTableName].ColDefs[colName].Id

		previoustype := conv.SpSchema[parentTableName].ColDefs[colName].T.Name
		err = reviewColumnTypeChangeTableSchema(conv, parentTableName, colName, newType)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, parentTableName, columnId, colName, previoustype, newType)
	}

	// review update of column type for curren table
	columnId := conv.SpSchema[table].ColDefs[colName].Id
	previoustype := conv.SpSchema[table].ColDefs[colName].T.Name
	err = reviewColumnTypeChangeTableSchema(conv, table, colName, newType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	if childTableName != "" || parentTableName != "" {
		interleaveTableSchema = typeinterleaveTableSchema(interleaveTableSchema, table, columnId, colName, previoustype, newType)
	}

	return interleaveTableSchema, nil
}

// reviewColumnTypeChangeTableSchema review update of column type to given newType.
func reviewColumnTypeChangeTableSchema(conv *internal.Conv, table string, colName string, newType string) error {
	srcTableName := conv.ToSource[table].Name
	sp, ty, err := utilities.GetType(conv, newType, table, colName, srcTableName)

	if err != nil {
		return err
	}

	colDef := sp.ColDefs[colName]
	colDef.T = ty
	sp.ColDefs[colName] = colDef
	conv.SpSchema[table] = sp

	return nil
}
