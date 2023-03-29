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
func ReviewColumnType(newType, tableId, colId string, conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, w http.ResponseWriter) (_ []InterleaveTableSchema, err error) {
	sp := conv.SpSchema[tableId]

	// review update of column type for refer table.
	for _, fk := range sp.ForeignKeys {
		fkReferColPosition := getFkColumnPosition(fk.ColIds, colId)
		if fkReferColPosition == -1 {
			continue
		}
		err = reviewColumnTypeChangeTableSchema(conv, fk.ReferTableId, fk.ReferColumnIds[fkReferColPosition], newType)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return interleaveTableSchema, err
		}
	}

	// review update of column type for table referring to the current table.
	for _, sp := range conv.SpSchema {
		for j := 0; j < len(sp.ForeignKeys); j++ {
			if sp.ForeignKeys[j].ReferTableId == tableId {
				fkColPosition := getFkColumnPosition(sp.ForeignKeys[j].ReferColumnIds, colId)
				if fkColPosition == -1 {
					continue
				}
				err = reviewColumnTypeChangeTableSchema(conv, sp.Id, sp.ForeignKeys[j].ColIds[fkColPosition], newType)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return interleaveTableSchema, err
				}
			}
		}
	}

	// review update of column type for child talbe.
	isParent, childTableId := IsParent(tableId)
	if isParent {
		childColId, err := getColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			previousType := conv.SpSchema[childTableId].ColDefs[childColId].T.Name
			err = reviewColumnTypeChangeTableSchema(conv, childTableId, childColId, newType)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return interleaveTableSchema, err
			}

			childTableName := conv.SpSchema[childTableId].Name
			childColName := conv.SpSchema[childTableId].ColDefs[childColId].Name
			interleaveTableSchema = updateTypeOfInterleaveTableSchema(interleaveTableSchema, childTableName, childColId, childColName, previousType, newType)
		}
	}

	// review update of column type for parent table.
	parentTableId := conv.SpSchema[tableId].ParentId
	if parentTableId != "" {
		parentColId, err := getColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			previousType := conv.SpSchema[parentTableId].ColDefs[parentColId].T.Name
			err = reviewColumnTypeChangeTableSchema(conv, parentTableId, parentColId, newType)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return interleaveTableSchema, err
			}

			parentTableName := conv.SpSchema[parentTableId].Name
			parentColName := conv.SpSchema[parentTableId].ColDefs[parentColId].Name
			interleaveTableSchema = updateTypeOfInterleaveTableSchema(interleaveTableSchema, parentTableName, parentColId, parentColName, previousType, newType)
		}
	}

	// review update of column type for curren table.
	previousType := conv.SpSchema[tableId].ColDefs[colId].T.Name
	err = reviewColumnTypeChangeTableSchema(conv, tableId, colId, newType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	if childTableId != "" || parentTableId != "" {
		tableName := conv.SpSchema[tableId].Name
		colName := conv.SpSchema[tableId].ColDefs[colId].Name
		interleaveTableSchema = updateTypeOfInterleaveTableSchema(interleaveTableSchema, tableName, colId, colName, previousType, newType)
	}

	return interleaveTableSchema, nil
}

// reviewColumnTypeChangeTableSchema review update of column type to given newType.
func reviewColumnTypeChangeTableSchema(conv *internal.Conv, tableId string, colId string, newType string) error {
	sp, ty, err := utilities.GetType(conv, newType, tableId, colId)

	if err != nil {
		return err
	}

	colDef := sp.ColDefs[colId]
	colDef.T = ty
	sp.ColDefs[colId] = colDef
	conv.SpSchema[tableId] = sp

	return nil
}
