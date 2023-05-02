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
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
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
	isParent, childTableId := utilities.IsParent(tableId)
	if isParent {
		childColId, err := utilities.GetColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			previousType := conv.SpSchema[childTableId].ColDefs[childColId].T.Name
			previousSize := int(conv.SpSchema[childTableId].ColDefs[childColId].T.Len)
			err = reviewColumnTypeChangeTableSchema(conv, childTableId, childColId, newType)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return interleaveTableSchema, err
			}

			childTableName := conv.SpSchema[childTableId].Name
			childColName := conv.SpSchema[childTableId].ColDefs[childColId].Name
			previousSize, newSize := populateColumnSize(previousType, newType, previousSize, 0)
			interleaveTableSchema = updateTypeOfInterleaveTableSchema(interleaveTableSchema, childTableName, childColId, childColName, previousType, newType, previousSize, newSize)
		}
	}

	// review update of column type for parent table.
	parentTableId := conv.SpSchema[tableId].ParentId
	if parentTableId != "" {
		parentColId, err := utilities.GetColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			previousType := conv.SpSchema[parentTableId].ColDefs[parentColId].T.Name
			previousSize := int(conv.SpSchema[parentTableId].ColDefs[parentColId].T.Len)
			err = reviewColumnTypeChangeTableSchema(conv, parentTableId, parentColId, newType)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return interleaveTableSchema, err
			}

			parentTableName := conv.SpSchema[parentTableId].Name
			parentColName := conv.SpSchema[parentTableId].ColDefs[parentColId].Name
			previousSize, newSize := populateColumnSize(previousType, newType, previousSize, 0)
			interleaveTableSchema = updateTypeOfInterleaveTableSchema(interleaveTableSchema, parentTableName, parentColId, parentColName, previousType, newType, previousSize, newSize)
		}
	}

	// review update of column type for curren table.
	previousType := conv.SpSchema[tableId].ColDefs[colId].T.Name
	previousSize := int(conv.SpSchema[tableId].ColDefs[colId].T.Len)
	err = reviewColumnTypeChangeTableSchema(conv, tableId, colId, newType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return interleaveTableSchema, err
	}

	if childTableId != "" || parentTableId != "" {
		tableName := conv.SpSchema[tableId].Name
		colName := conv.SpSchema[tableId].ColDefs[colId].Name
		previousSize, newSize := populateColumnSize(previousType, newType, previousSize, 0)
		interleaveTableSchema = updateTypeOfInterleaveTableSchema(interleaveTableSchema, tableName, colId, colName, previousType, newType, previousSize, newSize)
	}

	return interleaveTableSchema, nil
}

func ReviewColumnSize(colSize int64, tableId, colId string, conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema, w http.ResponseWriter) ([]InterleaveTableSchema, error) {
	sp := conv.SpSchema[tableId]
	// review update of column size for child talbe.
	isParent, childTableId := utilities.IsParent(tableId)
	if isParent {
		childColId, err := utilities.GetColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			colType := conv.SpSchema[childTableId].ColDefs[childColId].T.Name
			previousSize := int(conv.SpSchema[childTableId].ColDefs[childColId].T.Len)
			reviewColumnSizeChangeTableSchema(conv, childTableId, childColId, colSize)
			childTableName := conv.SpSchema[childTableId].Name
			childColName := conv.SpSchema[childTableId].ColDefs[childColId].Name
			previousSize, newSize := populateColumnSize(colType, colType, int(previousSize), int(colSize))
			interleaveTableSchema = updateSizeOfInterleaveTableSchema(interleaveTableSchema, childTableName, childColId, childColName, colType, previousSize, newSize)
		}
	}

	// review update of column size for parent table.
	parentTableId := conv.SpSchema[tableId].ParentId
	if parentTableId != "" {
		parentColId, err := utilities.GetColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			colType := conv.SpSchema[parentTableId].ColDefs[parentColId].T.Name
			previousSize := int(conv.SpSchema[parentTableId].ColDefs[parentColId].T.Len)
			reviewColumnSizeChangeTableSchema(conv, parentTableId, parentColId, colSize)
			parentTableName := conv.SpSchema[parentTableId].Name
			parentColName := conv.SpSchema[parentTableId].ColDefs[parentColId].Name
			previousSize, newSize := populateColumnSize(colType, colType, int(previousSize), int(colSize))
			interleaveTableSchema = updateSizeOfInterleaveTableSchema(interleaveTableSchema, parentTableName, parentColId, parentColName, colType, previousSize, newSize)
		}
	}

	// review update of column size for current table.
	colType := conv.SpSchema[tableId].ColDefs[colId].T.Name
	previousSize := int(conv.SpSchema[tableId].ColDefs[colId].T.Len)
	reviewColumnSizeChangeTableSchema(conv, tableId, colId, colSize)

	if childTableId != "" || parentTableId != "" {
		tableName := conv.SpSchema[tableId].Name
		colName := conv.SpSchema[tableId].ColDefs[colId].Name
		previousSize, newSize := populateColumnSize(colType, colType, int(previousSize), int(colSize))
		interleaveTableSchema = updateSizeOfInterleaveTableSchema(interleaveTableSchema, tableName, colId, colName, colType, previousSize, newSize)
	}
	return interleaveTableSchema, nil
}

func populateColumnSize(previousType, newType string, prevSize int, newSize int) (int, int) {
	if newType == ddl.String || newType == ddl.Bytes {
		if newSize == 0 {
			return prevSize, ddl.MaxLength
		} else {
			return prevSize, newSize
		}
	}
	return prevSize, newSize
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

// review update of column size to given newSize.
func reviewColumnSizeChangeTableSchema(conv *internal.Conv, tableId string, colId string, newSize int64) {
	sp := conv.SpSchema[tableId]
	colDef := sp.ColDefs[colId]
	colDef.T.Len = newSize
	sp.ColDefs[colId] = colDef
	conv.SpSchema[tableId] = sp
}
