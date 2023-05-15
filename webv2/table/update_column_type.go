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
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// UpdateColumnType updates type of given column to newType.
func UpdateColumnType(newType, tableId, colId string, conv *internal.Conv, w http.ResponseWriter) {
	sp := conv.SpSchema[tableId]

	// update column type for current table.
	err := UpdateColumnTypeChangeTableSchema(conv, tableId, colId, newType, w)
	if err != nil {
		return
	}

	// update column type for refer tables.
	for _, fk := range sp.ForeignKeys {
		fkReferColPosition := getFkColumnPosition(fk.ColIds, colId)
		if fkReferColPosition == -1 {
			continue
		}
		err = UpdateColumnTypeChangeTableSchema(conv, fk.ReferTableId, fk.ReferColumnIds[fkReferColPosition], newType, w)
		if err != nil {
			return
		}
	}

	// update column type for tables referring to the current table.
	for _, sp := range conv.SpSchema {
		for j := 0; j < len(sp.ForeignKeys); j++ {
			if sp.ForeignKeys[j].ReferTableId == tableId {
				fkColPosition := getFkColumnPosition(sp.ForeignKeys[j].ReferColumnIds, colId)
				if fkColPosition == -1 {
					continue
				}
				err = UpdateColumnTypeChangeTableSchema(conv, sp.Id, sp.ForeignKeys[j].ColIds[fkColPosition], newType, w)
				if err != nil {
					return
				}
			}
		}
	}

	// update column type of child table.
	isParent, childTableId := utilities.IsParent(tableId)
	if isParent {
		childColId, err := utilities.GetColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			err = UpdateColumnTypeChangeTableSchema(conv, childTableId, childColId, newType, w)
			if err != nil {
				return
			}
		}
	}

	// update column type of parent table.
	parentTableId := conv.SpSchema[tableId].ParentId
	if parentTableId != "" {
		parentColId, err := utilities.GetColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			err = UpdateColumnTypeChangeTableSchema(conv, parentTableId, parentColId, newType, w)
			if err != nil {
				return
			}
		}
	}
}

func UpdateColumnSize(newSize, tableId, colId string, conv *internal.Conv) {
	sp := conv.SpSchema[tableId]
	UpdateColumnSizeChangeTableSchema(conv, tableId, colId, newSize)
	// update column size of child table.
	isParent, childTableId := utilities.IsParent(tableId)
	if isParent {
		childColId, err := utilities.GetColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			UpdateColumnSizeChangeTableSchema(conv, childTableId, childColId, newSize)
		}
	}

	// update column size of parent table.
	parentTableId := conv.SpSchema[tableId].ParentId
	if parentTableId != "" {
		parentColId, err := utilities.GetColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			UpdateColumnSizeChangeTableSchema(conv, parentTableId, parentColId, newSize)
		}
	}
}

// UpdateColumnSizeTableSchema updates column size to newSize for a column of a table.
func UpdateColumnSizeChangeTableSchema(conv *internal.Conv, tableId string, colId string, newSize string) {
	sp := conv.SpSchema[tableId]
	spColDef := sp.ColDefs[colId]
	len := int64(0)
	if strings.ToLower(newSize) == "max" {
		len = ddl.MaxLength
	} else {
		len, _ = strconv.ParseInt(newSize, 10, 64)
	}
	spColDef.T.Len = len
	sp.ColDefs[colId] = spColDef
	conv.SpSchema[tableId] = sp
}

// UpdateColumnTypeTableSchema updates column type to newtype for a column of a table.
func UpdateColumnTypeChangeTableSchema(conv *internal.Conv, tableId string, colId string, newType string, w http.ResponseWriter) error {
	err := utilities.UpdateDataType(conv, newType, tableId, colId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}
	return nil
}
