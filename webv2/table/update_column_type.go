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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	utilities "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

// UpdateColumnType updates type of given column to newType.
func UpdateColumnType(newType, tableId, colId string, conv *internal.Conv, w http.ResponseWriter) {

	// update column type for current table.
	err := UpdateColumnTypeChangeTableSchema(conv, tableId, colId, newType, w)
	if err != nil {
		return
	}

	// update column type for refer tables.
	err = updateColumnTypeForReferredTable(newType, tableId, colId, conv, w)
	if err != nil {
		return
	}

	// update column type for tables referring to the current table.
	err = updateColumnTypeForReferringTable(newType, tableId, colId, conv, w)
	if err != nil {
		return
	}

	// update column type of child table.
	updateColumnTypeForChildTable(newType, tableId, colId, conv, w)

	// update column type of parent table.
	updateColumnTypeForParentTable(newType, tableId, colId, conv, w)
}

func updateColumnTypeForReferredTable(newType, tableId, colId string, conv *internal.Conv, w http.ResponseWriter) error {
	sp := conv.SpSchema[tableId]
	for _, fk := range sp.ForeignKeys {
		fkReferColPosition := getFkColumnPosition(fk.ColIds, colId)
		if fkReferColPosition == -1 {
			continue
		}
		err := UpdateColumnTypeChangeTableSchema(conv, fk.ReferTableId, fk.ReferColumnIds[fkReferColPosition], newType, w)
		if err != nil {
			return err
		}
		err = updateColumnTypeForReferredTable(newType, fk.ReferTableId, fk.ReferColumnIds[fkReferColPosition], conv, w)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateColumnTypeForReferringTable(newType, tableId, colId string, conv *internal.Conv, w http.ResponseWriter) error {
	for _, sp := range conv.SpSchema {
		for j := 0; j < len(sp.ForeignKeys); j++ {
			if sp.ForeignKeys[j].ReferTableId == tableId {
				fkColPosition := getFkColumnPosition(sp.ForeignKeys[j].ReferColumnIds, colId)
				if fkColPosition == -1 {
					continue
				}
				err := UpdateColumnTypeChangeTableSchema(conv, sp.Id, sp.ForeignKeys[j].ColIds[fkColPosition], newType, w)
				if err != nil {
					return err
				}
				err = updateColumnTypeForReferringTable(newType, sp.Id, sp.ForeignKeys[j].ColIds[fkColPosition], conv, w)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func updateColumnTypeForChildTable(newType, tableId, colId string, conv *internal.Conv, w http.ResponseWriter) {
	sp := conv.SpSchema[tableId]

	isParent, childTableId := utilities.IsParent(tableId)
	if isParent {
		childColId, err := utilities.GetColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			err = UpdateColumnTypeChangeTableSchema(conv, childTableId, childColId, newType, w)
			if err != nil {
				return
			}
			updateColumnTypeForChildTable(newType, childTableId, childColId, conv, w)
		}
	}
}

func updateColumnTypeForParentTable(newType, tableId, colId string, conv *internal.Conv, w http.ResponseWriter) {
	sp := conv.SpSchema[tableId]

	parentTableId := conv.SpSchema[tableId].ParentTable.Id
	if parentTableId != "" {
		parentColId, err := utilities.GetColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			err = UpdateColumnTypeChangeTableSchema(conv, parentTableId, parentColId, newType, w)
			if err != nil {
				return
			}
			updateColumnTypeForParentTable(newType, parentTableId, parentColId, conv, w)
		}
	}
}

func UpdateColumnSize(newSize, tableId, colId string, conv *internal.Conv) {
	UpdateColumnSizeChangeTableSchema(conv, tableId, colId, newSize)
	// update column size of child table.
	updateColumnSizeForChildTable(newSize, tableId, colId, conv)

	// update column size of parent table.
	updateColumnSizeForParentTable(newSize, tableId, colId, conv)
}

func updateColumnSizeForChildTable(newSize, tableId, colId string, conv *internal.Conv) {
	sp := conv.SpSchema[tableId]
	isParent, childTableId := utilities.IsParent(tableId)
	if isParent {
		childColId, err := utilities.GetColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			UpdateColumnSizeChangeTableSchema(conv, childTableId, childColId, newSize)
			updateColumnSizeForChildTable(newSize, childTableId, childColId, conv)
		}
	}
}

func updateColumnSizeForParentTable(newSize, tableId, colId string, conv *internal.Conv) {
	sp := conv.SpSchema[tableId]
	parentTableId := conv.SpSchema[tableId].ParentTable.Id
	if parentTableId != "" {
		parentColId, err := utilities.GetColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			UpdateColumnSizeChangeTableSchema(conv, parentTableId, parentColId, newSize)
			updateColumnSizeForParentTable(newSize, parentTableId, parentColId, conv)
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
