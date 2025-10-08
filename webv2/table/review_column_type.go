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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	utilities "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

// ReviewColumnNameType review update of column type to given newType.
func ReviewColumnType(newType, tableId, colId string, conv *internal.Conv, w http.ResponseWriter) (err error) {
	// review update of column type for refer table.
	err = reviewColumnTypeForReferredTable(newType, tableId, colId, conv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	// review update of column type for table referring to the current table.
	err = reviewColumnTypeForReferringTable(newType, tableId, colId, conv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}
	// review update of column type for current table.
	err = reviewColumnTypeChangeTableSchema(conv, tableId, colId, newType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}
	return nil
}

func reviewColumnTypeForReferredTable(newType, tableId, colId string, conv *internal.Conv) (err error) {
	sp := conv.SpSchema[tableId]
	for _, fk := range sp.ForeignKeys {
		fkReferColPosition := getFkColumnPosition(fk.ColIds, colId)
		if fkReferColPosition == -1 {
			continue
		}
		err = reviewColumnTypeChangeTableSchema(conv, fk.ReferTableId, fk.ReferColumnIds[fkReferColPosition], newType)
		if err != nil {
			return err
		}
		err = reviewColumnTypeForReferredTable(newType, fk.ReferTableId, fk.ReferColumnIds[fkReferColPosition], conv)
		if err != nil {
			return err
		}
	}
	return nil
}

func reviewColumnTypeForReferringTable(newType, tableId, colId string, conv *internal.Conv) (err error) {
	for _, sp := range conv.SpSchema {
		for j := 0; j < len(sp.ForeignKeys); j++ {
			if sp.ForeignKeys[j].ReferTableId == tableId {
				fkColPosition := getFkColumnPosition(sp.ForeignKeys[j].ReferColumnIds, colId)
				if fkColPosition == -1 {
					continue
				}
				err = reviewColumnTypeChangeTableSchema(conv, sp.Id, sp.ForeignKeys[j].ColIds[fkColPosition], newType)
				if err != nil {
					return err
				}
				err = reviewColumnTypeForReferredTable(newType, sp.Id, sp.ForeignKeys[j].ColIds[fkColPosition], conv)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ReviewColumnSize(colSize int64, tableId, colId string, conv *internal.Conv) {
	// review update of column size for current table.
	reviewColumnSizeChangeTableSchema(conv, tableId, colId, colSize)
}

// reviewColumnTypeChangeTableSchema review update of column type to given newType.
func reviewColumnTypeChangeTableSchema(conv *internal.Conv, tableId string, colId string, newType string) error {
	sp, ty, err := utilities.GetType(conv, newType, tableId, colId)

	if err != nil {
		return err
	}

	colDef := sp.ColDefs[colId]
	colDef.T = ty
	if conv.Source == constants.CASSANDRA {
		toddl := cassandra.InfoSchemaImpl{}.GetToDdl()
		if optionProvider, ok := toddl.(common.OptionProvider); ok {
			srcCol := conv.SrcSchema[tableId].ColDefs[colId]
			option := optionProvider.GetTypeOption(srcCol.Type.Name, ty)
			if colDef.Opts == nil {
				colDef.Opts = make(map[string]string)
			}
			colDef.Opts["cassandra_type"] = option
		}
	}
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
