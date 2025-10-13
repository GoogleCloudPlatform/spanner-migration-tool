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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	utilities "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

type ReviewTableSchemaResponse struct {
	DDL string
}

// ReviewTableSchema review Spanner Table Schema.
func ReviewTableSchema(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var t updateTable

	tableId := r.FormValue("table")

	err = json.Unmarshal(reqBody, &t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()

	var conv *internal.Conv

	convByte, err := json.Marshal(sessionState.Conv)

	if err != nil {
		http.Error(w, fmt.Sprintf("conversion object parse error : %v", err), http.StatusInternalServerError)
		return
	}
	if err := json.Unmarshal(convByte, &conv); err != nil {
		http.Error(w, fmt.Sprintf("conversion object parse error : %v", err), http.StatusInternalServerError)
		return
	}

	conv.UsedNames = internal.ComputeUsedNames(conv)

	for colId, v := range t.UpdateCols {

		isPkColumn := false
		pkOrder := -1
		for _, pk := range conv.SpSchema[tableId].PrimaryKeys {
			if pk.ColId == colId {
				isPkColumn = true
				pkOrder = pk.Order
				break
			}
		}

		if isPkColumn {
			isModification := false
			isRename := v.Rename != "" && v.Rename != conv.SpSchema[tableId].ColDefs[colId].Name
			isTypeChange, _ := utilities.IsTypeChanged(v.ToType, tableId, colId, conv)

			var isSizeChange bool
			if v.MaxColLength != "" {
				var colMaxLength int64
				if strings.ToLower(v.MaxColLength) == "max" {
					colMaxLength = ddl.MaxLength
				} else {
					colMaxLength, _ = strconv.ParseInt(v.MaxColLength, 10, 64)
				}
				if conv.SpSchema[tableId].ColDefs[colId].T.Len != colMaxLength {
					isSizeChange = true
				}
			}
			if v.Removed || isRename || isTypeChange || isSizeChange {
				isModification = true
			}

			if isModification {
				isParent, childTableIds := utilities.IsParent(tableId)
				isChild := conv.SpSchema[tableId].ParentTable.Id != ""

				// Rule 1: If it's a parent table, any change to a PK column is disallowed.
				if isParent {
					http.Error(w, fmt.Sprintf("Modifying primary key column '%s' is not allowed because table '%s' is a parent in an interleave relationship with '%s'. Please remove the interleave relationship first.", conv.SpSchema[tableId].ColDefs[colId].Name, conv.SpSchema[tableId].Name, strings.Join(childTableIds, ", ")), http.StatusBadRequest)
					return
				}

				// Rule 2: If it's a child table, check if the PK column is part of the parent's key.
				if isChild {
					parentTableId := conv.SpSchema[tableId].ParentTable.Id
					parentTable, parentExists := conv.SpSchema[parentTableId]
					if !parentExists {
						// This would be an inconsistent state, but handle it.
						http.Error(w, fmt.Sprintf("Internal server error: Parent table with ID %s not found for interleaved table %s", parentTableId, conv.SpSchema[tableId].Name), http.StatusInternalServerError)
						return
					}
					numParentPKs := len(parentTable.PrimaryKeys)

					// If the column's order in the PK is within the count of parent PKs, it's an inherited key.
					if pkOrder != -1 && pkOrder <= numParentPKs {
						http.Error(w, fmt.Sprintf("Modifying column '%s' is not allowed because it is part of the interleaved primary key from parent table '%s'. Please remove the interleave relationship first.", conv.SpSchema[tableId].ColDefs[colId].Name, parentTable.Name), http.StatusBadRequest)
						return
					}
				}
			}
		}

		if v.Add {
			addColumn(tableId, colId, conv)
		}

		if v.Removed {
			RemoveColumn(tableId, colId, conv)
		}

		if v.Rename != "" && v.Rename != conv.SpSchema[tableId].ColDefs[colId].Name {

			for _, c := range conv.SpSchema[tableId].ColDefs {
				if strings.EqualFold(c.Name, v.Rename) {
					http.Error(w, fmt.Sprintf("Multiple columns with similar name cannot exist for column : %v", v.Rename), http.StatusBadRequest)
					return
				}
			}
			oldName := conv.SpSchema[tableId].ColDefs[colId].Name
			// Using a regular expression to match the exact column name
			re := regexp.MustCompile(`\b` + regexp.QuoteMeta(oldName) + `\b`)

			for i := range conv.SpSchema[tableId].CheckConstraints {
				originalString := conv.SpSchema[tableId].CheckConstraints[i].Expr
				updatedValue := re.ReplaceAllString(originalString, v.Rename)
				conv.SpSchema[tableId].CheckConstraints[i].Expr = updatedValue
			}

			reviewRenameColumn(v.Rename, tableId, colId, conv)

		}

		_, found := conv.SrcSchema[tableId].ColDefs[colId]
		if v.ToType != "" && found {

			typeChange, err := utilities.IsTypeChanged(v.ToType, tableId, colId, conv)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {
				err = ReviewColumnType(v.ToType, tableId, colId, conv, w)
				if err != nil {
					return
				}
			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, tableId, colId, conv)
		}

		if v.MaxColLength != "" {
			var colMaxLength int64
			if strings.ToLower(v.MaxColLength) == "max" {
				colMaxLength = ddl.MaxLength
			} else {
				colMaxLength, _ = strconv.ParseInt(v.MaxColLength, 10, 64)
			}
			if conv.SpSchema[tableId].ColDefs[colId].T.Len != colMaxLength {
				ReviewColumnSize(colMaxLength, tableId, colId, conv)
			}
		}

		if !v.Removed && !v.Add && v.Rename == "" {
			sequences := UpdateAutoGenCol(v.AutoGen, tableId, colId, conv)
			conv.SpSequences = sequences
			UpdateDefaultValue(v.DefaultValue, tableId, colId, conv)
		}
	}

	ddl := GetSpannerTableDDL(conv.SpSchema[tableId], conv.SpDialect, sessionState.Driver)

	resp := ReviewTableSchemaResponse{
		DDL: ddl,
	}

	sessionMetaData := session.GetSessionState().SessionMetadata
	if sessionMetaData.DatabaseName == "" || sessionMetaData.DatabaseType == "" || sessionMetaData.SessionName == "" {
		sessionMetaData.DatabaseName = sessionState.DbName
		sessionMetaData.DatabaseType = sessionState.Driver
		sessionMetaData.SessionName = "NewSession"
	}
	session.GetSessionState().SessionMetadata = sessionMetaData
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}
