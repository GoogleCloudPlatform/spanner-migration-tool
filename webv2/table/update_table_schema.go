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
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// Actions to be performed on a column.
// (1) Add : Add column if true.
// (2) Removed: Remove column if true.
// (3) Rename: New name or empty string.
// (4) NotNull: "ADDED", "REMOVED" or "".
// (5) ToType: New type or empty string.
type updateCol struct {
	Add          bool   `json:"Add"`
	Removed      bool   `json:"Removed"`
	Rename       string `json:"Rename"`
	NotNull      string `json:"NotNull"`
	ToType       string `json:"ToType"`
	MaxColLength string `json:MaxColLength`
}

type updateTable struct {
	UpdateCols map[string]updateCol `json:"UpdateCols"`
}

// updateTableSchema updates the Spanner schema.
// Following actions can be performed on a specified table:
// (1) Add column.
// (2) Remove column.
// (3) Rename column.
// (4) Add or Remove NotNull constraint.
// (5) Update Spanner type.
func UpdateTableSchema(w http.ResponseWriter, r *http.Request) {

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

	var conv *internal.Conv
	conv = nil
	conv = sessionState.Conv

	for colId, v := range t.UpdateCols {

		if v.Add {

			addColumn(tableId, colId, conv)

		}

		if v.Removed {

			RemoveColumn(tableId, colId, conv)

		}

		if v.Rename != "" && v.Rename != conv.SpSchema[tableId].ColDefs[colId].Name {

			renameColumn(v.Rename, tableId, colId, conv)
		}

		if v.ToType != "" {

			typeChange, err := utilities.IsTypeChanged(v.ToType, tableId, colId, conv)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {

				UpdateColumnType(v.ToType, tableId, colId, conv, w)

			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, tableId, colId, conv)
		}
		if v.MaxColLength != "" {
			sp := conv.SpSchema[tableId]
			spColDef := sp.ColDefs[colId]
			if strings.ToLower(v.MaxColLength) == "max" {
				spColDef.T.Len = ddl.MaxLength
			} else {
				spColDef.T.Len, _ = strconv.ParseInt(v.MaxColLength, 10, 64)
			}
			sp.ColDefs[colId] = spColDef
			conv.SpSchema[tableId] = sp
		}
	}

	common.ComputeNonKeyColumnSize(conv, tableId)

	delete(conv.SpSchema[tableId].ColDefs, "")
	sessionState.Conv = conv

	session.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}
