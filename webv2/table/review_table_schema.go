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

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

type ReviewTableSchemaResponse struct {
	DDL     string
	Changes []InterleaveTableSchema
}

type InterleaveTableSchema struct {
	Table                   string
	InterleaveColumnChanges []InterleaveColumn
}

type InterleaveColumn struct {
	ColumnName       string
	Type             string
	UpdateColumnName string
	UpdateType       string
	ColumnId         string
}

// ReviewTableSchema review Spanner Table Schema.
func ReviewTableSchema(w http.ResponseWriter, r *http.Request) {

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var t updateTable

	table := r.FormValue("table")

	err = json.Unmarshal(reqBody, &t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()

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

	interleaveTableSchema := []InterleaveTableSchema{}

	for colName, v := range t.UpdateCols {

		if v.Add {

			addColumn(table, colName, conv)

		}

		if v.Removed {

			removeColumn(table, colName, conv)

		}

		if v.Rename != "" && v.Rename != colName {

			for _, c := range conv.SpSchema[table].ColNames {
				if c == v.Rename {
					http.Error(w, fmt.Sprintf("Multiple columns with similar name cannot exist for column : %v", v.Rename), http.StatusBadRequest)
					return
				}
			}

			interleaveTableSchema = reviewRenameColumn(v.Rename, table, colName, conv, interleaveTableSchema)

			colName = v.Rename
		}

		if v.ToType != "" {

			typeChange, err := utilities.IsTypeChanged(v.ToType, table, colName, conv)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {

				interleaveTableSchema, err = ReviewColumnType(v.ToType, table, colName, conv, interleaveTableSchema, w)
				if err != nil {
					return
				}
			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, table, colName, conv)
		}
	}

	ddl := GetSpannerTableDDL(conv.SpSchema[table], conv.TargetDb)

	interleaveTableSchema = trimRedundantInterleaveTableSchema(interleaveTableSchema)
	// update interleaveTableSchema by filling the missing fields.
	interleaveTableSchema = updateInterleaveTableSchema(conv, interleaveTableSchema)

	resp := ReviewTableSchemaResponse{
		DDL:     ddl,
		Changes: interleaveTableSchema,
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
