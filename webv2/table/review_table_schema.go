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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
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

		interleavingImpact := IsInterleavingImpacted(v, tableId, colId, conv)

		if interleavingImpact != "" {
			http.Error(w, interleavingImpact, http.StatusBadRequest)
			return
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

			sp := conv.SpSchema[tableId]
			column, ok := sp.ColDefs[colId]
			if ok {
				column.Name = v.Rename
				sp.ColDefs[colId] = column
				conv.SpSchema[tableId] = sp
			}
		}

		_, found := conv.SrcSchema[tableId].ColDefs[colId]
		if v.ToType != "" && found {

			typeChange, err := utilities.IsTypeChanged(v.ToType, tableId, colId, conv)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {
				sp, ty, err := utilities.GetType(conv, v.ToType, tableId, colId)

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
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
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
				sp := conv.SpSchema[tableId]
				colDef := sp.ColDefs[colId]
				colDef.T.Len = colMaxLength
				sp.ColDefs[colId] = colDef
				conv.SpSchema[tableId] = sp
			}
		}

		if !v.Removed && !v.Add && v.Rename == "" {
			sequences := UpdateAutoGenCol(v.AutoGen, tableId, colId, conv)
			conv.SpSequences = sequences
			UpdateDefaultValue(v.DefaultValue, tableId, colId, conv)
			UpdateGeneratedCol(v.GeneratedColumn, tableId, colId, conv)
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
