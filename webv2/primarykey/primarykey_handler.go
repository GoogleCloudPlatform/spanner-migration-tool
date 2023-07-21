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

package primarykey

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/index"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/table"
	"github.com/google/uuid"
)

// PrimaryKeyRequest represents  Primary keys API Payload.
type PrimaryKeyRequest struct {
	TableId string         `json:"TableId"`
	Columns []ddl.IndexKey `json:"Columns"`
}

// PrimaryKeyResponse represents  Primary keys API response.
// Synth is true is for table Primary Key Id is not present and it is generated.
type PrimaryKeyResponse struct {
	TableId string         `json:"TableId"`
	Columns []ddl.IndexKey `json:"Columns"`
	Synth   bool           `json:"Synth"`
}

// primaryKey updates Primary keys in Spanner Table.
func PrimaryKey(w http.ResponseWriter, r *http.Request) {

	id := uuid.New()

	log.Println("request started", "traceid", id.String(), "method", r.Method, "path", r.URL.Path)

	reqBody, err := ioutil.ReadAll(r.Body)

	if err != nil {
		log.Println("request's body Read Error")
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	pkRequest := PrimaryKeyRequest{}

	err = json.Unmarshal(reqBody, &pkRequest)

	if err != nil {
		log.Println("request's Body parse error")
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()
	spannerTable, found := getSpannerTable(sessionState, pkRequest)

	if !found {
		log.Println("TableId not found")
		http.Error(w, fmt.Sprintf("tableId not found"), http.StatusNotFound)
		return

	}

	if len(pkRequest.Columns) == 0 {
		log.Println("Empty columm error")
		http.Error(w, fmt.Sprintf("empty columm error"), http.StatusBadRequest)
		return

	}

	if !isValidColumnIds(pkRequest, spannerTable) {
		log.Println("ColummId not found error")
		http.Error(w, fmt.Sprintf("colummId not found error"), http.StatusBadRequest)
		return

	}

	if isValidColumnOrder(pkRequest) {
		log.Println("Two primary key column can  not have same order")
		http.Error(w, fmt.Sprintf("two primary key column can  not have same order"), http.StatusBadRequest)
		return

	}

	UpdatePrimaryKeyAndSessionFile(pkRequest)

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)

	log.Println("request completed", "traceid", id.String(), "method", r.Method, "path", r.URL.Path, "remoteaddr", r.RemoteAddr)
}

func UpdatePrimaryKeyAndSessionFile(pkRequest PrimaryKeyRequest) {

	sessionState := session.GetSessionState()
	spannerTable, _ := getSpannerTable(sessionState, pkRequest)
	tableId := spannerTable.Id
	synthColId := ""
	if synthCol, found := sessionState.Conv.SyntheticPKeys[tableId]; found {
		synthColId = synthCol.ColId
	}

	spannerTable, isSynthPkRemoved := updatePrimaryKey(pkRequest, spannerTable, synthColId)

	if isSynthPkRemoved {
		synthPks := sessionState.Conv.SyntheticPKeys
		delete(synthPks, tableId)
		sessionState.Conv.SyntheticPKeys = synthPks
		table.RemoveColumn(tableId, synthColId, sessionState.Conv)
		colIds := []string{}
		for _, colId := range spannerTable.ColIds {
			if colId != synthColId {
				colIds = append(colIds, colId)
			}
		}
		spannerTable.ColIds = colIds
	}

	//update spannerTable into sessionState.Conv.SpSchema.
	for _, table := range sessionState.Conv.SpSchema {
		if pkRequest.TableId == table.Id {
			sessionState.Conv.SpSchema[table.Id] = spannerTable
			for _, ind := range spannerTable.Indexes {
				index.RemoveIndexIssues(spannerTable.Id, ind)
			}
		}
	}
	common.ComputeNonKeyColumnSize(sessionState.Conv, pkRequest.TableId)
	RemoveInterleave(sessionState.Conv, spannerTable)
	session.UpdateSessionFile()

}
