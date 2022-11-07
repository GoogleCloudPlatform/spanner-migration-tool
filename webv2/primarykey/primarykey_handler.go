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

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/index"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"

	"github.com/google/uuid"
)

// PrimaryKeyRequest represents  Primary keys API Payload.
type PrimaryKeyRequest struct {
	TableId string   `json:"TableId"`
	Columns []Column `json:"Columns"`
}

// PrimaryKeyResponse represents  Primary keys API response.
// Synth is true is for table Primary Key Id is not present and it is generated.
type PrimaryKeyResponse struct {
	TableId string   `json:"TableId"`
	Columns []Column `json:"Columns"`
	Synth   bool     `json:"Synth"`
}

// Column represents  SpannerTables Column.
type Column struct {
	ColumnId string `json:"ColumnId"`
	Desc     bool   `json:"Desc"`
	Order    int    `json:"Order"`
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

	spannerTable = updatePrimaryKey(pkRequest, spannerTable)

	//update spannerTable into sessionState.Conv.SpSchema.
	for _, table := range sessionState.Conv.SpSchema {
		if pkRequest.TableId == table.Id {
			sessionState.Conv.SpSchema[table.Id] = spannerTable
			for _, ind := range spannerTable.Indexes {
				index.RemoveIndexIssues(spannerTable.Id, ind)
			}
		}
	}

	RemoveInterleave(sessionState.Conv, spannerTable)
	session.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)

	log.Println("request completed", "traceid", id.String(), "method", r.Method, "path", r.URL.Path, "remoteaddr", r.RemoteAddr)
}

// PrimaryKeyResponse represents primary key API response.
// Synth is true for tables in which primary key is not present and is generated.
func prepareResponse(pkRequest PrimaryKeyRequest, spannerTable ddl.CreateTable) PrimaryKeyResponse {

	var pKeyResponse PrimaryKeyResponse

	pKeyResponse.TableId = pkRequest.TableId

	var isSynthPrimaryKey bool

	for i := 0; i < len(spannerTable.ColIds); i++ {
		if spannerTable.ColIds[i] == "synth_id" {
			isSynthPrimaryKey = true
		}
	}

	pKeyResponse.Synth = isSynthPrimaryKey

	for _, indexkey := range spannerTable.PrimaryKeys {

		responseColumn := Column{}
		responseColumn.ColumnId = indexkey.ColId
		responseColumn.Desc = indexkey.Desc
		responseColumn.Order = indexkey.Order
		pKeyResponse.Columns = append(pKeyResponse.Columns, responseColumn)
	}
	return pKeyResponse
}
