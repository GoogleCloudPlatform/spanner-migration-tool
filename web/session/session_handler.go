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

package session

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// session contains the metadata for a session file.
// A session file is a snapshot of an ongoing HarbourBridge conversion session,
// and consists of an internal.Conv struct in JSON format.
type sessionParams struct {
	Driver    string `json:"driver"`
	FilePath  string `json:"filePath"`
	DBName    string `json:"dbName"`
	CreatedAt string `json:"createdAt"`
}

func CreateSession(w http.ResponseWriter, r *http.Request) {
	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	now := time.Now()
	sessionState := GetSessionState()

	dbName := sessionState.DbName
	var err error
	if dbName == "" {
		dbName, err = utils.GetDatabaseName(sessionState.Driver, now)
		if err != nil {
			http.Error(w, fmt.Sprintf("Can not create database name : %v", err), http.StatusInternalServerError)
		}
	}
	dirPath, err := conversion.WriteConvGeneratedFiles(sessionState.Conv, dbName, sessionState.Driver, ioHelper.BytesRead, ioHelper.Out)
	if err != nil {
		http.Error(w, fmt.Sprintf("Cannot write files : %v", err), http.StatusInternalServerError)
	}
	filePath := dirPath + dbName + ".session.json"
	session := sessionParams{Driver: sessionState.Driver, FilePath: filePath, DBName: dbName, CreatedAt: now.Format(time.RFC1123)}
	sessionState.DbName = dbName
	sessionState.SessionFile = filePath
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(session)
}

func ResumeLocalSession(w http.ResponseWriter, r *http.Request) {
	sessionState := GetSessionState()

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var s sessionParams
	err = json.Unmarshal(reqBody, &s)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	sessionState.Conv = internal.MakeConv()
	err = conversion.ReadSessionFile(sessionState.Conv, s.FilePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to open the session file: %v", err), http.StatusNotFound)
		return
	}
	sessionState.Driver = s.Driver
	sessionState.DbName = s.DBName
	sessionState.SessionFile = s.FilePath
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionState.Conv)
}

func ResumeRemoteSession(w http.ResponseWriter, r *http.Request) {
	// This function should resume either Remote or Local resume
	// ToDo: Check if user has access to spanner

	vars := mux.Vars(r)
	vid, ok := vars["versionId"]
	if !ok {
		http.Error(w, "VersionId not supplied", http.StatusBadRequest)
		return
	}

	convm, err := resumeRemoteSession(vid)
	if err != nil {
		http.Error(w, "Data access error", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func GetConvSessionsMetadata(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	spannerClient, err := spanner.NewClient(ctx, getSpannerUri())
	if err != nil {
		http.Error(w, fmt.Sprintf("Spanner Client error : %v", err), http.StatusInternalServerError)
		return
	}
	defer spannerClient.Close()

	ssvc := NewSessionService(spannerClient)
	result, err := ssvc.GetSessionsMetadata(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Spanner Transaction error : %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}

func GetConvSession(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vid, ok := vars["versionId"]
	if !ok {
		http.Error(w, "VersionId not supplied", http.StatusBadRequest)
		return
	}

	conv, err := getConvWithMetadata(vid)
	if err != nil {
		http.Error(w, "Data access error", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(conv)
}

func SaveSession(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}

	var sm SessionMetadata
	err = json.Unmarshal(reqBody, &sm)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	spannerClient, err := spanner.NewClient(ctx, getSpannerUri())
	if err != nil {
		http.Error(w, fmt.Sprintf("Spanner Client error : %v", err), http.StatusInternalServerError)
		return
	}
	defer spannerClient.Close()

	sessionState := GetSessionState()
	ssvc := NewSessionService(spannerClient)
	conv, err := json.Marshal(sessionState.Conv)
	if err != nil {
		http.Error(w, fmt.Sprintf("Conv object error : %v", err), http.StatusInternalServerError)
		return
	}

	// TODO: To compute few metadata fields if empty
	t := time.Now()
	scs := SchemaConversionSession{
		VersionId:              uuid.New().String(),
		PreviousVersionId:      []string{},
		SchemaChanges:          "N/A",
		SchemaConversionObject: string(conv),
		CreatedOn:              t,
		SessionMetadata:        sm,
	}

	err = ssvc.SaveSession(ctx, scs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Spanner Transaction error : %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode("Save successful, VersionId : " + scs.VersionId)
}

func getConvWithMetadata(versionId string) (ConvWithMetadata, error) {
	var convm ConvWithMetadata
	ctx := context.Background()
	spannerClient, err := spanner.NewClient(ctx, getSpannerUri())
	if err != nil {
		return convm, err
	}
	defer spannerClient.Close()

	ssvc := NewSessionService(spannerClient)
	convm, err = ssvc.GetConvWithMetadata(ctx, versionId)
	if err != nil {
		return convm, err
	}
	return convm, nil
}

func resumeRemoteSession(vid string) (ConvWithMetadata, error) {
	convm, err := getConvWithMetadata(vid)
	if err != nil {
		return convm, err
	}

	sessionState := GetSessionState()
	sessionState.Conv = &convm.Conv
	return convm, nil
}

func getSpannerUri() string {
	return "projects/searce-academy/instances/appdev-ps1/databases/harbourbridge_metadata"
}
