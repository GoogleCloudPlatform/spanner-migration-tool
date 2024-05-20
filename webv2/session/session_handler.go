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
	"time"

	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	helpers "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// session contains the metadata for a session file.
// A session file is a snapshot of an ongoing Spanner migration tool conversion session,
// and consists of an internal.Conv struct in JSON format.
type SessionParams struct {
	Driver    string `json:"driver"`
	FilePath  string `json:"filePath"`
	DBName    string `json:"dbName"`
	CreatedAt string `json:"createdAt"`
}

func IsOfflineSession(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(GetSessionState().IsOffline)
}

func GetSessions(w http.ResponseWriter, r *http.Request) {
	var sessions []SchemaConversionSession
	var err error
	if GetSessionState().IsOffline {
		sessions, err = getLocalSessions()
	} else {
		sessions, err = getRemoteSessions()
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessions)
}

func GetConv(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vid, ok := vars["versionId"]
	if !ok {
		http.Error(w, "VersionId not supplied", http.StatusBadRequest)
		return
	}

	var convm ConvWithMetadata
	var err error
	if GetSessionState().IsOffline {
		convm, err = getLocalConv(vid)
	} else {
		convm, err = getRemoteConv(vid)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func ResumeSession(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vid, ok := vars["versionId"]
	if !ok {
		http.Error(w, "VersionId not supplied", http.StatusBadRequest)
		return
	}

	var convm ConvWithMetadata
	var err error
	if GetSessionState().IsOffline {
		convm, err = getLocalConv(vid)
	} else {
		convm, err = getRemoteConv(vid)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}

	sessionState := GetSessionState()
	sessionState.Conv = &convm.Conv
	sessionState.Driver = convm.DatabaseType
	sessionState.DbName = convm.DatabaseName
	sessionState.SourceDBConnDetails = SourceDBConnDetails{
		ConnectionType: helpers.SESSION_FILE_MODE,
	}
	sessionState.Conv.UsedNames = internal.ComputeUsedNames(sessionState.Conv)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func SaveRemoteSession(w http.ResponseWriter, r *http.Request) {
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
	spannerClient, err := spanner.NewClient(ctx, getMetadataDbUri())
	if err != nil {
		http.Error(w, fmt.Sprintf("Spanner Client error : %v", err), http.StatusInternalServerError)
		return
	}
	defer spannerClient.Close()

	sessionState := GetSessionState()
	ssvc := NewSessionService(ctx, NewRemoteSessionStore(spannerClient))
	conv, err := json.Marshal(sessionState.Conv)
	if err != nil {
		http.Error(w, fmt.Sprintf("Conv object error : %v", err), http.StatusInternalServerError)
		return
	}

	// TODO: To compute few metadata fields if empty
	t := time.Now()

	switch sessionState.Driver {
	case constants.MYSQLDUMP:
		sm.DatabaseType = constants.MYSQL
	case constants.PGDUMP:
		sm.DatabaseType = constants.POSTGRES
	default:
		sm.DatabaseType = sessionState.Driver
	}

	sm.Dialect = helpers.GetDialectDisplayStringFromDialect(sessionState.Dialect)

	scs := SchemaConversionSession{
		VersionId:              uuid.New().String(),
		PreviousVersionId:      []string{},
		SchemaConversionObject: string(conv),
		CreateTimestamp:        t,
		SessionMetadata:        sm,
	}

	err = ssvc.SaveSession(scs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Spanner Transaction error : %v", err), http.StatusInternalServerError)
		return
	}

	sessionMetaData := GetSessionState().SessionMetadata

	sessionMetaData.DatabaseName = sm.DatabaseName
	sessionMetaData.DatabaseType = sm.DatabaseType
	sessionMetaData.SessionName = sm.SessionName
	sessionMetaData.Dialect = sm.Dialect

	GetSessionState().SessionMetadata = sessionMetaData

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode("Save successful, VersionId : " + scs.VersionId)
}

//Helpers

func getRemoteSessions() ([]SchemaConversionSession, error) {
	ctx := context.Background()
	spannerClient, err := spanner.NewClient(ctx, getMetadataDbUri())
	if err != nil {
		return nil, fmt.Errorf("Spanner Client error : %v", err)
	}
	defer spannerClient.Close()

	svc := NewSessionService(ctx, NewRemoteSessionStore(spannerClient))
	result, err := svc.GetSessionsMetadata()
	if err != nil {
		return nil, fmt.Errorf("Spanner Transaction error : %v", err)
	}
	return result, nil
}

func getLocalSessions() ([]SchemaConversionSession, error) {
	svc := NewSessionService(context.Background(), NewLocalSessionStore())
	result, err := svc.GetSessionsMetadata()
	if err != nil {
		return nil, fmt.Errorf("Local session store error : %v", err)
	}
	return result, nil
}

func getRemoteConv(versionId string) (ConvWithMetadata, error) {
	var convm ConvWithMetadata
	ctx := context.Background()
	spannerClient, err := spanner.NewClient(ctx, getMetadataDbUri())
	if err != nil {
		return convm, err
	}
	defer spannerClient.Close()

	ssvc := NewSessionService(ctx, NewRemoteSessionStore(spannerClient))
	convm, err = ssvc.GetConvWithMetadata(versionId)
	if err != nil {
		return convm, err
	}
	return convm, nil
}

func getLocalConv(versionId string) (ConvWithMetadata, error) {
	svc := NewSessionService(context.Background(), NewLocalSessionStore())
	result, err := svc.GetConvWithMetadata(versionId)
	if err != nil {
		return result, fmt.Errorf("Local session store error : %v", err)
	}
	return result, nil
}

func getMetadataDbUri() string {
	sessionState := GetSessionState()
	if sessionState.SpannerProjectId == "" || sessionState.SpannerInstanceID == "" {
		return ""
	}
	return helpers.GetSpannerUri(sessionState.SpannerProjectId, sessionState.SpannerInstanceID)
}
