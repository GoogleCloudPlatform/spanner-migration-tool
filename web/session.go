// Copyright 2020 Google LLC
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

package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

// session contains the metadata for a session file.
// A session file is a snapshot of an ongoing HarbourBridge conversion session,
// and consists of an internal.Conv struct in JSON format.
type session struct {
	Driver    string `json:"driver"`
	FilePath  string `json:"path"`
	FileName  string `json:"fileName"`
	DBName    string `json:"dbName"`
	CreatedAt string `json:"createdAt"`
}

func createSession(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	dbName := app.dbName
	var err error
	if dbName == "" {
		dbName, err = conversion.GetDatabaseName(app.driver, now)
		if err != nil {
			http.Error(w, fmt.Sprintf("Can not create database name : %v", err), http.StatusInternalServerError)
		}
	}
	sessionFile := ".session.json"
	filePath := "frontend/"
	out := os.Stdout
	fileName := filePath + dbName + sessionFile
	conversion.WriteSessionFile(app.conv, fileName, out)
	session := session{Driver: app.driver, FilePath: filePath, FileName: dbName + sessionFile, DBName: dbName, CreatedAt: now.Format(time.RFC1123)}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(session)
}

func resumeSession(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var s session
	err = json.Unmarshal(reqBody, &s)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	app.conv = internal.MakeConv()
	err = conversion.ReadSessionFile(app.conv, s.FilePath+s.FileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to open the session file: %v", err), http.StatusNotFound)
		return
	}
	app.driver = s.Driver
	app.dbName = s.DBName
	w.WriteHeader(http.StatusOK)
}
