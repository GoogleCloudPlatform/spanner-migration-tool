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
)

// Metadata of session file, which contains app.conv in
// JSON format.
type Session struct {
	Driver    string    `json:"driver"`
	FilePath  string    `json:"path"`
	FileName  string    `json:"fileName"`
	CreatedAt time.Time `json:"createdAt"`
}

func getSession(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	dbName, err := conversion.GetDatabaseName(app.driver, now)
	if err != nil {
		fmt.Printf("\nCan't get database name: %v\n", err)
		panic(fmt.Errorf("can't get database name"))
	}
	sessionFile := ".session.json"
	filePath := "frontend/"
	out := os.Stdout
	f, err := os.Create(filePath + dbName + sessionFile)
	if err != nil {
		fmt.Fprintf(out, "Can't create session file %s: %v\n", dbName+sessionFile, err)
		return
	}
	// Session file will basically contain 'conv' struct in JSON format.
	// It contains all the information for schema and data conversion state.
	convJSON, err := json.MarshalIndent(app.conv, "", " ")
	if err != nil {
		fmt.Fprintf(out, "Can't encode session state to JSON: %v\n", err)
		return
	}
	if _, err := f.Write(convJSON); err != nil {
		fmt.Fprintf(out, "Can't write out session file: %v\n", err)
		return
	}
	session := Session{Driver: app.driver, FilePath: filePath, FileName: dbName + sessionFile, CreatedAt: now}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(session)
}

func resumeSession(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var s Session
	err = json.Unmarshal(reqBody, &s)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	f, err := os.Open(s.FilePath + s.FileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to open the session file: %v", err), http.StatusNotFound)
		return
	}
	defer f.Close()
	sessionJSON, _ := ioutil.ReadAll(f)
	json.Unmarshal(sessionJSON, &app.conv)
	app.driver = s.Driver
	w.WriteHeader(http.StatusOK)
}
