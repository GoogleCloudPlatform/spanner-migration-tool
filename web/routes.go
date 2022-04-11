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
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/web/session"
	"github.com/gorilla/mux"
)

func getRoutes() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	staticFileDirectory := http.Dir("./frontend/")
	router.HandleFunc("/connect", databaseConnection).Methods("POST")
	router.HandleFunc("/convert/infoschema", convertSchemaSQL).Methods("GET")
	router.HandleFunc("/convert/dump", convertSchemaDump).Methods("POST")
	router.HandleFunc("/ddl", getDDL).Methods("GET")
	router.HandleFunc("/overview", getOverview).Methods("GET")
	router.HandleFunc("/conversion", getConversionRate).Methods("GET")
	router.HandleFunc("/typemap", getTypeMap).Methods("GET")
	router.HandleFunc("/report", getReportFile).Methods("GET")
	router.HandleFunc("/schema", getSchemaFile).Methods("GET")
	router.HandleFunc("/typemap/global", setTypeMapGlobal).Methods("POST")
	router.HandleFunc("/typemap/table", updateTableSchema).Methods("POST")
	router.HandleFunc("/setparent", setParentTable).Methods("GET")

	// TODO:(searce) take constraint names themselves which are guaranteed to be unique for Spanner.
	router.HandleFunc("/drop/fk", dropForeignKey).Methods("GET")

	// TODO:(searce) take constraint names themselves which are guaranteed to be unique for Spanner.
	router.HandleFunc("/drop/secondaryindex", dropSecondaryIndex).Methods("GET")

	router.HandleFunc("/rename/fks", renameForeignKeys).Methods("POST")
	router.HandleFunc("/rename/indexes", renameIndexes).Methods("POST")
	router.HandleFunc("/add/indexes", addIndexes).Methods("POST")

	// Session Management
	router.HandleFunc("/IsValidRemoteSessionStore", session.IsValidRemoteStore).Methods("GET")
	router.HandleFunc("/InitiateSession", session.InitiateSession).Methods("POST")
	router.HandleFunc("/GetSessions", session.GetSessions).Methods("GET")
	router.HandleFunc("/GetSession/{versionId}", session.GetConvSession).Methods("GET")
	router.HandleFunc("/SaveSession", session.SaveSession).Methods("POST")
	router.HandleFunc("/ResumeSession/{versionId}", session.ResumeRemoteSession).Methods("POST")
	//router.HandleFunc("/session/resume", session.ResumeLocalSession).Methods("POST")

	// Summary
	router.HandleFunc("/summary", getSummary).Methods("GET")

	// Application Configuration
	router.HandleFunc("/getConfig", getConfig).Methods("GET")
	router.HandleFunc("/setSpannerConfig", setSpannerConfig).Methods("POST")
	router.PathPrefix("/").Handler(http.FileServer(staticFileDirectory))

	return router
}
