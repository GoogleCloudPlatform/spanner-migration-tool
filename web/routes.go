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

	"github.com/gorilla/mux"
)

func getRoutes() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	staticFileDirectory := http.Dir("./frontend/")
	staticFileHandler := http.StripPrefix("/frontend/", http.FileServer(staticFileDirectory))
	router.PathPrefix("/frontend/").Handler(staticFileHandler).Methods("GET")
	router.HandleFunc("/", homeLink)
	router.HandleFunc("/connect", databaseConnection).Methods("POST")
	router.HandleFunc("/convert/infoschema", convertSchemaSQL).Methods("GET")
	router.HandleFunc("/convert/dump", convertSchemaDump).Methods("POST")
	router.HandleFunc("/ddl", getDDL).Methods("GET")
	router.HandleFunc("/session", getSession).Methods("GET")
	router.HandleFunc("/session/resume", resumeSession).Methods("POST")
	router.HandleFunc("/summary", getSummary).Methods("GET")
	router.HandleFunc("/overview", getOverview).Methods("GET")
	router.HandleFunc("/conversion", getConversionRate).Methods("GET")
	router.HandleFunc("/typemap", getTypeMap).Methods("GET")
	router.HandleFunc("/filepaths", getSchemaAndReportFile).Methods("GET")
	router.HandleFunc("/typemap/global", setTypeMapGlobal).Methods("POST")
	router.HandleFunc("/typemap/table", setTypeMapTableLevel).Methods("POST")
	router.HandleFunc("/checkinterleave/table", checkForInterleavedTables).Methods("GET")
	return router
}
