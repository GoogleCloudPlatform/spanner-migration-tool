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

package webv2

import (
	"github.com/cloudspannerecosystem/harbourbridge/webv2/config"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/primarykey"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/summary"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/updateTableSchema"

	"github.com/gorilla/mux"
)

func getRoutes() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/connect", databaseConnection).Methods("POST")
	router.HandleFunc("/convert/infoschema", convertSchemaSQL).Methods("GET")
	router.HandleFunc("/convert/dump", convertSchemaDump).Methods("POST")
	router.HandleFunc("/convert/session", loadSession).Methods("POST")
	router.HandleFunc("/ddl", getDDL).Methods("GET")
	router.HandleFunc("/overview", getOverview).Methods("GET")
	router.HandleFunc("/conversion", getConversionRate).Methods("GET")
	router.HandleFunc("/typemap", getTypeMap).Methods("GET")
	router.HandleFunc("/report", getReportFile).Methods("GET")
	router.HandleFunc("/schema", getSchemaFile).Methods("GET")
	router.HandleFunc("/typemap/global", setTypeMapGlobal).Methods("POST")
	router.HandleFunc("/typemap/table", updateTableSchema.UpdateTableSchema).Methods("POST")
	router.HandleFunc("/typemap/reviewtableschema", updateTableSchema.ReviewTableSchema).Methods("POST")

	router.HandleFunc("/setparent", setParentTable).Methods("GET")

	// TODO:(searce) take constraint names themselves which are guaranteed to be unique for Spanner.
	router.HandleFunc("/drop/fk", dropForeignKey).Methods("POST")

	// TODO:(searce) take constraint names themselves which are guaranteed to be unique for Spanner.
	router.HandleFunc("/drop/secondaryindex", dropSecondaryIndex).Methods("POST")

	router.HandleFunc("/restore/table", restoreTable).Methods("POST")
	router.HandleFunc("/drop/table", dropTable).Methods("POST")

	router.HandleFunc("/rename/fks", renameForeignKeys).Methods("POST")
	router.HandleFunc("/rename/indexes", renameIndexes).Methods("POST")
	router.HandleFunc("/add/indexes", addIndexes).Methods("POST")
	router.HandleFunc("/update/indexes", updateIndexes).Methods("POST")

	// Session Management
	router.HandleFunc("/IsOffline", session.IsOfflineSession).Methods("GET")
	router.HandleFunc("/InitiateSession", session.InitiateSession).Methods("POST")
	router.HandleFunc("/GetSessions", session.GetSessions).Methods("GET")
	router.HandleFunc("/GetSession/{versionId}", session.GetConv).Methods("GET")
	router.HandleFunc("/SaveRemoteSession", session.SaveRemoteSession).Methods("POST")
	router.HandleFunc("/ResumeSession/{versionId}", session.ResumeSession).Methods("POST")

	// primarykey
	router.HandleFunc("/primaryKey", primarykey.PrimaryKey).Methods("POST")

	// Summary
	router.HandleFunc("/summary", summary.GetSummary).Methods("GET")

	// Application Configuration
	router.HandleFunc("/GetConfig", config.GetConfig).Methods("GET")
	router.HandleFunc("/SetSpannerConfig", config.SetSpannerConfig).Methods("POST")

	// Run migration
	router.HandleFunc("/Migrate", migrate).Methods("POST")

	router.HandleFunc("/GetSourceDestinationSummary", getSourceDestinationSummary).Methods("GET")

	return router
}
