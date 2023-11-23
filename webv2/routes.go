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
	"io/fs"
	"net/http"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/config"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/primarykey"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/profile"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/summary"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/table"

	"github.com/gorilla/mux"
)

func getRoutes() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	frontendRoot, _ := fs.Sub(FrontendDir, "ui/dist/ui")
	frontendStatic := http.FileServer(http.FS(frontendRoot))
	router.HandleFunc("/connect", databaseConnection).Methods("POST")
	router.HandleFunc("/convert/infoschema", convertSchemaSQL).Methods("GET")
	router.HandleFunc("/convert/dump", convertSchemaDump).Methods("POST")
	router.HandleFunc("/convert/session", loadSession).Methods("POST")
	router.HandleFunc("/ddl", getDDL).Methods("GET")
	router.HandleFunc("/conversion", getConversionRate).Methods("GET")
	router.HandleFunc("/typemap", getTypeMap).Methods("GET")
	router.HandleFunc("/report", getReportFile).Methods("GET")
	router.HandleFunc("/downloadStructuredReport", getDStructuredReport).Methods("GET")
	router.HandleFunc("/downloadTextReport", getDTextReport).Methods("GET")
	router.HandleFunc("/downloadDDL", getDSpannerDDL).Methods("GET")
	router.HandleFunc("/schema", getSchemaFile).Methods("GET")
	router.HandleFunc("/applyrule", applyRule).Methods("POST")
	router.HandleFunc("/dropRule", dropRule).Methods("POST")
	router.HandleFunc("/typemap/table", table.UpdateTableSchema).Methods("POST")
	router.HandleFunc("/typemap/reviewTableSchema", table.ReviewTableSchema).Methods("POST")
	router.HandleFunc("/typemap/GetStandardTypeToPGSQLTypemap", getStandardTypeToPGSQLTypemap).Methods("GET")
	router.HandleFunc("/typemap/GetPGSQLToStandardTypeTypemap", getPGSQLToStandardTypeTypemap).Methods("GET")
	router.HandleFunc("/spannerDefaultTypeMap", spannerDefaultTypeMap).Methods("GET")

	router.HandleFunc("/setparent", setParentTable).Methods("GET")
	router.HandleFunc("/removeParent", removeParentTable).Methods("POST")

	// TODO:(searce) take constraint names themselves which are guaranteed to be unique for Spanner.
	router.HandleFunc("/drop/secondaryindex", dropSecondaryIndex).Methods("POST")
	router.HandleFunc("/restore/secondaryIndex", restoreSecondaryIndex).Methods("POST")

	router.HandleFunc("/restore/table", restoreTable).Methods("POST")
	router.HandleFunc("/restore/tables", restoreTables).Methods("POST")
	router.HandleFunc("/drop/table", dropTable).Methods("POST")
	router.HandleFunc("/drop/tables", dropTables).Methods("POST")

	router.HandleFunc("/update/fks", updateForeignKeys).Methods("POST")
	router.HandleFunc("/update/indexes", updateIndexes).Methods("POST")

	// Session Management
	router.HandleFunc("/IsOffline", session.IsOfflineSession).Methods("GET")
	router.HandleFunc("/GetSessions", session.GetSessions).Methods("GET")
	router.HandleFunc("/GetSession/{versionId}", session.GetConv).Methods("GET")
	router.HandleFunc("/SaveRemoteSession", session.SaveRemoteSession).Methods("POST")
	router.HandleFunc("/ResumeSession/{versionId}", session.ResumeSession).Methods("POST")

	// primarykey
	router.HandleFunc("/primaryKey", primarykey.PrimaryKey).Methods("POST")

	router.HandleFunc("/AddColumn", table.AddNewColumn).Methods("POST")

	// Summary
	router.HandleFunc("/summary", summary.GetSummary).Methods("GET")

	// Issue Description
	router.HandleFunc("/issueDescription", getIssueDescription).Methods("GET")

	// Application Configuration
	router.HandleFunc("/GetConfig", config.GetConfig).Methods("GET")
	router.HandleFunc("/SetSpannerConfig", config.SetSpannerConfig).Methods("POST")

	// Run migration
	router.HandleFunc("/Migrate", migrate).Methods("POST")

	router.HandleFunc("/GetSourceDestinationSummary", getSourceDestinationSummary).Methods("GET")
	router.HandleFunc("/GetProgress", updateProgress).Methods("GET")
	router.HandleFunc("/GetLatestSessionDetails", fetchLastLoadedSessionDetails).Methods("GET")
	router.HandleFunc("/GetGeneratedResources", getGeneratedResources).Methods("GET")

	// Connection profiles
	router.HandleFunc("/GetConnectionProfiles", profile.ListConnectionProfiles).Methods("GET")
	router.HandleFunc("/GetStaticIps", profile.GetStaticIps).Methods("GET")
	router.HandleFunc("/CreateConnectionProfile", profile.CreateConnectionProfile).Methods("POST")

	// Clean up datastream and data flow jobs
	router.HandleFunc("/CleanUpStreamingJobs", profile.CleanUpStreamingJobs).Methods("POST")

	router.HandleFunc("/SetSourceDBDetailsForDump", setSourceDBDetailsForDump).Methods("POST")
	router.HandleFunc("/SetSourceDBDetailsForDirectConnect", setSourceDBDetailsForDirectConnect).Methods("POST")
	router.HandleFunc("/SetShardsSourceDBDetailsForBulk", setShardsSourceDBDetailsForBulk).Methods("POST")
	router.HandleFunc("/SetShardsSourceDBDetailsForDataflow", setShardsSourceDBDetailsForDataflow).Methods("POST")
	router.HandleFunc("/SetDatastreamDetailsForShardedMigrations", setDatastreamDetailsForShardedMigrations).Methods("POST")
	router.HandleFunc("/SetDataflowDetailsForShardedMigrations", setDataflowDetailsForShardedMigrations).Methods("POST")
	router.HandleFunc("/GetSourceProfileConfig", getSourceProfileConfig).Methods("GET")
	router.HandleFunc("/uploadFile", uploadFile).Methods("POST")

	router.HandleFunc("/GetTableWithErrors", getTableWithErrors).Methods("GET")
	router.HandleFunc("/ping", getBackendHealth).Methods("GET")

	router.PathPrefix("/").Handler(frontendStatic)
	return router
}
