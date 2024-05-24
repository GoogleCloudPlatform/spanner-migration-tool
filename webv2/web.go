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

// Package web defines web APIs to be used with Spanner migration tool frontend.
// Apart from schema conversion, this package involves API to update
// converted schema.
package webv2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
	storageaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/cmd"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/config"
	helpers "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/types"
	utilities "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
	"github.com/pkg/browser"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/handlers"

	index "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/index"
	primarykey "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/primarykey"

	go_ora "github.com/sijms/go-ora/v2"
)

// TODO:(searce):
// 1) Test cases for APIs
// 2) API for saving/updating table-level changes.
// 3) API for showing logs
// 4) Split all routing to an route.go file
// 5) API for downloading the schema file, ddl file and summary report file.
// 6) Update schema conv after setting global datatypes and return conv. (setTypeMap)
// 7) Add rateConversion() in schema conversion, ddl and report APIs.
// 8) Add an overview in summary report API

// databaseConnection creates connection with database
func databaseConnection(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var config types.DriverConfig
	err = json.Unmarshal(reqBody, &config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	var dataSourceName string
	switch config.Driver {
	case constants.POSTGRES:
		dataSourceName = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", config.Host, config.Port, config.User, config.Password, config.Database)
	case constants.MYSQL:
		dataSourceName = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
	case constants.SQLSERVER:
		dataSourceName = fmt.Sprintf(`sqlserver://%s:%s@%s:%s?database=%s`, config.User, config.Password, config.Host, config.Port, config.Database)
	case constants.ORACLE:
		portNumber, _ := strconv.Atoi(config.Port)
		dataSourceName = go_ora.BuildUrl(config.Host, portNumber, config.Database, config.User, config.Password, nil)
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", config.Driver), http.StatusBadRequest)
		return
	}
	sourceDB, err := sql.Open(config.Driver, dataSourceName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Database connection error, check connection properties, ERROR: %v", err), http.StatusInternalServerError)
		return
	}
	// Open doesn't open a connection. Validate database connection.
	err = sourceDB.Ping()
	if err != nil {
		http.Error(w, fmt.Sprintf("Database connection error, check connection properties, ERROR: %v", err), http.StatusInternalServerError)
		return
	}

	sessionState := session.GetSessionState()
	sessionState.SourceDB = sourceDB
	sessionState.DbName = config.Database
	// schema and user is same in oracle.
	if config.Driver == constants.ORACLE {
		sessionState.DbName = config.User
	}
	sessionState.Driver = config.Driver
	sessionState.SessionFile = ""
	sessionState.Dialect = config.Dialect
	sessionState.IsSharded = config.IsSharded
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Host:           config.Host,
		Port:           config.Port,
		User:           config.User,
		Password:       config.Password,
		ConnectionType: helpers.DIRECT_CONNECT_MODE,
	}
	w.WriteHeader(http.StatusOK)
}

func setSourceDBDetailsForDump(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var dc types.DumpConfig
	err = json.Unmarshal(reqBody, &dc)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	dc.FilePath = "upload-file/" + dc.FilePath
	_, err = os.Open(dc.FilePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open dump file : %v, no such file or directory", dc.FilePath), http.StatusNotFound)
		return
	}
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Path:           dc.FilePath,
		ConnectionType: helpers.DUMP_MODE,
	}
	w.WriteHeader(http.StatusOK)
}

// getSourceProfileConfig returns the configured source profile by the user
func getSourceProfileConfig(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sourceProfileConfig := sessionState.SourceProfileConfig
	if sourceProfileConfig.ConfigType == "dataflow" {
		for _, dataShard := range sourceProfileConfig.ShardConfigurationDataflow.DataShards {
			bucket, rootPath, err := conversion.GetBucketFromDatastreamProfile(sessionState.GCPProjectID, sessionState.Region, dataShard.DstConnectionProfile.Name)
			if err != nil {
				http.Error(w, fmt.Sprintf("error while getting target bucket: %v", err), http.StatusInternalServerError)
				return
			}
			dataShard.TmpDir = "gs://" + bucket + rootPath
		}
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sourceProfileConfig)
}

func setDatastreamDetailsForShardedMigrations(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var datastreamConfig profiles.DatastreamConfig
	err = json.Unmarshal(reqBody, &datastreamConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	sessionState.SourceProfileConfig.ShardConfigurationDataflow.DatastreamConfig = datastreamConfig
	w.WriteHeader(http.StatusOK)
}

func setGcsDetailsForShardedMigrations(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var gcsConfig profiles.GcsConfig
	err = json.Unmarshal(reqBody, &gcsConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	sessionState.SourceProfileConfig.ShardConfigurationDataflow.GcsConfig = gcsConfig
	w.WriteHeader(http.StatusOK)
}

func setDataflowDetailsForShardedMigrations(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var dataflowConfig profiles.DataflowConfig
	err = json.Unmarshal(reqBody, &dataflowConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	if dataflowConfig.Location == "" {
		dataflowConfig.Location = sessionState.Region
	}
	sessionState.SourceProfileConfig.ShardConfigurationDataflow.DataflowConfig = dataflowConfig
	w.WriteHeader(http.StatusOK)
}

func setShardsSourceDBDetailsForDataflow(w http.ResponseWriter, r *http.Request) {
	//Take the received object and store it into session state.
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var srcConfig types.ShardedDataflowConfig
	err = json.Unmarshal(reqBody, &srcConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	sessionState.SourceProfileConfig.ConfigType = srcConfig.MigrationProfile.ConfigType
	sessionState.SourceProfileConfig.ShardConfigurationDataflow.DataShards = srcConfig.MigrationProfile.ShardConfigurationDataflow.DataShards
	sessionState.SourceProfileConfig.ShardConfigurationDataflow.SchemaSource = srcConfig.MigrationProfile.ShardConfigurationDataflow.SchemaSource

	if sessionState.SourceProfileConfig.ShardConfigurationDataflow.DataflowConfig.Location == "" {
		// Create dataflow config with defaults, it gets overridden if DataflowConfig is specified using the form.
		sessionState.SourceProfileConfig.ShardConfigurationDataflow.DataflowConfig = profiles.DataflowConfig{
			Location:            sessionState.Region,
			Network:             "",
			Subnetwork:          "",
			MaxWorkers:          "",
			NumWorkers:          "",
			ServiceAccountEmail: "",
			VpcHostProjectId:    sessionState.GCPProjectID,
		}
	}
	w.WriteHeader(http.StatusOK)
}

func setShardsSourceDBDetailsForBulk(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var shardConfigs types.DriverConfigs
	err = json.Unmarshal(reqBody, &shardConfigs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	var connDetailsList []profiles.DirectConnectionConfig
	for i, config := range shardConfigs.DbConfigs {
		dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
		sourceDB, err := sql.Open(config.Driver, dataSourceName)
		if err != nil {
			http.Error(w, "Database connection error, check connection properties.", http.StatusInternalServerError)
			return
		}
		// Open doesn't open a connection. Validate database connection.
		err = sourceDB.Ping()
		if err != nil {
			http.Error(w, "Database connection error, check connection properties.", http.StatusInternalServerError)
			return
		}
		sessionState.DbName = config.Database
		sessionState.SessionFile = ""
		connDetail := profiles.DirectConnectionConfig{
			Host:        config.Host,
			Port:        config.Port,
			User:        config.User,
			Password:    config.Password,
			DbName:      config.Database,
			DataShardId: config.DataShardId,
		}
		connDetailsList = append(connDetailsList, connDetail)
		//set the first shard as the schema shard when restoring from a session file
		if shardConfigs.IsRestoredSession == constants.SESSION_FILE {
			if i == 0 {
				sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
					Host:           config.Host,
					Port:           config.Port,
					User:           config.User,
					Password:       config.Password,
					ConnectionType: helpers.DIRECT_CONNECT_MODE,
				}
			}
		}
	}
	sessionState.ShardedDbConnDetails = connDetailsList
	w.WriteHeader(http.StatusOK)
}

func setSourceDBDetailsForDirectConnect(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var config types.DriverConfig
	err = json.Unmarshal(reqBody, &config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	var dataSourceName string
	switch config.Driver {
	case constants.POSTGRES:
		dataSourceName = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", config.Host, config.Port, config.User, config.Password, config.Database)
	case constants.MYSQL:
		dataSourceName = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
	case constants.SQLSERVER:
		dataSourceName = fmt.Sprintf(`sqlserver://%s:%s@%s:%s?database=%s`, config.User, config.Password, config.Host, config.Port, config.Database)
	case constants.ORACLE:
		portNumber, _ := strconv.Atoi(config.Port)
		dataSourceName = go_ora.BuildUrl(config.Host, portNumber, config.Database, config.User, config.Password, nil)
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", config.Driver), http.StatusBadRequest)
		return
	}
	sourceDB, err := sql.Open(config.Driver, dataSourceName)
	if err != nil {
		http.Error(w, "Database connection error, check connection properties.", http.StatusInternalServerError)
		return
	}
	// Open doesn't open a connection. Validate database connection.
	err = sourceDB.Ping()
	if err != nil {
		http.Error(w, "Database connection error, check connection properties.", http.StatusInternalServerError)
		return
	}

	sessionState.DbName = config.Database
	// schema and user is same in oracle.
	if config.Driver == constants.ORACLE {
		sessionState.DbName = config.User
	}
	sessionState.SessionFile = ""
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Host:           config.Host,
		Port:           config.Port,
		User:           config.User,
		Password:       config.Password,
		ConnectionType: helpers.DIRECT_CONNECT_MODE,
	}
	w.WriteHeader(http.StatusOK)
}

// loadSession load seesion file to Spanner migration tool.
func loadSession(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()

	utilities.InitObjectId()

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var s session.SessionParams
	err = json.Unmarshal(reqBody, &s)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	conv := internal.MakeConv()
	metadata := session.SessionMetadata{}

	err = session.ReadSessionFileForSessionMetadata(&metadata, constants.UPLOAD_FILE_DIR+"/"+s.FilePath)
	if err != nil {
		switch err.(type) {
		case *fs.PathError:
			http.Error(w, fmt.Sprintf("Failed to open session file : %v, no such file or directory", s.FilePath), http.StatusNotFound)
		default:
			http.Error(w, fmt.Sprintf("Failed to parse session file : %v", err), http.StatusBadRequest)
		}
		return
	}

	dbType := metadata.DatabaseType
	switch dbType {
	case constants.PGDUMP:
		dbType = constants.POSTGRES
	case constants.MYSQLDUMP:
		dbType = constants.MYSQL
	}
	if dbType != s.Driver {
		http.Error(w, fmt.Sprintf("Not a valid %v session file", dbType), http.StatusBadRequest)
		return
	}

	err = conversion.ReadSessionFile(conv, constants.UPLOAD_FILE_DIR+"/"+s.FilePath)
	if err != nil {
		switch err.(type) {
		case *fs.PathError:
			http.Error(w, fmt.Sprintf("Failed to open session file : %v, no such file or directory", s.FilePath), http.StatusNotFound)
		default:
			http.Error(w, fmt.Sprintf("Failed to parse session file : %v", err), http.StatusBadRequest)
		}
		return
	}

	sessionMetadata := session.SessionMetadata{
		SessionName:  "NewSession",
		DatabaseType: s.Driver,
		DatabaseName: metadata.DatabaseName,
		Dialect:      conv.SpDialect,
	}

	if sessionMetadata.DatabaseName == "" {
		sessionMetadata.DatabaseName = strings.TrimRight(filepath.Base(s.FilePath), filepath.Ext(s.FilePath))
	}

	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()

	sessionState.Conv = conv

	primarykey.DetectHotspot()
	index.IndexSuggestion()

	sessionState.Conv.UsedNames = internal.ComputeUsedNames(sessionState.Conv)

	sessionState.SessionMetadata = sessionMetadata
	sessionState.Driver = s.Driver
	sessionState.SessionFile = constants.UPLOAD_FILE_DIR + s.FilePath
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Path:           constants.UPLOAD_FILE_DIR + "/" + s.FilePath,
		ConnectionType: helpers.SESSION_FILE_MODE,
	}
	sessionState.Dialect = conv.SpDialect

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionMetadata,
		Conv:            *conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func fetchLastLoadedSessionDetails(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

// getSchemaFile generates schema file and returns file path.
func getSchemaFile(w http.ResponseWriter, r *http.Request) {
	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	var err error
	now := time.Now()
	filePrefix, err := utilities.GetFilePrefix(now)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not get file prefix : %v", err), http.StatusInternalServerError)
	}
	schemaFileName := "frontend/" + filePrefix + "schema.txt"

	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.RLock()
	defer sessionState.Conv.ConvLock.RUnlock()
	conversion.WriteSchemaFile(sessionState.Conv, now, schemaFileName, ioHelper.Out, sessionState.Driver)
	schemaAbsPath, err := filepath.Abs(schemaFileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not create absolute path : %v", err), http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(schemaAbsPath))
}

// getIssueDescription maps IssueDB's Category to corresponding CategoryDescription(if present),
// or to the Brief if not present and pass the map to frontend to be used in assessment report UI
func getIssueDescription(w http.ResponseWriter, r *http.Request) {
	var issuesMap = make(map[string]string)
	for _, issue := range reports.IssueDB {
		if issue.CategoryDescription == "" {
			issuesMap[issue.Category] = issue.Brief
		} else {
			issuesMap[issue.Category] = issue.CategoryDescription
		}
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(issuesMap)
}

func getBackendHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// ToDo : To Remove once Rules Component updated
// addIndexes checks the new names for spanner name validity, ensures the new names are already not used by existing tables
// secondary indexes or foreign key constraints. If above checks passed then new indexes are added to the schema else appropriate
// error thrown.
func getSourceDestinationSummary(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.RLock()
	defer sessionState.Conv.ConvLock.RUnlock()
	// GetSourceDestinationSummary is called when the user enters prepare migration page
	// Getting and populating SpannerProjectId if it doesn't exist.
	if sessionState.SpannerProjectId == "" {
		sessionState.SpannerProjectId = sessionState.GCPProjectID
	}
	var sessionSummary types.SessionSummary
	databaseType, err := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while getting source database: %v", err), http.StatusBadRequest)
		return
	}
	sessionSummary.DatabaseType = databaseType
	sessionSummary.SourceDatabaseName = sessionState.DbName
	sessionSummary.ConnectionType = sessionState.SourceDBConnDetails.ConnectionType
	sessionSummary.SourceTableCount = len(sessionState.Conv.SrcSchema)
	sessionSummary.SpannerTableCount = len(sessionState.Conv.SpSchema)

	sourceIndexCount, spannerIndexCount := 0, 0
	for _, spannerSchema := range sessionState.Conv.SpSchema {
		spannerIndexCount = spannerIndexCount + len(spannerSchema.Indexes)
	}
	for _, sourceSchema := range sessionState.Conv.SrcSchema {
		sourceIndexCount = sourceIndexCount + len(sourceSchema.Indexes)
	}
	sessionSummary.SourceIndexCount = sourceIndexCount
	sessionSummary.SpannerIndexCount = spannerIndexCount
	ctx := context.Background()
	instanceClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		log.Println("instance admin client creation error")
		http.Error(w, fmt.Sprintf("Error while creating instance admin client : %v", err), http.StatusBadRequest)
		return
	}
	instanceInfo, err := instanceClient.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: fmt.Sprintf("projects/%s/instances/%s", sessionState.SpannerProjectId, sessionState.SpannerInstanceID)})
	if err != nil {
		log.Println("get instance error")
		http.Error(w, fmt.Sprintf("Error while getting instance information : %v", err), http.StatusBadRequest)
		return
	}
	instanceConfig, err := instanceClient.GetInstanceConfig(ctx, &instancepb.GetInstanceConfigRequest{Name: instanceInfo.Config})
	if err != nil {
		log.Println("get instance config error")
		http.Error(w, fmt.Sprintf("Error while getting instance config : %v", err), http.StatusBadRequest)
		return
	}
	for _, replica := range instanceConfig.Replicas {
		if replica.DefaultLeaderLocation {
			sessionSummary.Region = replica.Location
		}
	}
	sessionState.Region = sessionSummary.Region
	sessionSummary.NodeCount = int(instanceInfo.NodeCount)
	sessionSummary.ProcessingUnits = int(instanceInfo.ProcessingUnits)
	sessionSummary.Instance = sessionState.SpannerInstanceID
	sessionSummary.Dialect = helpers.GetDialectDisplayStringFromDialect(sessionState.Dialect)
	sessionSummary.IsSharded = sessionState.Conv.IsSharded
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionSummary)
}

func updateProgress(w http.ResponseWriter, r *http.Request) {

	var detail types.ProgressDetails
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.RLock()
	defer sessionState.Conv.ConvLock.RUnlock()
	if sessionState.Error != nil {
		detail.ErrorMessage = sessionState.Error.Error()
	} else {
		detail.ErrorMessage = ""
		detail.Progress, detail.ProgressStatus = sessionState.Conv.Audit.Progress.ReportProgress()
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(detail)
}

func migrate(w http.ResponseWriter, r *http.Request) {

	log.Println("request started", "method", r.Method, "path", r.URL.Path)
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("request's body Read Error")
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	details := types.MigrationDetails{}
	err = json.Unmarshal(reqBody, &details)
	if err != nil {
		log.Println("request's Body parse error")
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	sessionState := session.GetSessionState()
	sessionState.Error = nil
	ctx := context.Background()
	sessionState.Conv.Audit.Progress = internal.Progress{}
	sessionState.Conv.UI = true
	sourceProfile, targetProfile, ioHelper, dbName, err := getSourceAndTargetProfiles(sessionState, details)
	// TODO: Fix UX flow of migration project id
	migrationProjectId := sessionState.GCPProjectID
	if sessionState.SpannerProjectId == "" {
		sessionState.SpannerProjectId = sessionState.GCPProjectID
	}
	if err != nil {
		log.Println("can't get source and target profile")
		http.Error(w, fmt.Sprintf("Can't get source and target profiles: %v", err), http.StatusBadRequest)
		return
	}
	err = writeSessionFile(ctx, sessionState)
	if err != nil {
		log.Println("can't write session file")
		http.Error(w, fmt.Sprintf("Can't write session file to GCS: %v", err), http.StatusBadRequest)
		return
	}
	sessionState.Conv.ResetStats()
	sessionState.Conv.Audit.Progress = internal.Progress{}
	// Set env variable SKIP_METRICS_POPULATION to true in case of dev testing
	sessionState.Conv.Audit.SkipMetricsPopulation = os.Getenv("SKIP_METRICS_POPULATION") == "true"
	if details.MigrationMode == helpers.SCHEMA_ONLY {
		log.Println("Starting schema only migration")
		sessionState.Conv.Audit.MigrationType = migration.MigrationData_SCHEMA_ONLY.Enum()
		go cmd.MigrateDatabase(ctx, migrationProjectId, targetProfile, sourceProfile, dbName, &ioHelper, &cmd.SchemaCmd{}, sessionState.Conv, &sessionState.Error)
	} else if details.MigrationMode == helpers.DATA_ONLY {
		dataCmd := &cmd.DataCmd{
			SkipForeignKeys: details.SkipForeignKeys,
			WriteLimit:      cmd.DefaultWritersLimit,
		}
		log.Println("Starting data only migration")
		sessionState.Conv.Audit.MigrationType = migration.MigrationData_DATA_ONLY.Enum()
		go cmd.MigrateDatabase(ctx, migrationProjectId, targetProfile, sourceProfile, dbName, &ioHelper, dataCmd, sessionState.Conv, &sessionState.Error)
	} else {
		schemaAndDataCmd := &cmd.SchemaAndDataCmd{
			SkipForeignKeys: details.SkipForeignKeys,
			WriteLimit:      cmd.DefaultWritersLimit,
		}
		log.Println("Starting schema and data migration")
		sessionState.Conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
		go cmd.MigrateDatabase(ctx, migrationProjectId, targetProfile, sourceProfile, dbName, &ioHelper, schemaAndDataCmd, sessionState.Conv, &sessionState.Error)
	}
	w.WriteHeader(http.StatusOK)
	log.Println("migration completed", "method", r.Method, "path", r.URL.Path, "remoteaddr", r.RemoteAddr)
}

func getGeneratedResources(w http.ResponseWriter, r *http.Request) {
	var generatedResources types.GeneratedResources
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.RLock()
	defer sessionState.Conv.ConvLock.RUnlock()
	generatedResources.MigrationJobId = sessionState.Conv.Audit.MigrationRequestId
	generatedResources.DatabaseName = sessionState.SpannerDatabaseName
	generatedResources.DatabaseUrl = fmt.Sprintf("https://console.cloud.google.com/spanner/instances/%v/databases/%v/details/tables?project=%v", sessionState.SpannerInstanceID, sessionState.SpannerDatabaseName, sessionState.SpannerProjectId)
	generatedResources.BucketName = sessionState.Bucket + sessionState.RootPath
	generatedResources.BucketUrl = fmt.Sprintf("https://console.cloud.google.com/storage/browser/%v", sessionState.Bucket+sessionState.RootPath)
	generatedResources.ShardToShardResourcesMap = map[string][]types.ResourceDetails{}
	if sessionState.Conv.Audit.StreamingStats.DatastreamResources.DatastreamName != "" {
		generatedResources.DataStreamJobName = sessionState.Conv.Audit.StreamingStats.DatastreamResources.DatastreamName
		generatedResources.DataStreamJobUrl = fmt.Sprintf("https://console.cloud.google.com/datastream/streams/locations/%v/instances/%v?project=%v", sessionState.Region, sessionState.Conv.Audit.StreamingStats.DatastreamResources.DatastreamName, sessionState.GCPProjectID)
	}
	if sessionState.Conv.Audit.StreamingStats.DataflowResources.JobId != "" {
		generatedResources.DataflowJobName = sessionState.Conv.Audit.StreamingStats.DataflowResources.JobId
		generatedResources.DataflowJobUrl = fmt.Sprintf("https://console.cloud.google.com/dataflow/jobs/%v/%v?project=%v", sessionState.Conv.Audit.StreamingStats.DataflowResources.Region, sessionState.Conv.Audit.StreamingStats.DataflowResources.JobId, sessionState.GCPProjectID)
		generatedResources.DataflowGcloudCmd = sessionState.Conv.Audit.StreamingStats.DataflowResources.GcloudCmd
	}
	if sessionState.Conv.Audit.StreamingStats.PubsubResources.TopicId != "" {
		generatedResources.PubsubTopicName = sessionState.Conv.Audit.StreamingStats.PubsubResources.TopicId
		generatedResources.PubsubTopicUrl = fmt.Sprintf("https://console.cloud.google.com/cloudpubsub/topic/detail/%v?project=%v", sessionState.Conv.Audit.StreamingStats.PubsubResources.TopicId, sessionState.GCPProjectID)
	}
	if sessionState.Conv.Audit.StreamingStats.PubsubResources.SubscriptionId != "" {
		generatedResources.PubsubSubscriptionName = sessionState.Conv.Audit.StreamingStats.PubsubResources.SubscriptionId
		generatedResources.PubsubSubscriptionUrl = fmt.Sprintf("https://console.cloud.google.com/cloudpubsub/subscription/detail/%v?project=%v", sessionState.Conv.Audit.StreamingStats.PubsubResources.SubscriptionId, sessionState.GCPProjectID)
	}
	if sessionState.Conv.Audit.StreamingStats.MonitoringResources.DashboardName != "" {
		generatedResources.MonitoringDashboardName = sessionState.Conv.Audit.StreamingStats.MonitoringResources.DashboardName
		generatedResources.MonitoringDashboardUrl = fmt.Sprintf("https://console.cloud.google.com/monitoring/dashboards/builder/%v?project=%v", sessionState.Conv.Audit.StreamingStats.MonitoringResources.DashboardName, sessionState.GCPProjectID)
	}
	if sessionState.Conv.Audit.StreamingStats.AggMonitoringResources.DashboardName != "" {
		generatedResources.AggMonitoringDashboardName = sessionState.Conv.Audit.StreamingStats.AggMonitoringResources.DashboardName
		generatedResources.AggMonitoringDashboardUrl = fmt.Sprintf("https://console.cloud.google.com/monitoring/dashboards/builder/%v?project=%v", sessionState.Conv.Audit.StreamingStats.AggMonitoringResources.DashboardName, sessionState.GCPProjectID)
	}
	for shardId, shardResources := range sessionState.Conv.Audit.StreamingStats.ShardToShardResourcesMap {
		//Datastream
		url := fmt.Sprintf("https://console.cloud.google.com/datastream/streams/locations/%v/instances/%v?project=%v", sessionState.Region, shardResources.DatastreamResources.DatastreamName, sessionState.GCPProjectID)
		resourceDetails := types.ResourceDetails{ResourceType: constants.DATASTREAM_RESOURCE, ResourceName: shardResources.DatastreamResources.DatastreamName, ResourceUrl: url}
		generatedResources.ShardToShardResourcesMap[shardId] = append(generatedResources.ShardToShardResourcesMap[shardId], resourceDetails)
		//Dataflow
		dfId := shardResources.DataflowResources.JobId
		url = fmt.Sprintf("https://console.cloud.google.com/dataflow/jobs/%v/%v?project=%v", sessionState.Conv.Audit.StreamingStats.DataflowResources.Region, dfId, sessionState.GCPProjectID)
		resourceDetails = types.ResourceDetails{ResourceType: constants.DATAFLOW_RESOURCE, ResourceName: dfId, ResourceUrl: url, GcloudCmd: shardResources.DataflowResources.GcloudCmd}
		generatedResources.ShardToShardResourcesMap[shardId] = append(generatedResources.ShardToShardResourcesMap[shardId], resourceDetails)
		//monitoring
		url = fmt.Sprintf("https://console.cloud.google.com/monitoring/dashboards/builder/%v?project=%v", shardResources.MonitoringResources.DashboardName, sessionState.GCPProjectID)
		resourceDetails = types.ResourceDetails{ResourceType: constants.MONITORING_RESOURCE, ResourceName: shardResources.MonitoringResources.DashboardName, ResourceUrl: url}
		generatedResources.ShardToShardResourcesMap[shardId] = append(generatedResources.ShardToShardResourcesMap[shardId], resourceDetails)
		//gcs
		url = fmt.Sprintf("https://console.cloud.google.com/storage/browser/%v?project=%v", shardResources.GcsResources.BucketName, sessionState.GCPProjectID)
		resourceDetails = types.ResourceDetails{ResourceType: constants.GCS_RESOURCE, ResourceName: shardResources.GcsResources.BucketName, ResourceUrl: url}
		generatedResources.ShardToShardResourcesMap[shardId] = append(generatedResources.ShardToShardResourcesMap[shardId], resourceDetails)
		//pubsub
		topicUrl := fmt.Sprintf("https://console.cloud.google.com/cloudpubsub/topic/detail/%v?project=%v", shardResources.PubsubResources.TopicId, sessionState.GCPProjectID)
		topicResourceDetails := types.ResourceDetails{ResourceType: constants.PUBSUB_TOPIC_RESOURCE, ResourceName: shardResources.PubsubResources.TopicId, ResourceUrl: topicUrl}
		generatedResources.ShardToShardResourcesMap[shardId] = append(generatedResources.ShardToShardResourcesMap[shardId], topicResourceDetails)
		subscriptionUrl := fmt.Sprintf("https://console.cloud.google.com/cloudpubsub/subscription/detail/%v?project=%v", shardResources.PubsubResources.SubscriptionId, sessionState.GCPProjectID)
		subscriptionResourceDetails := types.ResourceDetails{ResourceType: constants.PUBSUB_SUB_RESOURCE, ResourceName: shardResources.PubsubResources.SubscriptionId, ResourceUrl: subscriptionUrl}
		generatedResources.ShardToShardResourcesMap[shardId] = append(generatedResources.ShardToShardResourcesMap[shardId], subscriptionResourceDetails)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(generatedResources)
}

func getSourceAndTargetProfiles(sessionState *session.SessionState, details types.MigrationDetails) (profiles.SourceProfile, profiles.TargetProfile, utils.IOStreams, string, error) {
	var (
		sourceProfileString string
		err                 error
	)
	sourceDBConnectionDetails := sessionState.SourceDBConnDetails
	if sourceDBConnectionDetails.ConnectionType == helpers.DUMP_MODE {
		sourceProfileString = fmt.Sprintf("file=%v,format=dump", sourceDBConnectionDetails.Path)
	} else if details.IsSharded {
		sourceProfileString, err = getSourceProfileStringForShardedMigrations(sessionState, details)
		if err != nil {
			return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", fmt.Errorf("error while creating config to initiate sharded migration:%v", err)
		}
	} else {
		sourceProfileString = fmt.Sprintf("host=%v,port=%v,user=%v,password=%v,dbName=%v",
			sourceDBConnectionDetails.Host, sourceDBConnectionDetails.Port, sourceDBConnectionDetails.User,
			sourceDBConnectionDetails.Password, sessionState.DbName)
	}

	sessionState.SpannerDatabaseName = details.TargetDetails.TargetDB
	targetProfileString := fmt.Sprintf("project=%v,instance=%v,dbName=%v,dialect=%v", sessionState.SpannerProjectId, sessionState.SpannerInstanceID, details.TargetDetails.TargetDB, sessionState.Dialect)
	if details.MigrationType == helpers.LOW_DOWNTIME_MIGRATION && !details.IsSharded {
		fileName := sessionState.Conv.Audit.MigrationRequestId + "-streaming.json"
		sessionState.Bucket, sessionState.RootPath, err = conversion.GetBucketFromDatastreamProfile(sessionState.GCPProjectID, sessionState.Region, details.TargetDetails.TargetConnectionProfileName)
		if err != nil {
			return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", fmt.Errorf("error while getting target bucket: %v", err)
		}
		err = createStreamingCfgFile(sessionState, details, fileName)
		if err != nil {
			return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", fmt.Errorf("error while creating streaming config file: %v", err)
		}
		sourceProfileString = sourceProfileString + fmt.Sprintf(",streamingCfg=%v", fileName)
	} else {
		sessionState.Conv.Audit.MigrationRequestId, _ = utils.GenerateName("smt-job")
		sessionState.Conv.Audit.MigrationRequestId = strings.Replace(sessionState.Conv.Audit.MigrationRequestId, "_", "-", -1)
		sessionState.Bucket = strings.ToLower(sessionState.Conv.Audit.MigrationRequestId)
		sessionState.RootPath = "/"
	}
	source, err := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if err != nil {
		return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", fmt.Errorf("error while getting source database: %v", err)
	}
	sourceProfile, targetProfile, ioHelper, dbName, err := cmd.PrepareMigrationPrerequisites(sourceProfileString, targetProfileString, source)
	if err != nil && sourceDBConnectionDetails.ConnectionType != helpers.SESSION_FILE_MODE {
		return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", fmt.Errorf("error while preparing prerequisites for migration: %v", err)
	}
	sourceProfile.Driver = sessionState.Driver
	if details.MigrationType == helpers.LOW_DOWNTIME_MIGRATION {
		sourceProfile.Config.ConfigType = constants.DATAFLOW_MIGRATION
	}
	return sourceProfile, targetProfile, ioHelper, dbName, nil
}

func getSourceProfileStringForShardedMigrations(sessionState *session.SessionState, details types.MigrationDetails) (string, error) {
	fileName := sessionState.Conv.Audit.MigrationRequestId + "-sharding.cfg"
	if details.MigrationType != helpers.LOW_DOWNTIME_MIGRATION {
		err := createConfigFileForShardedBulkMigration(sessionState, details, fileName)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("config=%v", fileName), nil
	} else if details.MigrationType == helpers.LOW_DOWNTIME_MIGRATION {
		err := createConfigFileForShardedDataflowMigration(sessionState, details, fileName)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("config=%v", fileName), nil
	} else {
		return "", fmt.Errorf("this migration type is not implemented yet")
	}

}

func createConfigFileForShardedDataflowMigration(sessionState *session.SessionState, details types.MigrationDetails, fileName string) error {
	sourceProfileConfig := sessionState.SourceProfileConfig
	file, err := json.MarshalIndent(sourceProfileConfig, "", " ")
	if err != nil {
		return fmt.Errorf("error while marshalling json: %v", err)
	}
	err = ioutil.WriteFile(fileName, file, 0644)
	if err != nil {
		return fmt.Errorf("error while writing json to file: %v", err)
	}
	return nil
}

func createConfigFileForShardedBulkMigration(sessionState *session.SessionState, details types.MigrationDetails, fileName string) error {
	sourceProfileConfig := profiles.SourceProfileConfig{
		ConfigType: constants.BULK_MIGRATION,
		ShardConfigurationBulk: profiles.ShardConfigurationBulk{
			SchemaSource: profiles.DirectConnectionConfig{
				Host:     sessionState.SourceDBConnDetails.Host,
				User:     sessionState.SourceDBConnDetails.User,
				Password: sessionState.SourceDBConnDetails.Password,
				Port:     sessionState.SourceDBConnDetails.Port,
				DbName:   sessionState.DbName,
			},
			DataShards: sessionState.ShardedDbConnDetails,
		},
	}
	file, err := json.MarshalIndent(sourceProfileConfig, "", " ")
	if err != nil {
		return fmt.Errorf("error while marshalling json: %v", err)
	}

	err = ioutil.WriteFile(fileName, file, 0644)
	if err != nil {
		return fmt.Errorf("error while writing json to file: %v", err)
	}
	return nil
}

func writeSessionFile(ctx context.Context, sessionState *session.SessionState) error {
	sc, err := storageclient.NewStorageClientImpl(ctx)
	if err != nil {
		return err
	}
	sa := storageaccessor.StorageAccessorImpl{}
	err = sa.CreateGCSBucket(ctx, sc, storageaccessor.StorageBucketMetadata{
		BucketName:    sessionState.Bucket,
		ProjectID:     sessionState.GCPProjectID,
		Location:      sessionState.Region,
		Ttl:           0,
		MatchesPrefix: nil,
	})
	if err != nil {
		return fmt.Errorf("error while creating bucket: %v", err)
	}

	convJSON, err := json.MarshalIndent(sessionState.Conv, "", " ")
	if err != nil {
		return fmt.Errorf("can't encode session state to JSON: %v", err)
	}
	err = sa.WriteDataToGCS(ctx, sc, "gs://"+sessionState.Bucket+sessionState.RootPath, "session.json", string(convJSON))
	if err != nil {
		return fmt.Errorf("error while writing to GCS: %v", err)
	}
	return nil
}

func createStreamingCfgFile(sessionState *session.SessionState, details types.MigrationDetails, fileName string) error {
	targetDetails, datastreamConfig, dataflowConfig := details.TargetDetails, details.DatastreamConfig, details.DataflowConfig
	dfLocation := sessionState.Region
	if dataflowConfig.Location != "" {
		dfLocation = dataflowConfig.Location
	}
	data := streaming.StreamingCfg{
		DatastreamCfg: streaming.DatastreamCfg{
			StreamId:          "",
			StreamLocation:    sessionState.Region,
			StreamDisplayName: "",
			SourceConnectionConfig: streaming.SrcConnCfg{
				Name:     targetDetails.SourceConnectionProfileName,
				Location: sessionState.Region,
			},
			DestinationConnectionConfig: streaming.DstConnCfg{
				Name:     targetDetails.TargetConnectionProfileName,
				Location: sessionState.Region,
			},
			MaxConcurrentBackfillTasks: datastreamConfig.MaxConcurrentBackfillTasks,
			MaxConcurrentCdcTasks:      datastreamConfig.MaxConcurrentCdcTasks,
		},
		GcsCfg: streaming.GcsCfg{
			TtlInDays:    details.GcsConfig.TtlInDays,
			TtlInDaysSet: details.GcsConfig.TtlInDaysSet,
		},
		DataflowCfg: streaming.DataflowCfg{
			ProjectId:            dataflowConfig.ProjectId,
			JobName:              "",
			Location:             dfLocation,
			Network:              dataflowConfig.Network,
			Subnetwork:           dataflowConfig.Subnetwork,
			MaxWorkers:           dataflowConfig.MaxWorkers,
			NumWorkers:           dataflowConfig.NumWorkers,
			ServiceAccountEmail:  dataflowConfig.ServiceAccountEmail,
			VpcHostProjectId:     dataflowConfig.VpcHostProjectId,
			MachineType:          dataflowConfig.MachineType,
			AdditionalUserLabels: dataflowConfig.AdditionalUserLabels,
			KmsKeyName:           dataflowConfig.KmsKeyName,
			GcsTemplatePath:      dataflowConfig.GcsTemplatePath,
		},
		TmpDir: "gs://" + sessionState.Bucket + sessionState.RootPath,
	}

	databaseType, _ := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if databaseType == constants.POSTGRES {
		data.DatastreamCfg.Properties = fmt.Sprintf("replicationSlot=%v,publication=%v", targetDetails.ReplicationSlot, targetDetails.Publication)
	}
	file, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return fmt.Errorf("error while marshalling json: %v", err)
	}

	err = ioutil.WriteFile(fileName, file, 0644)
	if err != nil {
		return fmt.Errorf("error while writing json to file: %v", err)
	}
	return nil
}

func uploadFile(w http.ResponseWriter, r *http.Request) {

	r.ParseMultipartForm(10 << 20)
	// FormFile returns the first file for the given key `myFile`
	// it also returns the FileHeader so we can get the Filename,
	// the Header and the size of the file
	file, handlers, err := r.FormFile("myFile")
	if err != nil {
		http.Error(w, fmt.Sprintf("error retrieving the file"), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Remove the existing files
	err = os.RemoveAll("upload-file/")
	if err != nil {
		http.Error(w, fmt.Sprintf("error removing existing files"), http.StatusBadRequest)
		return
	}

	err = os.MkdirAll("upload-file", os.ModePerm)
	if err != nil {
		http.Error(w, fmt.Sprintf("error while creating directory"), http.StatusBadRequest)
		return
	}

	f, err := os.Create("upload-file/" + handlers.Filename)
	if err != nil {
		http.Error(w, fmt.Sprintf("not able to create file"), http.StatusBadRequest)
		return
	}

	// read all of the contents of our uploaded file into a byte array
	fileBytes, err := ioutil.ReadAll(file)
	if err != nil {
		http.Error(w, fmt.Sprintf("error reading the file"), http.StatusBadRequest)
		return
	}
	if _, err := f.Write(fileBytes); err != nil {
		http.Error(w, fmt.Sprintf("error writing the file"), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode("file uploaded successfully")
}

// rollback is used to get previous state of conversion in case
// some unexpected error occurs during update operations.
func rollback(err error) error {
	sessionState := session.GetSessionState()

	if sessionState.SessionFile == "" {
		return fmt.Errorf("encountered error %w. rollback failed because we don't have a session file", err)
	}
	sessionState.Conv = internal.MakeConv()
	sessionState.Conv.SpDialect = constants.DIALECT_GOOGLESQL
	err2 := conversion.ReadSessionFile(sessionState.Conv, sessionState.SessionFile)
	if err2 != nil {
		return fmt.Errorf("encountered error %w. rollback failed: %v", err, err2)
	}
	return err
}

func init() {
	sessionState := session.GetSessionState()
	utilities.InitObjectId()
	sessionState.Conv = internal.MakeConv()
	config := config.TryInitializeSpannerConfig()
	session.SetSessionStorageConnectionState(config.GCPProjectID, config.SpannerProjectID, config.SpannerInstanceID)
}

// App connects to the web app v2.
func App(logLevel string, open bool, port int) error {
	err := logger.InitializeLogger(logLevel)
	if err != nil {
		return fmt.Errorf("error initialising webapp, did you specify a valid log-level? [DEBUG, INFO]")
	}
	addr := fmt.Sprintf(":%s", strconv.Itoa(port))
	router := getRoutes()
	fmt.Println("Starting Spanner migration tool UI at:", fmt.Sprintf("http://localhost%s", addr))
	fmt.Println("Reverse Replication feature in preview: Please refer to https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/reverse_replication/README.md for detailed instructions.")
	if open {
		browser.OpenURL(fmt.Sprintf("http://localhost%s", addr))
	}
	return http.ListenAndServe(addr, handlers.CORS(handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}), handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS"}), handlers.AllowedOrigins([]string{"*"}))(router))
}
