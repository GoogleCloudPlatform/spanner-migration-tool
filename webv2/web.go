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

// Package web defines web APIs to be used with harbourbridge frontend.
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
	"sort"
	"strconv"
	"strings"
	"time"

	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/cmd"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/internal/reports"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/sources/mysql"
	"github.com/cloudspannerecosystem/harbourbridge/sources/oracle"
	"github.com/cloudspannerecosystem/harbourbridge/sources/postgres"
	"github.com/cloudspannerecosystem/harbourbridge/sources/sqlserver"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/config"
	helpers "github.com/cloudspannerecosystem/harbourbridge/webv2/helpers"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/profile"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/table"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
	"github.com/google/uuid"
	"github.com/pkg/browser"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/handlers"

	index "github.com/cloudspannerecosystem/harbourbridge/webv2/index"
	primarykey "github.com/cloudspannerecosystem/harbourbridge/webv2/primarykey"

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
var mysqlTypeMap = make(map[string][]typeIssue)
var postgresTypeMap = make(map[string][]typeIssue)
var sqlserverTypeMap = make(map[string][]typeIssue)
var oracleTypeMap = make(map[string][]typeIssue)

// TODO:(searce) organize this file according to go style guidelines: generally
// have public constants and public type definitions first, then public
// functions, and finally helper functions (usually in order of importance).

// driverConfig contains the parameters needed to make a direct database connection. It is
// used to communicate via HTTP with the frontend.
type driverConfig struct {
	Driver    string `json:"Driver"`
	IsSharded bool   `json:"IsSharded"`
	Host      string `json:"Host"`
	Port      string `json:"Port"`
	Database  string `json:"Database"`
	User      string `json:"User"`
	Password  string `json:"Password"`
	Dialect   string `json:"Dialect"`
}

type driverConfigs struct {
	DbConfigs         []driverConfig `json:"DbConfigs"`
	IsRestoredSession string         `json:"IsRestoredSession"`
}

type shardedDataflowConfig struct {
	MigrationProfile profiles.SourceProfileConfig
}

type sessionSummary struct {
	DatabaseType       string
	ConnectionDetail   string
	SourceTableCount   int
	SpannerTableCount  int
	SourceIndexCount   int
	SpannerIndexCount  int
	ConnectionType     string
	SourceDatabaseName string
	Region             string
	NodeCount          int
	ProcessingUnits    int
	Instance           string
	Dialect            string
	IsSharded          bool
}

type progressDetails struct {
	Progress       int
	ErrorMessage   string
	ProgressStatus int
}

type migrationDetails struct {
	TargetDetails  targetDetails  `json:"TargetDetails"`
	DataflowConfig dataflowConfig `json:"DataflowConfig"`
	MigrationMode  string         `json:MigrationMode`
	MigrationType  string         `json:MigrationType`
	IsSharded      bool         `json:"IsSharded"`
}

type dataflowConfig struct {
	Network       string `json:Network`
	Subnetwork    string `json:Subnetwork`
	HostProjectId string `json:HostProjectId`
}

type targetDetails struct {
	TargetDB                    string `json:"TargetDB"`
	SourceConnectionProfileName string `json:"SourceConnProfile"`
	TargetConnectionProfileName string `json:"TargetConnProfile"`
	ReplicationSlot             string `json:"ReplicationSlot"`
	Publication                 string `json:"Publication"`
}
type StreamingCfg struct {
	DatastreamCfg DatastreamCfg `json:"datastreamCfg"`
	DataflowCfg   DataflowCfg   `json:"dataflowCfg"`
	TmpDir        string        `json:"tmpDir"`
}
type DataflowCfg struct {
	JobName       string `json:"JobName"`
	Location      string `json:"Location"`
	Network       string `json:"Network"`
	Subnetwork    string `json:"Subnetwork"`
	HostProjectId string `json:"HostProjectId"`
}
type ConnectionConfig struct {
	Name     string `json:"name"`
	Location string `json:"location"`
}
type DatastreamCfg struct {
	StreamId               string           `json:"streamId"`
	StreamLocation         string           `json:"streamLocation"`
	StreamDisplayName      string           `json:"streamDisplayName"`
	SourceConnectionConfig ConnectionConfig `json:"sourceConnectionConfig"`
	TargetConnectionConfig ConnectionConfig `json:"destinationConnectionConfig"`
	Properties             string           `json:properties`
}

type ColMaxLength struct {
	SpDataType     string `json:"spDataType"`
	SpColMaxLength string `json:"spColMaxLength"`
}

// databaseConnection creates connection with database
func databaseConnection(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var config driverConfig
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
		http.Error(w, fmt.Sprintf("Database connection error, check connection properties."), http.StatusInternalServerError)
		return
	}
	// Open doesn't open a connection. Validate database connection.
	err = sourceDB.Ping()
	if err != nil {
		http.Error(w, fmt.Sprintf("Database connection error, check connection properties."), http.StatusInternalServerError)
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

// convertSchemaSQL converts source database to Spanner when using
// with postgres and mysql driver.
func convertSchemaSQL(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	if sessionState.SourceDB == nil || sessionState.DbName == "" || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Database is not configured or Database connection is lost. Please set configuration and connect to database."), http.StatusNotFound)
		return
	}
	conv := internal.MakeConv()

	conv.SpDialect = sessionState.Dialect
	var err error
	switch sessionState.Driver {
	case constants.MYSQL:
		err = common.ProcessSchema(conv, mysql.InfoSchemaImpl{DbName: sessionState.DbName, Db: sessionState.SourceDB}, common.DefaultWorkers)
	case constants.POSTGRES:
		temp := false
		err = common.ProcessSchema(conv, postgres.InfoSchemaImpl{Db: sessionState.SourceDB, IsSchemaUnique: &temp}, common.DefaultWorkers)
	case constants.SQLSERVER:
		err = common.ProcessSchema(conv, sqlserver.InfoSchemaImpl{DbName: sessionState.DbName, Db: sessionState.SourceDB}, common.DefaultWorkers)
	case constants.ORACLE:
		err = common.ProcessSchema(conv, oracle.InfoSchemaImpl{DbName: strings.ToUpper(sessionState.DbName), Db: sessionState.SourceDB}, common.DefaultWorkers)
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", sessionState.Driver), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Schema Conversion Error : %v", err), http.StatusNotFound)
		return
	}

	sessionState.Conv = conv

	primarykey.DetectHotspot()
	index.IndexSuggestion()

	sessionMetadata := session.SessionMetadata{
		SessionName:  "NewSession",
		DatabaseType: sessionState.Driver,
		DatabaseName: sessionState.DbName,
		Dialect:      sessionState.Dialect,
	}

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionMetadata,
		Conv:            *conv,
	}
	sessionState.Conv = conv
	sessionState.SessionMetadata = sessionMetadata
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

// dumpConfig contains the parameters needed to run the tool using dump approach. It is
// used to communicate via HTTP with the frontend.
type dumpConfig struct {
	Driver   string `json:"Driver"`
	FilePath string `json:"Path"`
}

type spannerDetails struct {
	Dialect string `json:"Dialect"`
}

type convertFromDumpRequest struct {
	Config         dumpConfig     `json:"Config"`
	SpannerDetails spannerDetails `json:"SpannerDetails"`
}

func setSourceDBDetailsForDump(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var dc dumpConfig
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

func setShardsSourceDBDetailsForDataflow(w http.ResponseWriter, r *http.Request) {
	//Take the received object and store it into session state.
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var srcConfig shardedDataflowConfig
	err = json.Unmarshal(reqBody, &srcConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	sessionState.SourceProfileConfig = srcConfig.MigrationProfile
	w.WriteHeader(http.StatusOK)
}

func setShardsSourceDBDetailsForBulk(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var shardConfigs driverConfigs
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
			Host:     config.Host,
			Port:     config.Port,
			User:     config.User,
			Password: config.Password,
			DbName:   config.Database,
		}
		connDetailsList = append(connDetailsList, connDetail)
		//set the first shard as the schema shard when restoring from a session file
		if shardConfigs.IsRestoredSession == "sessionFile" {
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
	var config driverConfig
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

// convertSchemaDump converts schema from dump file to Spanner schema for
// mysqldump and pg_dump driver.
func convertSchemaDump(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var dc convertFromDumpRequest
	err = json.Unmarshal(reqBody, &dc)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	f, err := os.Open(constants.UPLOAD_FILE_DIR + "/" + dc.Config.FilePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open dump file : %v, no such file or directory", dc.Config.FilePath), http.StatusNotFound)
		return
	}
	// We don't support Dynamodb in web hence no need to pass schema sample size here.
	sourceProfile, _ := profiles.NewSourceProfile("", dc.Config.Driver)
	sourceProfile.Driver = dc.Config.Driver
	targetProfile, _ := profiles.NewTargetProfile(fmt.Sprintf("dialect=%s", dc.SpannerDetails.Dialect))
	conv, err := conversion.SchemaConv(sourceProfile, targetProfile, &utils.IOStreams{In: f, Out: os.Stdout})
	if err != nil {
		http.Error(w, fmt.Sprintf("Schema Conversion Error : %v", err), http.StatusNotFound)
		return
	}

	sessionMetadata := session.SessionMetadata{
		SessionName:  "NewSession",
		DatabaseType: dc.Config.Driver,
		DatabaseName: filepath.Base(dc.Config.FilePath),
		Dialect:      dc.SpannerDetails.Dialect,
	}

	sessionState := session.GetSessionState()
	sessionState.Conv = conv

	primarykey.DetectHotspot()
	index.IndexSuggestion()

	sessionState.SessionMetadata = sessionMetadata
	sessionState.Driver = dc.Config.Driver
	sessionState.DbName = ""
	sessionState.SessionFile = ""
	sessionState.SourceDB = nil
	sessionState.Dialect = dc.SpannerDetails.Dialect
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Path:           constants.UPLOAD_FILE_DIR + "/" + dc.Config.FilePath,
		ConnectionType: helpers.DUMP_MODE,
	}

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionMetadata,
		Conv:            *conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

// loadSession load seesion file to Harbourbridge.
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

// getDDL returns the Spanner DDL for each table in alphabetical order.
// Unlike internal/convert.go's GetDDL, it does not print tables in a way that
// respects the parent/child ordering of interleaved tables.
// Though foreign keys and secondary indexes are displayed, getDDL cannot be used to
// build DDL to send to Spanner.
func getDDL(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	c := ddl.Config{Comments: true, ProtectIds: false, SpDialect: sessionState.Conv.SpDialect, Source: sessionState.Driver}
	var tables []string
	for t := range sessionState.Conv.SpSchema {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	ddl := make(map[string]string)
	for _, t := range tables {
		table := sessionState.Conv.SpSchema[t]
		tableDdl := table.PrintCreateTable(sessionState.Conv.SpSchema, c) + ";"
		if len(table.Indexes) > 0 {
			tableDdl = tableDdl + "\n"
		}
		for _, index := range table.Indexes {
			tableDdl = tableDdl + "\n" + index.PrintCreateIndex(table, c) + ";"
		}
		if len(table.ForeignKeys) > 0 {
			tableDdl = tableDdl + "\n"
		}
		for _, fk := range table.ForeignKeys {
			tableDdl = tableDdl + "\n" + fk.PrintForeignKeyAlterTable(sessionState.Conv.SpSchema, c, t) + ";"
		}

		ddl[t] = tableDdl
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ddl)
}

func getStandardTypeToPGSQLTypemap(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ddl.STANDARD_TYPE_TO_PGSQL_TYPEMAP)
}

func getPGSQLToStandardTypeTypemap(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ddl.PGSQL_TO_STANDARD_TYPE_TYPEMAP)
}

// getTypeMap returns the source to Spanner typemap only for the
// source types used in current conversion.
func getTypeMap(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()

	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	var typeMap map[string][]typeIssue
	initializeTypeMap()
	switch sessionState.Driver {
	case constants.MYSQL, constants.MYSQLDUMP:
		typeMap = mysqlTypeMap
	case constants.POSTGRES, constants.PGDUMP:
		typeMap = postgresTypeMap
	case constants.SQLSERVER:
		typeMap = sqlserverTypeMap
	case constants.ORACLE:
		typeMap = oracleTypeMap
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", sessionState.Driver), http.StatusBadRequest)
		return
	}
	// Filter typeMap so it contains just the types SrcSchema uses.
	filteredTypeMap := make(map[string][]typeIssue)
	for _, srcTable := range sessionState.Conv.SrcSchema {
		for _, colDef := range srcTable.ColDefs {
			if _, ok := filteredTypeMap[colDef.Type.Name]; ok {
				continue
			}
			// Timestamp and interval types do not have exact key in typemap.
			// Typemap for  TIMESTAMP(6), TIMESTAMP(6) WITH LOCAL TIMEZONE,TIMESTAMP(6) WITH TIMEZONE is stored into TIMESTAMP key.
			// Same goes with interval types like INTERVAL YEAR(2) TO MONTH, INTERVAL DAY(2) TO SECOND(6) etc.
			// If exact key not found then check with regex.
			if _, ok := typeMap[colDef.Type.Name]; !ok {
				if oracle.TimestampReg.MatchString(colDef.Type.Name) {
					filteredTypeMap[colDef.Type.Name] = typeMap["TIMESTAMP"]
				} else if oracle.IntervalReg.MatchString(colDef.Type.Name) {
					filteredTypeMap[colDef.Type.Name] = typeMap["INTERVAL"]
				}
				continue
			}
			filteredTypeMap[colDef.Type.Name] = typeMap[colDef.Type.Name]
		}
	}
	for key, values := range filteredTypeMap {
		for i := range values {
			if sessionState.Dialect == constants.DIALECT_POSTGRESQL {
				spType := ddl.Type{
					Name: filteredTypeMap[key][i].T,
				}
				filteredTypeMap[key][i].DisplayT = ddl.GetPGType(spType)
			} else {
				filteredTypeMap[key][i].DisplayT = filteredTypeMap[key][i].T
			}

		}
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(filteredTypeMap)
}

// applyRule allows to add rules that changes the schema
// currently it supports two types of operations viz. SetGlobalDataType and AddIndex
func applyRule(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var rule internal.Rule
	err = json.Unmarshal(reqBody, &rule)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	if rule.Type == constants.GlobalDataTypeChange {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		typeMap := map[string]string{}
		err = json.Unmarshal(d, &typeMap)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		setGlobalDataType(typeMap)
	} else if rule.Type == constants.AddIndex {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		newIdx := ddl.CreateIndex{}
		err = json.Unmarshal(d, &newIdx)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		addedIndex, err := addIndex(newIdx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rule.Data = addedIndex
	} else if rule.Type == constants.EditColumnMaxLength {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		var colMaxLength ColMaxLength
		err = json.Unmarshal(d, &colMaxLength)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		setSpColMaxLength(colMaxLength, rule.AssociatedObjects)
	} else {
		http.Error(w, "Invalid rule type", http.StatusInternalServerError)
		return
	}

	ruleId := internal.GenerateRuleId()
	rule.Id = ruleId

	sessionState := session.GetSessionState()
	sessionState.Conv.Rules = append(sessionState.Conv.Rules, rule)
	session.UpdateSessionFile()
	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func dropRule(w http.ResponseWriter, r *http.Request) {
	ruleId := r.FormValue("id")
	if ruleId == "" {
		http.Error(w, fmt.Sprint("Rule id is empty"), http.StatusBadRequest)
		return
	}
	sessionState := session.GetSessionState()
	conv := sessionState.Conv
	var rule internal.Rule
	position := -1

	for i, r := range conv.Rules {
		if r.Id == ruleId {
			rule = r
			position = i
			break
		}
	}
	if position == -1 {
		http.Error(w, fmt.Sprint("Rule to be deleted not found"), http.StatusBadRequest)
		return
	}

	if rule.Type == constants.AddIndex {
		if rule.Enabled {
			d, err := json.Marshal(rule.Data)
			if err != nil {
				http.Error(w, "Invalid rule data", http.StatusInternalServerError)
				return
			}
			var index ddl.CreateIndex
			err = json.Unmarshal(d, &index)
			if err != nil {
				http.Error(w, "Invalid rule data", http.StatusInternalServerError)
				return
			}
			tableId := index.TableId
			indexId := index.Id
			err = dropSecondaryIndexHelper(tableId, indexId)
			if err != nil {
				http.Error(w, fmt.Sprintf("%v", err), http.StatusBadRequest)
				return
			}
		}
	} else if rule.Type == constants.GlobalDataTypeChange {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		typeMap := map[string]string{}
		err = json.Unmarshal(d, &typeMap)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		revertGlobalDataType(typeMap)
	} else if rule.Type == constants.EditColumnMaxLength {
		d, err := json.Marshal(rule.Data)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		var colMaxLength ColMaxLength
		err = json.Unmarshal(d, &colMaxLength)
		if err != nil {
			http.Error(w, "Invalid rule data", http.StatusInternalServerError)
			return
		}
		revertSpColMaxLength(colMaxLength, rule.AssociatedObjects)
	} else {
		http.Error(w, "Invalid rule type", http.StatusInternalServerError)
		return
	}

	sessionState.Conv.Rules = append(conv.Rules[:position], conv.Rules[position+1:]...)
	if len(sessionState.Conv.Rules) == 0 {
		sessionState.Conv.Rules = nil
	}
	session.UpdateSessionFile()
	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)

}

// setGlobalDataType allows to change Spanner type globally.
// It takes a map from source type to Spanner type and updates
// the Spanner schema accordingly.
func setGlobalDataType(typeMap map[string]string) {
	sessionState := session.GetSessionState()

	// Redo source-to-Spanner typeMap using t (the mapping specified in the http request).
	// We drive this process by iterating over the Spanner schema because we want to preserve all
	// other customizations that have been performed via the UI (dropping columns, renaming columns
	// etc). In particular, note that we can't just blindly redo schema conversion (using an appropriate
	// version of 'toDDL' with the new typeMap).
	for tableId, spSchema := range sessionState.Conv.SpSchema {
		for colId := range spSchema.ColDefs {
			srcColDef := sessionState.Conv.SrcSchema[tableId].ColDefs[colId]
			// If the srcCol's type is in the map, then recalculate the Spanner type
			// for this column using the map. Otherwise, leave the ColDef for this
			// column as is. Note that per-column type overrides could be lost in
			// this process -- the mapping in typeMap always takes precendence.
			if _, found := typeMap[srcColDef.Type.Name]; found {
				utilities.UpdateDataType(sessionState.Conv, typeMap[srcColDef.Type.Name], tableId, colId)
			}
		}
		common.ComputeNonKeyColumnSize(sessionState.Conv, tableId)
	}
}

func setSpColMaxLength(spColMaxLength ColMaxLength, associatedObjects string) {
	sessionState := session.GetSessionState()
	if associatedObjects == "All table" {
		for tId := range sessionState.Conv.SpSchema {
			for _, colDef := range sessionState.Conv.SpSchema[tId].ColDefs {
				if colDef.T.Name == spColMaxLength.SpDataType {
					spColDef := colDef
					if spColDef.T.Len == ddl.MaxLength {
						spColDef.T.Len, _ = strconv.ParseInt(spColMaxLength.SpColMaxLength, 10, 64)
					}
					sessionState.Conv.SpSchema[tId].ColDefs[colDef.Id] = spColDef
				}
			}
			common.ComputeNonKeyColumnSize(sessionState.Conv, tId)
		}
	} else {
		for _, colDef := range sessionState.Conv.SpSchema[associatedObjects].ColDefs {
			if colDef.T.Name == spColMaxLength.SpDataType {
				spColDef := colDef
				if spColDef.T.Len == ddl.MaxLength {
					table.UpdateColumnSize(spColMaxLength.SpColMaxLength, associatedObjects, colDef.Id, sessionState.Conv)
				}
			}
		}
		common.ComputeNonKeyColumnSize(sessionState.Conv, associatedObjects)
	}
}

func revertSpColMaxLength(spColMaxLength ColMaxLength, associatedObjects string) {
	sessionState := session.GetSessionState()
	spColLen, _ := strconv.ParseInt(spColMaxLength.SpColMaxLength, 10, 64)
	if associatedObjects == "All table" {
		for tId := range sessionState.Conv.SpSchema {
			for colId, colDef := range sessionState.Conv.SpSchema[tId].ColDefs {
				if colDef.T.Name == spColMaxLength.SpDataType {
					utilities.UpdateMaxColumnLen(sessionState.Conv, spColMaxLength.SpDataType, tId, colId, spColLen)
				}
			}
			common.ComputeNonKeyColumnSize(sessionState.Conv, tId)
		}
	} else {
		for colId, colDef := range sessionState.Conv.SpSchema[associatedObjects].ColDefs {
			if colDef.T.Name == spColMaxLength.SpDataType {
				utilities.UpdateMaxColumnLen(sessionState.Conv, spColMaxLength.SpDataType, associatedObjects, colId, spColLen)
			}
		}
		common.ComputeNonKeyColumnSize(sessionState.Conv, associatedObjects)
	}
}

// revertGlobalDataType revert back the spanner type to default
// when the rule that is used to apply the data-type change is deleted.
// It takes a map from source type to Spanner type and updates
// the Spanner schema accordingly.
func revertGlobalDataType(typeMap map[string]string) {
	sessionState := session.GetSessionState()

	for tableId, spSchema := range sessionState.Conv.SpSchema {
		for colId, colDef := range spSchema.ColDefs {
			srcColDef, found := sessionState.Conv.SrcSchema[tableId].ColDefs[colId]
			if !found {
				continue
			}
			spType, found := typeMap[srcColDef.Type.Name]

			if !found {
				continue
			}

			if colDef.T.Name == spType {
				utilities.UpdateDataType(sessionState.Conv, "", tableId, colId)
			}
		}
		common.ComputeNonKeyColumnSize(sessionState.Conv, tableId)
	}
}

// addIndex checks the new name for spanner name validity, ensures the new name is already not used by existing tables
// secondary indexes or foreign key constraints. If above checks passed then new indexes are added to the schema else appropriate
// error thrown.
func addIndex(newIndex ddl.CreateIndex) (ddl.CreateIndex, error) {
	// Check new name for spanner name validity.
	newNames := []string{}
	newNames = append(newNames, newIndex.Name)

	if ok, invalidNames := utilities.CheckSpannerNamesValidity(newNames); !ok {
		return ddl.CreateIndex{}, fmt.Errorf("following names are not valid Spanner identifiers: %s", strings.Join(invalidNames, ","))
	}
	// Check that the new names are not already used by existing tables, secondary indexes or foreign key constraints.
	if ok, err := utilities.CanRename(newNames, newIndex.TableId); !ok {
		return ddl.CreateIndex{}, err
	}

	sessionState := session.GetSessionState()
	sp := sessionState.Conv.SpSchema[newIndex.TableId]

	newIndexes := []ddl.CreateIndex{newIndex}
	index.CheckIndexSuggestion(newIndexes, sp)
	for i := 0; i < len(newIndexes); i++ {
		newIndexes[i].Id = internal.GenerateIndexesId()
	}

	sessionState.Conv.UsedNames[newIndex.Name] = true
	sp.Indexes = append(sp.Indexes, newIndexes...)
	sessionState.Conv.SpSchema[newIndex.TableId] = sp
	return newIndexes[0], nil
}

// getConversionRate returns table wise color coded conversion rate.
func getConversionRate(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	hb_reports := reports.AnalyzeTables(sessionState.Conv, nil)
	rate := make(map[string]string)
	for _, t := range hb_reports {
		rate[t.SpTable], _ = reports.RateSchema(t.Cols, t.Warnings, t.Errors, t.SyntheticPKey != "", false)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(rate)
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
	conversion.WriteSchemaFile(sessionState.Conv, now, schemaFileName, ioHelper.Out, sessionState.Driver)
	schemaAbsPath, err := filepath.Abs(schemaFileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not create absolute path : %v", err), http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(schemaAbsPath))
}

// getReportFile generates report file and returns file path.
func getReportFile(w http.ResponseWriter, r *http.Request) {
	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	var err error
	now := time.Now()
	filePrefix, err := utilities.GetFilePrefix(now)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not get file prefix : %v", err), http.StatusInternalServerError)
	}
	reportFileName := "frontend/" + filePrefix
	sessionState := session.GetSessionState()
	conversion.Report(sessionState.Driver, nil, ioHelper.BytesRead, "", sessionState.Conv, reportFileName, sessionState.DbName, ioHelper.Out)
	reportAbsPath, err := filepath.Abs(reportFileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not create absolute path : %v", err), http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(reportAbsPath))
}

// TableInterleaveStatus stores data regarding interleave status.
type TableInterleaveStatus struct {
	Possible bool
	Parent   string
	Comment  string
}

// setParentTable checks whether specified table can be interleaved, and updates the schema to convert foreign
// key to interleaved table if 'update' parameter is set to true. If 'update' parameter is set to false, then return
// whether the foreign key can be converted to interleave table without updating the schema.
func setParentTable(w http.ResponseWriter, r *http.Request) {
	tableId := r.FormValue("table")
	update := r.FormValue("update") == "true"
	sessionState := session.GetSessionState()

	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
	}
	tableInterleaveStatus := parentTableHelper(tableId, update)

	if tableInterleaveStatus.Possible {

		childPks := sessionState.Conv.SpSchema[tableId].PrimaryKeys
		childindex := utilities.GetPrimaryKeyIndexFromOrder(childPks, 1)
		sessionState := session.GetSessionState()
		schemaissue := []internal.SchemaIssue{}

		colId := childPks[childindex].ColId
		schemaissue = sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId]
		if update {
			schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
		} else {
			schemaissue = append(schemaissue, internal.InterleavedOrder)
		}

		sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] = schemaissue
	} else {
		// Remove "Table cart can be converted as Interleaved Table" suggestion from columns
		// of the table if interleaving is not possible.
		for _, colId := range sessionState.Conv.SpSchema[tableId].ColIds {
			schemaIssue := []internal.SchemaIssue{}
			for _, v := range sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] {
				if v != internal.InterleavedOrder {
					schemaIssue = append(schemaIssue, v)
				}
			}
			sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] = schemaIssue
		}
	}

	index.IndexSuggestion()
	session.UpdateSessionFile()
	w.WriteHeader(http.StatusOK)

	if update {
		convm := session.ConvWithMetadata{
			SessionMetadata: sessionState.SessionMetadata,
			Conv:            *sessionState.Conv,
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tableInterleaveStatus": tableInterleaveStatus,
			"sessionState":          convm})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tableInterleaveStatus": tableInterleaveStatus,
		})
	}
}

func parentTableHelper(tableId string, update bool) *TableInterleaveStatus {
	tableInterleaveStatus := &TableInterleaveStatus{
		Possible: false,
		Comment:  "No valid prefix",
	}
	sessionState := session.GetSessionState()

	if _, found := sessionState.Conv.SyntheticPKeys[tableId]; found {
		tableInterleaveStatus.Possible = false
		tableInterleaveStatus.Comment = "Has synthetic pk"
	}

	childPks := sessionState.Conv.SpSchema[tableId].PrimaryKeys

	// Search this table's foreign keys for a suitable parent table.
	// If there are several possible parent tables, we pick the first one.
	// TODO: Allow users to pick which parent to use if more than one.
	for i, fk := range sessionState.Conv.SpSchema[tableId].ForeignKeys {
		refTableId := fk.ReferTableId

		if _, found := sessionState.Conv.SyntheticPKeys[refTableId]; found {
			continue
		}

		if checkPrimaryKeyPrefix(tableId, refTableId, fk, tableInterleaveStatus) {
			sp := sessionState.Conv.SpSchema[tableId]
			setInterleave := false

			pkFirstOrderColIndex := utilities.GetPrimaryKeyIndexFromOrder(sessionState.Conv.SpSchema[tableId].PrimaryKeys, 1)
			childFirstOrderPkColId := sessionState.Conv.SpSchema[tableId].PrimaryKeys[pkFirstOrderColIndex].ColId
			for _, colId := range fk.ColIds {
				if childFirstOrderPkColId == colId {
					setInterleave = true
				}
			}

			if update && sp.ParentId == "" && setInterleave {
				usedNames := sessionState.Conv.UsedNames
				delete(usedNames, sp.ForeignKeys[i].Name)
				sp.ParentId = refTableId
				sp.ForeignKeys = utilities.RemoveFk(sp.ForeignKeys, sp.ForeignKeys[i].Id)
			}
			sessionState.Conv.SpSchema[tableId] = sp

			parentpks := sessionState.Conv.SpSchema[refTableId].PrimaryKeys
			if len(parentpks) >= 1 {

				parentindex := utilities.GetPrimaryKeyIndexFromOrder(parentpks, 1)

				childindex := utilities.GetPrimaryKeyIndexFromOrder(childPks, 1)

				if parentindex != -1 && childindex != -1 {
					parentTable := sessionState.Conv.SpSchema[refTableId]
					childTable := sessionState.Conv.SpSchema[tableId]

					if (parentpks[parentindex].Order == childPks[childindex].Order) && (parentTable.ColDefs[parentpks[parentindex].ColId].Name == childTable.ColDefs[childPks[childindex].ColId].Name) &&
						(parentTable.ColDefs[parentpks[parentindex].ColId].T.Len == childTable.ColDefs[childPks[childindex].ColId].T.Len) &&
						(parentTable.ColDefs[parentpks[parentindex].ColId].T.Name == childTable.ColDefs[childPks[childindex].ColId].T.Name) {

						sessionState := session.GetSessionState()
						schemaissue := []internal.SchemaIssue{}

						colId := childPks[childindex].ColId
						schemaissue = sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId]

						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedChangeColumnSize)

						sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] = schemaissue
						tableInterleaveStatus.Possible = true
						tableInterleaveStatus.Parent = refTableId
						tableInterleaveStatus.Comment = ""

					}

					// Check if the tables can be interleaved after changing the order of primary key.
					referColIndex := utilities.GetRefColIndexFromFk(fk, parentpks[parentindex].ColId)
					if !setInterleave && referColIndex != -1 && fk.ColIds[referColIndex] != childPks[childindex].ColId {

						sessionState := session.GetSessionState()

						colId := fk.ColIds[referColIndex]

						schemaissue := []internal.SchemaIssue{}
						schemaissue = sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId]

						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedChangeColumnSize)

						schemaissue = append(schemaissue, internal.InterleavedNotInOrder)

						sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] = schemaissue

					}
				}
			}
		}
	}

	return tableInterleaveStatus
}

func removeParentTable(w http.ResponseWriter, r *http.Request) {
	tableId := r.FormValue("tableId")
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
		return
	}

	conv := sessionState.Conv

	if conv.SpSchema[tableId].ParentId == "" {
		http.Error(w, fmt.Sprintf("Table is not interleaved"), http.StatusBadRequest)
		return
	}
	spTable := conv.SpSchema[tableId]

	var firstOrderPk ddl.IndexKey

	for _, pk := range spTable.PrimaryKeys {
		if pk.Order == 1 {
			firstOrderPk = pk
			break
		}
	}

	spColId := conv.SpSchema[tableId].ColDefs[firstOrderPk.ColId].Id
	srcCol := conv.SrcSchema[tableId].ColDefs[spColId]
	interleavedFk, err := utilities.GetInterleavedFk(conv, tableId, srcCol.Id)
	if err != nil {
		http.Error(w, fmt.Sprintf("%v", err), http.StatusBadRequest)
		return
	}

	spFk, err := common.CvtForeignKeysHelper(conv, conv.SpSchema[tableId].Name, tableId, interleavedFk, true)
	if err != nil {
		http.Error(w, fmt.Sprintf("Foreign key conversion fail"), http.StatusBadRequest)
		return
	}

	spFks := spTable.ForeignKeys
	spFks = append(spFks, spFk)
	spTable.ForeignKeys = spFks
	spTable.ParentId = ""
	conv.SpSchema[tableId] = spTable

	sessionState.Conv = conv

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)

}

type DropDetail struct {
	Name string `json:"Name"`
}

func restoreTable(w http.ResponseWriter, r *http.Request) {
	tableId := r.FormValue("table")
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
	}

	conv := sessionState.Conv
	var toddl common.ToDdl
	switch sessionState.Driver {
	case constants.MYSQL:
		toddl = mysql.InfoSchemaImpl{}.GetToDdl()
	case constants.POSTGRES:
		toddl = postgres.InfoSchemaImpl{}.GetToDdl()
	case constants.SQLSERVER:
		toddl = sqlserver.InfoSchemaImpl{}.GetToDdl()
	case constants.ORACLE:
		toddl = oracle.InfoSchemaImpl{}.GetToDdl()
	case constants.MYSQLDUMP:
		toddl = mysql.DbDumpImpl{}.GetToDdl()
	case constants.PGDUMP:
		toddl = postgres.DbDumpImpl{}.GetToDdl()
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", sessionState.Driver), http.StatusBadRequest)
		return
	}

	err := common.SrcTableToSpannerDDL(conv, toddl, sessionState.Conv.SrcSchema[tableId])
	if err != nil {
		http.Error(w, fmt.Sprintf("Restoring spanner table fail"), http.StatusBadRequest)
		return
	}
	conv.AddPrimaryKeys()
	sessionState.Conv = conv
	primarykey.DetectHotspot()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func dropTable(w http.ResponseWriter, r *http.Request) {
	tableId := r.FormValue("table")
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
	}
	spSchema := sessionState.Conv.SpSchema
	issues := sessionState.Conv.SchemaIssues
	syntheticPkey := sessionState.Conv.SyntheticPKeys

	//remove deleted name from usedName
	usedNames := sessionState.Conv.UsedNames
	delete(usedNames, sessionState.Conv.SpSchema[tableId].Name)
	for _, index := range sessionState.Conv.SpSchema[tableId].Indexes {
		delete(usedNames, index.Name)
	}
	for _, fk := range sessionState.Conv.SpSchema[tableId].ForeignKeys {
		delete(usedNames, fk.Name)
	}

	delete(spSchema, tableId)
	issues[tableId] = internal.TableIssues{
		TableLevelIssues:  []internal.SchemaIssue{},
		ColumnLevelIssues: map[string][]internal.SchemaIssue{},
	}
	delete(syntheticPkey, tableId)

	//drop reference foreign key
	for tableName, spTable := range spSchema {
		fks := []ddl.Foreignkey{}
		for _, fk := range spTable.ForeignKeys {
			if fk.ReferTableId != tableId {
				fks = append(fks, fk)
			} else {
				delete(usedNames, fk.Name)
			}

		}
		spTable.ForeignKeys = fks
		spSchema[tableName] = spTable
	}

	//remove interleave that are interleaved on the drop table as parent
	for tableId, spTable := range spSchema {
		if spTable.ParentId == tableId {
			spTable.ParentId = ""
			spSchema[tableId] = spTable
		}
	}

	//remove interleavable suggestion on droping the parent table
	for tableName, tableIssues := range issues {
		for colName, colIssues := range tableIssues.ColumnLevelIssues {
			updatedColIssues := []internal.SchemaIssue{}
			for _, val := range colIssues {
				if val != internal.InterleavedOrder {
					updatedColIssues = append(updatedColIssues, val)
				}
			}
			if len(updatedColIssues) == 0 {
				delete(issues[tableName].ColumnLevelIssues, colName)
			} else {
				issues[tableName].ColumnLevelIssues[colName] = updatedColIssues
			}
		}
	}

	sessionState.Conv.SpSchema = spSchema
	sessionState.Conv.SchemaIssues = issues
	sessionState.Conv.UsedNames = usedNames

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func restoreSecondaryIndex(w http.ResponseWriter, r *http.Request) {
	tableId := r.FormValue("tableId")
	indexId := r.FormValue("indexId")
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
		return
	}
	if indexId == "" {
		http.Error(w, fmt.Sprintf("Index Id is empty"), http.StatusBadRequest)
		return
	}

	var srcIndex schema.Index
	srcIndexFound := false
	for _, index := range sessionState.Conv.SrcSchema[tableId].Indexes {
		if index.Id == indexId {
			srcIndex = index
			srcIndexFound = true
			break
		}
	}
	if !srcIndexFound {
		http.Error(w, fmt.Sprintf("Source index not found"), http.StatusBadRequest)
		return
	}

	conv := sessionState.Conv

	spIndex := common.CvtIndexHelper(conv, tableId, srcIndex, conv.SpSchema[tableId].ColIds)
	spIndexes := conv.SpSchema[tableId].Indexes
	spIndexes = append(spIndexes, spIndex)
	spTable := conv.SpSchema[tableId]
	spTable.Indexes = spIndexes
	conv.SpSchema[tableId] = spTable

	sessionState.Conv = conv
	index.AssignInitialOrders()
	index.IndexSuggestion()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)

}

// renameForeignKeys checks the new names for spanner name validity, ensures the new names are already not used by existing tables
// secondary indexes or foreign key constraints. If above checks passed then foreignKey renaming reflected in the schema else appropriate
// error thrown.
func updateForeignKeys(w http.ResponseWriter, r *http.Request) {
	tableId := r.FormValue("table")
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}

	newFKs := []ddl.Foreignkey{}
	if err = json.Unmarshal(reqBody, &newFKs); err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	// Check new name for spanner name validity.
	newNames := []string{}
	newNamesMap := map[string]bool{}
	for _, newFk := range newFKs {
		for _, oldFk := range sessionState.Conv.SpSchema[tableId].ForeignKeys {
			if newFk.Id == oldFk.Id && newFk.Name != oldFk.Name && newFk.Name != "" {
				newNames = append(newNames, strings.ToLower(newFk.Name))
			}
		}
	}

	for _, newFk := range newFKs {
		if _, ok := newNamesMap[strings.ToLower(newFk.Name)]; ok {
			http.Error(w, fmt.Sprintf("Found duplicate names in input : %s", strings.ToLower(newFk.Name)), http.StatusBadRequest)
			return
		}
		newNamesMap[strings.ToLower(newFk.Name)] = true
	}

	if ok, invalidNames := utilities.CheckSpannerNamesValidity(newNames); !ok {
		http.Error(w, fmt.Sprintf("Following names are not valid Spanner identifiers: %s", strings.Join(invalidNames, ",")), http.StatusBadRequest)
		return
	}

	// Check that the new names are not already used by existing tables, secondary indexes or foreign key constraints.
	if ok, err := utilities.CanRename(newNames, tableId); !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sp := sessionState.Conv.SpSchema[tableId]
	usedNames := sessionState.Conv.UsedNames

	// Update session with renamed foreignkeys.
	updatedFKs := []ddl.Foreignkey{}

	for _, foreignKey := range sp.ForeignKeys {
		for _, updatedForeignkey := range newFKs {
			if foreignKey.Id == updatedForeignkey.Id && len(updatedForeignkey.ColIds) != 0 && updatedForeignkey.ReferTableId != "" {
				delete(usedNames, foreignKey.Name)
				foreignKey.Name = updatedForeignkey.Name
				updatedFKs = append(updatedFKs, foreignKey)
			}
		}
	}

	position := -1

	for i, fk := range updatedFKs {
		// Condition to check whether FK has to be dropped
		if len(fk.ReferColumnIds) == 0 && fk.ReferTableId == "" {
			position = i
			dropFkId := fk.Id

			// To remove the interleavable suggestions if they exist on dropping fk
			colId := sp.ForeignKeys[position].ColIds[0]
			schemaIssue := []internal.SchemaIssue{}
			for _, v := range sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] {
				if v != internal.InterleavedAddColumn && v != internal.InterleavedRenameColumn && v != internal.InterleavedNotInOrder && v != internal.InterleavedChangeColumnSize {
					schemaIssue = append(schemaIssue, v)
				}
			}
			if _, ok := sessionState.Conv.SchemaIssues[tableId]; ok {
				sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] = schemaIssue
			}

			sp.ForeignKeys = utilities.RemoveFk(updatedFKs, dropFkId)
		}
	}
	sp.ForeignKeys = updatedFKs
	sessionState.Conv.SpSchema[tableId] = sp
	session.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

// renameIndexes checks the new names for spanner name validity, ensures the new names are already not used by existing tables
// secondary indexes or foreign key constraints. If above checks passed then index renaming reflected in the schema else appropriate
// error thrown.
func renameIndexes(w http.ResponseWriter, r *http.Request) {
	table := r.FormValue("table")
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	renameMap := map[string]string{}
	if err = json.Unmarshal(reqBody, &renameMap); err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	// Check new name for spanner name validity.
	newNames := []string{}
	newNamesMap := map[string]bool{}
	for _, value := range renameMap {
		newNames = append(newNames, strings.ToLower(value))
		newNamesMap[strings.ToLower(value)] = true
	}
	if len(newNames) != len(newNamesMap) {
		http.Error(w, fmt.Sprintf("Found duplicate names in input : %s", strings.Join(newNames, ",")), http.StatusBadRequest)
		return
	}

	if ok, invalidNames := utilities.CheckSpannerNamesValidity(newNames); !ok {
		http.Error(w, fmt.Sprintf("Following names are not valid Spanner identifiers: %s", strings.Join(invalidNames, ",")), http.StatusBadRequest)
		return
	}

	// Check that the new names are not already used by existing tables, secondary indexes or foreign key constraints.
	if ok, err := utilities.CanRename(newNames, table); !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sessionState := session.GetSessionState()

	sp := sessionState.Conv.SpSchema[table]

	// Update session with renamed secondary indexes.
	newIndexes := []ddl.CreateIndex{}
	for _, index := range sp.Indexes {
		if newName, ok := renameMap[index.Id]; ok {
			index.Name = newName
		}
		newIndexes = append(newIndexes, index)
	}
	sp.Indexes = newIndexes

	sessionState.Conv.SpSchema[table] = sp
	session.UpdateSessionFile()
	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

// ToDo : To Remove once Rules Component updated
// addIndexes checks the new names for spanner name validity, ensures the new names are already not used by existing tables
// secondary indexes or foreign key constraints. If above checks passed then new indexes are added to the schema else appropriate
// error thrown.
func getSourceDestinationSummary(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	var sessionSummary sessionSummary
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
	instanceInfo, err := instanceClient.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: fmt.Sprintf("projects/%s/instances/%s", sessionState.GCPProjectID, sessionState.SpannerInstanceID)})
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
	sessionSummary.IsSharded = sessionState.IsSharded
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionSummary)
}

func updateProgress(w http.ResponseWriter, r *http.Request) {

	var detail progressDetails
	sessionState := session.GetSessionState()
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

	details := migrationDetails{}
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
	sourceProfile, targetProfile, ioHelper, dbName, err := getSourceAndTargetProfiles(sessionState, details)
	if err != nil {
		log.Println("can't get source and target profile")
		http.Error(w, fmt.Sprintf("Can't get source and target profiles: %v", err), http.StatusBadRequest)
		return
	}
	err = writeSessionFile(sessionState)
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
		go cmd.MigrateDatabase(ctx, targetProfile, sourceProfile, dbName, &ioHelper, &cmd.SchemaCmd{}, sessionState.Conv, &sessionState.Error)
	} else if details.MigrationMode == helpers.DATA_ONLY {
		dataCmd := &cmd.DataCmd{
			SkipForeignKeys: false,
			WriteLimit:      cmd.DefaultWritersLimit,
		}
		log.Println("Starting data only migration")
		sessionState.Conv.Audit.MigrationType = migration.MigrationData_DATA_ONLY.Enum()
		go cmd.MigrateDatabase(ctx, targetProfile, sourceProfile, dbName, &ioHelper, dataCmd, sessionState.Conv, &sessionState.Error)
	} else {
		schemaAndDataCmd := &cmd.SchemaAndDataCmd{
			SkipForeignKeys: false,
			WriteLimit:      cmd.DefaultWritersLimit,
		}
		log.Println("Starting schema and data migration")
		sessionState.Conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
		go cmd.MigrateDatabase(ctx, targetProfile, sourceProfile, dbName, &ioHelper, schemaAndDataCmd, sessionState.Conv, &sessionState.Error)
	}
	w.WriteHeader(http.StatusOK)
	log.Println("migration completed", "method", r.Method, "path", r.URL.Path, "remoteaddr", r.RemoteAddr)
}

func getGeneratedResources(w http.ResponseWriter, r *http.Request) {
	var generatedResources GeneratedResources
	sessionState := session.GetSessionState()
	generatedResources.DatabaseName = sessionState.SpannerDatabaseName
	generatedResources.DatabaseUrl = fmt.Sprintf("https://pantheon.corp.google.com/spanner/instances/%v/databases/%v/details/tables?project=%v", sessionState.SpannerInstanceID, sessionState.SpannerDatabaseName, sessionState.GCPProjectID)
	generatedResources.BucketName = sessionState.Bucket + sessionState.RootPath
	generatedResources.BucketUrl = fmt.Sprintf("https://pantheon.corp.google.com/storage/browser/%v", sessionState.Bucket+sessionState.RootPath)
	generatedResources.ShardToDataflowMap = make(map[string]ResourceDetails)
	generatedResources.ShardToDatastreamMap = make(map[string]ResourceDetails)
	if sessionState.Conv.Audit.StreamingStats.DataStreamName != "" {
		generatedResources.DataStreamJobName = sessionState.Conv.Audit.StreamingStats.DataStreamName
		generatedResources.DataStreamJobUrl = fmt.Sprintf("https://pantheon.corp.google.com/datastream/streams/locations/%v/instances/%v?project=%v", sessionState.Region, sessionState.Conv.Audit.StreamingStats.DataStreamName, sessionState.GCPProjectID)
	}
	if sessionState.Conv.Audit.StreamingStats.DataflowJobId != "" {
		generatedResources.DataflowJobName = sessionState.Conv.Audit.StreamingStats.DataflowJobId
		generatedResources.DataflowJobUrl = fmt.Sprintf("https://pantheon.corp.google.com/dataflow/jobs/%v/%v?project=%v", sessionState.Region, sessionState.Conv.Audit.StreamingStats.DataflowJobId, sessionState.GCPProjectID)
	}
	for shardId, dsName := range sessionState.Conv.Audit.StreamingStats.ShardToDataStreamNameMap {
		url := fmt.Sprintf("https://pantheon.corp.google.com/datastream/streams/locations/%v/instances/%v?project=%v", sessionState.Region, dsName, sessionState.GCPProjectID)
		resourceDetails := ResourceDetails{JobName: dsName, JobUrl: url}
		generatedResources.ShardToDatastreamMap[shardId] = resourceDetails
	}
	for shardId, dfId := range sessionState.Conv.Audit.StreamingStats.ShardToDataflowJobMap {
		url := fmt.Sprintf("https://pantheon.corp.google.com/dataflow/jobs/%v/%v?project=%v", sessionState.Region, dfId, sessionState.GCPProjectID)
		resourceDetails := ResourceDetails{JobName: dfId, JobUrl: url}
		generatedResources.ShardToDataflowMap[shardId] = resourceDetails
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(generatedResources)
}

func getSourceAndTargetProfiles(sessionState *session.SessionState, details migrationDetails) (profiles.SourceProfile, profiles.TargetProfile, utils.IOStreams, string, error) {
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
	targetProfileString := fmt.Sprintf("project=%v,instance=%v,dbName=%v,dialect=%v", sessionState.GCPProjectID, sessionState.SpannerInstanceID, details.TargetDetails.TargetDB, sessionState.Dialect)
	if details.MigrationType == helpers.LOW_DOWNTIME_MIGRATION && !details.IsSharded {
		fileName := sessionState.Conv.Audit.MigrationRequestId + "-streaming.json"
		sessionState.Bucket, sessionState.RootPath, err = profile.GetBucket(sessionState.GCPProjectID, sessionState.Region, details.TargetDetails.TargetConnectionProfileName)
		if err != nil {
			return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", fmt.Errorf("error while getting target bucket: %v", err)
		}
		err = createStreamingCfgFile(sessionState, details.TargetDetails, details.DataflowConfig, fileName)
		if err != nil {
			return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", fmt.Errorf("error while creating streaming config file: %v", err)
		}
		sourceProfileString = sourceProfileString + fmt.Sprintf(",streamingCfg=%v", fileName)
	} else {
		sessionState.Conv.Audit.MigrationRequestId = "HB-" + uuid.New().String()
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
	return sourceProfile, targetProfile, ioHelper, dbName, nil
}

func getSourceProfileStringForShardedMigrations(sessionState *session.SessionState, details migrationDetails) (string, error) {
	fileName := "HB-" + uuid.New().String() + "-sharding.cfg"
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

func createConfigFileForShardedDataflowMigration(sessionState *session.SessionState, details migrationDetails, fileName string) error {
	sourceProfileConfig := sessionState.SourceProfileConfig
	sourceProfileConfig.ShardConfigurationDataflow.DataflowConfig = profiles.DataflowConfig{
		Location:      sessionState.Region,
		HostProjectId: details.DataflowConfig.HostProjectId,
		Network:       details.DataflowConfig.Network,
		Subnetwork:    details.DataflowConfig.Subnetwork,
	}
	//Set the TmpDir from the sessionState bucket which is derived from the target connection profile
	for _, dataShard := range sourceProfileConfig.ShardConfigurationDataflow.DataShards {
		bucket, rootPath, err := profile.GetBucket(sessionState.GCPProjectID, sessionState.Region, dataShard.DstConnectionProfile.Name)
		if err != nil {
			return fmt.Errorf("error while getting target bucket: %v", err)
		}
		dataShard.TmpDir = "gs://" + bucket + rootPath
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

func createConfigFileForShardedBulkMigration(sessionState *session.SessionState, details migrationDetails, fileName string) error {
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

func writeSessionFile(sessionState *session.SessionState) error {

	err := utils.CreateGCSBucket(sessionState.Bucket, sessionState.GCPProjectID)
	if err != nil {
		return fmt.Errorf("error while creating bucket: %v", err)
	}

	convJSON, err := json.MarshalIndent(sessionState.Conv, "", " ")
	if err != nil {
		return fmt.Errorf("can't encode session state to JSON: %v", err)
	}
	err = utils.WriteToGCS("gs://"+sessionState.Bucket+sessionState.RootPath, "session.json", string(convJSON))
	if err != nil {
		return fmt.Errorf("error while writing to GCS: %v", err)
	}
	return nil
}

func createStreamingCfgFile(sessionState *session.SessionState, targetDetails targetDetails, dataflowConfig dataflowConfig, fileName string) error {
	data := StreamingCfg{
		DatastreamCfg: DatastreamCfg{
			StreamId:          "",
			StreamLocation:    sessionState.Region,
			StreamDisplayName: "",
			SourceConnectionConfig: ConnectionConfig{
				Name:     targetDetails.SourceConnectionProfileName,
				Location: sessionState.Region,
			},
			TargetConnectionConfig: ConnectionConfig{
				Name:     targetDetails.TargetConnectionProfileName,
				Location: sessionState.Region,
			},
		},
		DataflowCfg: DataflowCfg{
			JobName:       "",
			Location:      sessionState.Region,
			Network:       dataflowConfig.Network,
			Subnetwork:    dataflowConfig.Subnetwork,
			HostProjectId: dataflowConfig.HostProjectId,
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

func updateIndexes(w http.ResponseWriter, r *http.Request) {
	table := r.FormValue("table")
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	newIndexes := []ddl.CreateIndex{}
	if err = json.Unmarshal(reqBody, &newIndexes); err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	list := []int{}
	for i := 0; i < len(newIndexes); i++ {
		for j := 0; j < len(newIndexes[i].Keys); j++ {
			list = append(list, newIndexes[i].Keys[j].Order)
		}
	}

	if utilities.DuplicateInArray(list) != -1 {
		http.Error(w, fmt.Sprintf("Two Index columns can not have same order"), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()
	sp := sessionState.Conv.SpSchema[table]

	st := sessionState.Conv.SrcSchema[table]

	for i, ind := range sp.Indexes {

		if ind.TableId == newIndexes[0].TableId && ind.Id == newIndexes[0].Id {

			index.RemoveIndexIssues(table, sp.Indexes[i])

			sp.Indexes[i].Keys = newIndexes[0].Keys
			sp.Indexes[i].Name = newIndexes[0].Name
			sp.Indexes[i].TableId = newIndexes[0].TableId
			sp.Indexes[i].Unique = newIndexes[0].Unique
			sp.Indexes[i].Id = newIndexes[0].Id

			break
		}
	}

	for i, spIndex := range sp.Indexes {

		for j, srcIndex := range st.Indexes {

			for k, spIndexKey := range spIndex.Keys {

				for l, srcIndexKey := range srcIndex.Keys {

					if srcIndexKey.ColId == spIndexKey.ColId {

						st.Indexes[j].Keys[l].Order = sp.Indexes[i].Keys[k].Order
					}

				}
			}

		}
	}

	sessionState.Conv.SpSchema[table] = sp

	sessionState.Conv.SrcSchema[table] = st

	session.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func dropSecondaryIndex(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()

	table := r.FormValue("table")
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	var dropDetail struct{ Id string }
	if err = json.Unmarshal(reqBody, &dropDetail); err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}

	if table == "" || dropDetail.Id == "" {
		http.Error(w, fmt.Sprintf("Table name or position is empty"), http.StatusBadRequest)
	}
	err = dropSecondaryIndexHelper(table, dropDetail.Id)
	if err != nil {
		http.Error(w, fmt.Sprintf("%v", err), http.StatusBadRequest)
		return
	}

	// To set enabled value to false for the rule associated with the dropped index.
	indexId := dropDetail.Id
	for i, rule := range sessionState.Conv.Rules {
		if rule.Type == constants.AddIndex {
			d, err := json.Marshal(rule.Data)
			if err != nil {
				http.Error(w, "Invalid rule data", http.StatusInternalServerError)
				return
			}
			var index ddl.CreateIndex
			err = json.Unmarshal(d, &index)
			if err != nil {
				http.Error(w, "Invalid rule data", http.StatusInternalServerError)
				return
			}
			if index.Id == indexId {
				sessionState.Conv.Rules[i].Enabled = false
				break
			}
		}
	}

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func dropSecondaryIndexHelper(tableId, idxId string) error {
	if tableId == "" || idxId == "" {
		return fmt.Errorf("Table id or index id is empty")
	}
	sessionState := session.GetSessionState()
	sp := sessionState.Conv.SpSchema[tableId]
	position := -1
	for i, index := range sp.Indexes {
		if idxId == index.Id {
			position = i
			break
		}
	}
	if position < 0 || position >= len(sp.Indexes) {
		return fmt.Errorf("No secondary index found at position %d", position)
	}

	usedNames := sessionState.Conv.UsedNames
	delete(usedNames, sp.Indexes[position].Name)
	index.RemoveIndexIssues(tableId, sp.Indexes[position])

	sp.Indexes = utilities.RemoveSecondaryIndex(sp.Indexes, position)
	sessionState.Conv.SpSchema[tableId] = sp
	session.UpdateSessionFile()
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

func checkPrimaryKeyPrefix(tableId string, refTableId string, fk ddl.Foreignkey, tableInterleaveStatus *TableInterleaveStatus) bool {

	sessionState := session.GetSessionState()
	childTable := sessionState.Conv.SpSchema[tableId]
	parentTable := sessionState.Conv.SpSchema[refTableId]
	childPks := sessionState.Conv.SpSchema[tableId].PrimaryKeys
	parentPks := sessionState.Conv.SpSchema[refTableId].PrimaryKeys

	parentIndex := utilities.GetPrimaryKeyIndexFromOrder(parentPks, 1)
	if parentIndex == -1 {
		return false
	}
	parentFirstOrderPkColId := parentPks[parentIndex].ColId
	possibleInterleave := false
	for _, colId := range fk.ReferColumnIds {
		if parentFirstOrderPkColId == colId {
			possibleInterleave = true
		}
	}
	if !possibleInterleave {
		removeInterleaveSuggestions(fk.ColIds, tableId)
		return false
	}

	childPkColIds := []string{}
	for _, k := range childPks {
		childPkColIds = append(childPkColIds, k.ColId)
	}

	interleaved := []ddl.IndexKey{}

	for i := 0; i < len(parentPks); i++ {

		for j := 0; j < len(childPks); j++ {

			for k := 0; k < len(fk.ReferColumnIds); k++ {

				if childTable.ColDefs[fk.ColIds[k]].Name == parentTable.ColDefs[fk.ReferColumnIds[k]].Name &&
					parentTable.ColDefs[parentPks[i].ColId].Name == childTable.ColDefs[childPks[j].ColId].Name &&
					parentTable.ColDefs[parentPks[i].ColId].T.Name == childTable.ColDefs[childPks[j].ColId].T.Name &&
					parentTable.ColDefs[parentPks[i].ColId].T.Len == childTable.ColDefs[childPks[j].ColId].T.Len &&
					parentTable.ColDefs[parentPks[i].ColId].Name == parentTable.ColDefs[fk.ReferColumnIds[k]].Name &&
					childTable.ColDefs[childPks[j].ColId].Name == parentTable.ColDefs[fk.ReferColumnIds[k]].Name {

					interleaved = append(interleaved, parentPks[i])
				}
			}

		}

	}

	diff := []ddl.IndexKey{}

	if len(interleaved) == 0 {

		for i := 0; i < len(parentPks); i++ {

			for j := 0; j < len(childPks); j++ {

				if parentTable.ColDefs[parentPks[i].ColId].Name != childTable.ColDefs[childPks[j].ColId].Name {

					diff = append(diff, parentPks[i])
				}

			}
		}

	}

	canInterleavedOnAdd := []string{}
	canInterleavedOnRename := []string{}

	fkReferColNames := []string{}
	childPkColNames := []string{}
	for _, colId := range fk.ReferColumnIds {
		fkReferColNames = append(fkReferColNames, parentTable.ColDefs[colId].Name)
	}
	for _, colId := range childPkColIds {
		childPkColNames = append(childPkColNames, childTable.ColDefs[colId].Name)
	}

	for i := 0; i < len(diff); i++ {

		parentColIndex := utilities.IsColumnPresent(fkReferColNames, parentTable.ColDefs[diff[i].ColId].Name)
		if parentColIndex == -1 {
			continue
		}
		childColIndex := utilities.IsColumnPresent(childPkColNames, childTable.ColDefs[fk.ColIds[parentColIndex]].Name)
		if childColIndex == -1 {
			canInterleavedOnAdd = append(canInterleavedOnAdd, fk.ColIds[parentColIndex])
		} else {
			canInterleavedOnRename = append(canInterleavedOnRename, fk.ColIds[parentColIndex])
		}
	}

	if parentTable.ColDefs[parentPks[0].ColId].Name == childTable.ColDefs[childPks[0].ColId].Name &&
		parentTable.ColDefs[parentPks[0].ColId].T.Len != childTable.ColDefs[childPks[0].ColId].T.Len {
		updateInterleaveSuggestion([]string{childPks[0].ColId}, tableId, internal.InterleavedChangeColumnSize)
	} else if len(canInterleavedOnRename) > 0 {
		updateInterleaveSuggestion(canInterleavedOnRename, tableId, internal.InterleavedRenameColumn)
	} else if len(canInterleavedOnAdd) > 0 {
		updateInterleaveSuggestion(canInterleavedOnAdd, tableId, internal.InterleavedAddColumn)
	}

	if len(interleaved) > 0 {
		return true
	}

	return false
}

func updateInterleaveSuggestion(colIds []string, tableId string, issue internal.SchemaIssue) {
	for i := 0; i < len(colIds); i++ {

		sessionState := session.GetSessionState()

		schemaissue := []internal.SchemaIssue{}

		schemaissue = sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colIds[i]]

		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedChangeColumnSize)

		schemaissue = append(schemaissue, issue)

		if sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues == nil {

			s := map[string][]internal.SchemaIssue{
				colIds[i]: schemaissue,
			}
			sessionState.Conv.SchemaIssues[tableId] = internal.TableIssues{
				ColumnLevelIssues: s,
			}
		} else {
			sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colIds[i]] = schemaissue
		}
	}
}

func removeInterleaveSuggestions(colIds []string, tableId string) {
	for i := 0; i < len(colIds); i++ {

		sessionState := session.GetSessionState()

		schemaissue := []internal.SchemaIssue{}

		schemaissue = sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colIds[i]]

		if len(schemaissue) == 0 {
			continue
		}

		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedChangeColumnSize)

		if sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues == nil {

			s := map[string][]internal.SchemaIssue{
				colIds[i]: schemaissue,
			}
			sessionState.Conv.SchemaIssues[tableId] = internal.TableIssues{
				ColumnLevelIssues: s,
			}
		} else {
			sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colIds[i]] = schemaissue
		}

	}
}

// SessionState stores information for the current migration session.
type SessionState struct {
	sourceDB    *sql.DB        // Connection to source database in case of direct connection
	dbName      string         // Name of source database
	driver      string         // Name of HarbourBridge driver in use
	conv        *internal.Conv // Current conversion state
	sessionFile string         // Path to session file
}

// Type and issue.
type typeIssue struct {
	T        string
	Brief    string
	DisplayT string
}

type ResourceDetails struct {
	JobName string `json:"JobName"`
	JobUrl  string `json:"JobUrl"`
}
type GeneratedResources struct {
	DatabaseName         string                     `json:"DatabaseName"`
	DatabaseUrl          string                     `json:"DatabaseUrl"`
	BucketName           string                     `json:"BucketName"`
	BucketUrl            string                     `json:"BucketUrl"`
	//Used for single instance migration flow
	DataStreamJobName    string                     `json:"DataStreamJobName"`
	DataStreamJobUrl     string                     `json:"DataStreamJobUrl"`
	DataflowJobName      string                     `json:"DataflowJobName"`
	DataflowJobUrl       string                     `json:"DataflowJobUrl"`
	//Used for sharded migration flow
	ShardToDatastreamMap map[string]ResourceDetails `json:"ShardToDatastreamMap"`
	ShardToDataflowMap   map[string]ResourceDetails `json:"ShardToDataflowMap"`
}

func addTypeToList(convertedType string, spType string, issues []internal.SchemaIssue, l []typeIssue) []typeIssue {
	if convertedType == spType {
		if len(issues) > 0 {
			var briefs []string
			for _, issue := range issues {
				briefs = append(briefs, reports.IssueDB[issue].Brief)
			}
			l = append(l, typeIssue{T: spType, Brief: fmt.Sprintf(strings.Join(briefs, ", "))})
		} else {
			l = append(l, typeIssue{T: spType})
		}
	}
	return l
}

func initializeTypeMap() {
	sessionState := session.GetSessionState()
	var toddl common.ToDdl

	// Initialize mysqlTypeMap.
	toddl = mysql.InfoSchemaImpl{}.GetToDdl()
	for _, srcTypeName := range []string{"bool", "boolean", "varchar", "char", "text", "tinytext", "mediumtext", "longtext", "set", "enum", "json", "bit", "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "double", "float", "numeric", "decimal", "date", "datetime", "timestamp", "time", "year", "geometrycollection", "multipoint", "multilinestring", "multipolygon", "point", "linestring", "polygon", "geometry"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			srcType := schema.MakeType()
			srcType.Name = srcTypeName
			ty, issues := toddl.ToSpannerType(sessionState.Conv, spType, srcType)
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		if srcTypeName == "tinyint" {
			l = append(l, typeIssue{T: ddl.Bool, Brief: "Only tinyint(1) can be converted to BOOL, for any other mods it will be converted to INT64"})
		}
		mysqlTypeMap[srcTypeName] = l
	}
	// Initialize postgresTypeMap.
	toddl = postgres.InfoSchemaImpl{}.GetToDdl()
	for _, srcTypeName := range []string{"bool", "boolean", "bigserial", "bpchar", "character", "bytea", "date", "float8", "double precision", "float4", "real", "int8", "bigint", "int4", "integer", "int2", "smallint", "numeric", "serial", "text", "timestamptz", "timestamp with time zone", "timestamp", "timestamp without time zone", "varchar", "character varying"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			srcType := schema.MakeType()
			srcType.Name = srcTypeName
			ty, issues := toddl.ToSpannerType(sessionState.Conv, spType, srcType)
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		postgresTypeMap[srcTypeName] = l
	}

	// Initialize sqlserverTypeMap.
	toddl = sqlserver.InfoSchemaImpl{}.GetToDdl()
	for _, srcTypeName := range []string{"int", "tinyint", "smallint", "bigint", "bit", "float", "real", "numeric", "decimal", "money", "smallmoney", "char", "nchar", "varchar", "nvarchar", "text", "ntext", "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp", "rowversion", "binary", "varbinary", "image", "xml", "geography", "geometry", "uniqueidentifier", "sql_variant", "hierarchyid"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			srcType := schema.MakeType()
			srcType.Name = srcTypeName
			ty, issues := toddl.ToSpannerType(sessionState.Conv, spType, srcType)
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		sqlserverTypeMap[srcTypeName] = l
	}

	// Initialize oracleTypeMap.
	toddl = oracle.InfoSchemaImpl{}.GetToDdl()
	for _, srcTypeName := range []string{"NUMBER", "BFILE", "BLOB", "CHAR", "CLOB", "DATE", "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT", "LONG", "RAW", "LONG RAW", "NCHAR", "NVARCHAR2", "VARCHAR", "VARCHAR2", "NCLOB", "ROWID", "UROWID", "XMLTYPE", "TIMESTAMP", "INTERVAL", "SDO_GEOMETRY"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			srcType := schema.MakeType()
			srcType.Name = srcTypeName
			ty, issues := toddl.ToSpannerType(sessionState.Conv, spType, srcType)
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		oracleTypeMap[srcTypeName] = l
	}
}

func init() {
	sessionState := session.GetSessionState()
	utilities.InitObjectId()
	sessionState.Conv = internal.MakeConv()
	config := config.TryInitializeSpannerConfig()
	session.SetSessionStorageConnectionState(config.GCPProjectID, config.SpannerInstanceID)
}

// App connects to the web app v2.
func App(logLevel string, open bool, port int) error {
	err := logger.InitializeLogger(logLevel)
	if err != nil {
		return fmt.Errorf("error initialising webapp, did you specify a valid log-level? [DEBUG, INFO]")
	}
	addr := fmt.Sprintf(":%s", strconv.Itoa(port))
	router := getRoutes()
	fmt.Println("Starting Harbourbridge UI at:", fmt.Sprintf("http://localhost%s", addr))
	if open {
		browser.OpenURL(fmt.Sprintf("http://localhost%s", addr))
	}
	return http.ListenAndServe(addr, handlers.CORS(handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}), handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS"}), handlers.AllowedOrigins([]string{"*"}))(router))
}
