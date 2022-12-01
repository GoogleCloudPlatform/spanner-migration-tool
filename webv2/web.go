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
	"bufio"
	"bytes"
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
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
	"github.com/google/uuid"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/typemap"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/handlers"

	index "github.com/cloudspannerecosystem/harbourbridge/webv2/index"
	primarykey "github.com/cloudspannerecosystem/harbourbridge/webv2/primarykey"

	uniqueid "github.com/cloudspannerecosystem/harbourbridge/webv2/uniqueid"

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
	Driver   string `json:"Driver"`
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Database string `json:"Database"`
	User     string `json:"User"`
	Password string `json:"Password"`
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
}

type progressDetails struct {
	Progress       int
	ErrorMessage   string
	ProgressStatus int
}

type migrationDetails struct {
	TargetDetails targetDetails `json:"TargetDetails"`
	MigrationMode string        `json:MigrationMode`
	MigrationType string        `json:MigrationType`
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
	JobName  string `json:"JobName"`
	Location string `json:"Location"`
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

// databaseConnection creates connection with database when using
// with postgres and mysql driver.
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

	// Setting target db to spanner by default.
	conv.TargetDb = constants.TargetSpanner
	var err error
	switch sessionState.Driver {
	case constants.MYSQL:
		err = common.ProcessSchema(conv, mysql.InfoSchemaImpl{DbName: sessionState.DbName, Db: sessionState.SourceDB})
	case constants.POSTGRES:
		err = common.ProcessSchema(conv, postgres.InfoSchemaImpl{Db: sessionState.SourceDB})
	case constants.SQLSERVER:
		err = common.ProcessSchema(conv, sqlserver.InfoSchemaImpl{DbName: sessionState.DbName, Db: sessionState.SourceDB})
	case constants.ORACLE:
		err = common.ProcessSchema(conv, oracle.InfoSchemaImpl{DbName: strings.ToUpper(sessionState.DbName), Db: sessionState.SourceDB})
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", sessionState.Driver), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Schema Conversion Error : %v", err), http.StatusNotFound)
		return
	}

	uniqueid.InitObjectId()

	uniqueid.AssignUniqueId(conv)
	sessionState.Conv = conv

	primarykey.DetectHotspot()
	index.AssignInitialOrders()
	index.IndexSuggestion()

	sessionMetadata := session.SessionMetadata{
		SessionName:  "NewSession",
		DatabaseType: sessionState.Driver,
		DatabaseName: sessionState.DbName,
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

func setSourceDBDetailsForDump(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	if sessionState.Driver != constants.MYSQLDUMP && sessionState.Driver != constants.PGDUMP {
		http.Error(w, "Connect via direct connect", http.StatusBadRequest)
	}
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

func setSourceDBDetailsForDirectConnect(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	if sessionState.Driver == constants.MYSQLDUMP || sessionState.Driver == constants.PGDUMP {
		http.Error(w, "Connect via dump file", http.StatusBadRequest)
	}
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
	var dc dumpConfig
	err = json.Unmarshal(reqBody, &dc)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	f, err := os.Open("upload-file/" + dc.FilePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open dump file : %v, no such file or directory", dc.FilePath), http.StatusNotFound)
		return
	}
	// We don't support Dynamodb in web hence no need to pass schema sample size here.
	sourceProfile, _ := profiles.NewSourceProfile("", dc.Driver)
	sourceProfile.Driver = dc.Driver
	targetProfile, _ := profiles.NewTargetProfile("")
	targetProfile.TargetDb = constants.TargetSpanner
	conv, err := conversion.SchemaConv(sourceProfile, targetProfile, &utils.IOStreams{In: f, Out: os.Stdout})
	if err != nil {
		http.Error(w, fmt.Sprintf("Schema Conversion Error : %v", err), http.StatusNotFound)
		return
	}

	sessionMetadata := session.SessionMetadata{
		SessionName:  "NewSession",
		DatabaseType: dc.Driver,
		DatabaseName: filepath.Base(dc.FilePath),
	}

	sessionState := session.GetSessionState()
	uniqueid.InitObjectId()

	uniqueid.AssignUniqueId(conv)
	sessionState.Conv = conv
	primarykey.DetectHotspot()
	index.AssignInitialOrders()
	index.IndexSuggestion()

	sessionState.SessionMetadata = sessionMetadata
	sessionState.Driver = dc.Driver
	sessionState.DbName = ""
	sessionState.SessionFile = ""
	sessionState.SourceDB = nil
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Path:           dc.FilePath,
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

	uniqueid.InitObjectId()

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

	err = session.ReadSessionFileForSessionMetadata(&metadata, "upload-file/"+s.FilePath)
	if err != nil {
		switch err.(type) {
		case *fs.PathError:
			http.Error(w, fmt.Sprintf("Failed to open session file : %v, no such file or directory", s.FilePath), http.StatusNotFound)
		default:
			http.Error(w, fmt.Sprintf("Failed to parse session file : %v", err), http.StatusBadRequest)
		}
		return
	}

	err = conversion.ReadSessionFile(conv, "upload-file/"+s.FilePath)
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
	}

	if sessionMetadata.DatabaseName == "" {
		sessionMetadata.DatabaseName = strings.TrimRight(filepath.Base(s.FilePath), filepath.Ext(s.FilePath))
	}

	sessionState.Conv = conv

	uniqueid.AssignUniqueId(conv)

	sessionState.Conv = conv

	primarykey.DetectHotspot()
	index.AssignInitialOrders()
	index.IndexSuggestion()

	sessionState.SessionMetadata = sessionMetadata
	sessionState.Driver = s.Driver
	sessionState.SessionFile = s.FilePath
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Path:           s.FilePath,
		ConnectionType: helpers.SESSION_FILE_MODE,
	}

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
// respects the parent/child ordering of interleaved tables, also foreign keys
// and secondary indexes are skipped. This means that getDDL cannot be used to
// build DDL to send to Spanner.
func getDDL(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	c := ddl.Config{Comments: true, ProtectIds: false}
	var tables []string
	for t := range sessionState.Conv.SpSchema {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	ddl := make(map[string]string)
	for _, t := range tables {
		ddl[t] = sessionState.Conv.SpSchema[t].PrintCreateTable(c)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ddl)
}

// getOverview returns the overview of conversion.
func getOverview(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
	bufWriter := bufio.NewWriter(&buf)
	sessionState := session.GetSessionState()
	internal.GenerateReport(sessionState.Driver, sessionState.Conv, bufWriter, nil, false, false)
	bufWriter.Flush()
	overview := buf.String()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(overview)
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
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(filteredTypeMap)
}

// setTypeMapGlobal allows to change Spanner type globally.
// It takes a map from source type to Spanner type and updates
// the Spanner schema accordingly.
func setTypeMapGlobal(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var typeMap map[string]string
	fmt.Println("typeMap:", typeMap)

	err = json.Unmarshal(reqBody, &typeMap)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()

	// Redo source-to-Spanner typeMap using t (the mapping specified in the http request).
	// We drive this process by iterating over the Spanner schema because we want to preserve all
	// other customizations that have been performed via the UI (dropping columns, renaming columns
	// etc). In particular, note that we can't just blindly redo schema conversion (using an appropriate
	// version of 'toDDL' with the new typeMap).
	for t, spSchema := range sessionState.Conv.SpSchema {
		for col := range spSchema.ColDefs {
			srcTable := sessionState.Conv.ToSource[t].Name
			srcCol := sessionState.Conv.ToSource[t].Cols[col]
			srcColDef := sessionState.Conv.SrcSchema[srcTable].ColDefs[srcCol]
			// If the srcCol's type is in the map, then recalculate the Spanner type
			// for this column using the map. Otherwise, leave the ColDef for this
			// column as is. Note that per-column type overrides could be lost in
			// this process -- the mapping in typeMap always takes precendence.
			if _, found := typeMap[srcColDef.Type.Name]; found {
				utilities.UpdateType(sessionState.Conv, typeMap[srcColDef.Type.Name], t, col, srcTable, w)
			}
		}
	}
	session.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

// getConversionRate returns table wise color coded conversion rate.
func getConversionRate(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	reports := internal.AnalyzeTables(sessionState.Conv, nil)
	rate := make(map[string]string)
	for _, t := range reports {
		rate[t.SpTable] = utilities.RateSchema(t.Cols, t.Warnings, t.SyntheticPKey != "")
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
	conversion.WriteSchemaFile(sessionState.Conv, now, schemaFileName, ioHelper.Out)
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
	reportFileName := "frontend/" + filePrefix + "report.txt"
	sessionState := session.GetSessionState()
	conversion.Report(sessionState.Driver, nil, ioHelper.BytesRead, "", sessionState.Conv, reportFileName, ioHelper.Out)
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
	table := r.FormValue("table")
	update := r.FormValue("update") == "true"
	sessionState := session.GetSessionState()

	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if table == "" {
		http.Error(w, fmt.Sprintf("Table name is empty"), http.StatusBadRequest)
	}
	tableInterleaveStatus := parentTableHelper(table, update)

	if tableInterleaveStatus.Possible {

		childPks := sessionState.Conv.SpSchema[table].Pks
		childindex := utilities.GetPrimaryKeyIndexFromOrder(childPks, 1)
		sessionState := session.GetSessionState()
		schemaissue := []internal.SchemaIssue{}

		column := childPks[childindex].Col
		schemaissue = sessionState.Conv.Issues[table][column]
		if update {
			schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
		} else {
			schemaissue = append(schemaissue, internal.InterleavedOrder)
		}

		sessionState.Conv.Issues[table][column] = schemaissue
	} else {
		// Remove "Table cart can be converted as Interleaved Table" suggestion from columns
		// of the table if interleaving is not possible.
		for _, column := range sessionState.Conv.SpSchema[table].ColNames {
			schemaIssue := []internal.SchemaIssue{}
			for _, v := range sessionState.Conv.Issues[table][column] {
				if v != internal.InterleavedOrder {
					schemaIssue = append(schemaIssue, v)
				}
			}
			sessionState.Conv.Issues[table][column] = schemaIssue
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

func parentTableHelper(table string, update bool) *TableInterleaveStatus {
	tableInterleaveStatus := &TableInterleaveStatus{Possible: true}
	sessionState := session.GetSessionState()

	if _, found := sessionState.Conv.SyntheticPKeys[table]; found {
		tableInterleaveStatus.Possible = false
		tableInterleaveStatus.Comment = "Has synthetic pk"
	}
	if tableInterleaveStatus.Possible {
		// Search this table's foreign keys for a suitable parent table.
		// If there are several possible parent tables, we pick the first one.
		// TODO: Allow users to pick which parent to use if more than one.
		for i, fk := range sessionState.Conv.SpSchema[table].Fks {
			refTable := fk.ReferTable

			if _, found := sessionState.Conv.SyntheticPKeys[refTable]; found {
				continue
			}

			if checkPrimaryKeyPrefix(table, refTable, fk, tableInterleaveStatus) {

				tableInterleaveStatus.Parent = refTable
				sp := sessionState.Conv.SpSchema[table]
				if update {
					usedNames := sessionState.Conv.UsedNames
					delete(usedNames, sp.Fks[i].Name)
					sp.Parent = refTable
					sp.Fks = utilities.RemoveFk(sp.Fks, i)
				}
				sessionState.Conv.SpSchema[table] = sp
				break
			}
		}
		if tableInterleaveStatus.Parent == "" {
			tableInterleaveStatus.Possible = false
			tableInterleaveStatus.Comment = "No valid prefix"
		}
	}

	parentpks := sessionState.Conv.SpSchema[tableInterleaveStatus.Parent].Pks

	childPks := sessionState.Conv.SpSchema[table].Pks

	if len(parentpks) >= 1 {

		parentindex := utilities.GetPrimaryKeyIndexFromOrder(parentpks, 1)

		childindex := utilities.GetPrimaryKeyIndexFromOrder(childPks, 1)

		if parentindex != -1 && childindex != -1 {

			if (parentpks[parentindex].Order == childPks[childindex].Order) && (parentpks[parentindex].Col == childPks[childindex].Col) {

				sessionState := session.GetSessionState()
				schemaissue := []internal.SchemaIssue{}

				column := childPks[childindex].Col
				schemaissue = sessionState.Conv.Issues[table][column]

				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)
				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)

				sessionState.Conv.Issues[table][column] = schemaissue
				tableInterleaveStatus.Possible = true

			}

			if parentpks[parentindex].Col != childPks[childindex].Col {

				tableInterleaveStatus.Possible = false

				sessionState := session.GetSessionState()

				column := parentpks[parentindex].Col

				schemaissue := []internal.SchemaIssue{}
				schemaissue = sessionState.Conv.Issues[table][column]

				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)

				schemaissue = append(schemaissue, internal.InterleavedNotInOrder)

				sessionState.Conv.Issues[table][column] = schemaissue

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

	spTableName := ""
	for _, value := range sessionState.Conv.SpSchema {
		if value.Id == tableId {
			spTableName = value.Name
			break
		}
	}
	if spTableName == "" {
		http.Error(w, fmt.Sprintf("Spanner table not found"), http.StatusBadRequest)
		return
	}

	srcTableName := ""
	for _, value := range sessionState.Conv.SrcSchema {
		if value.Id == tableId {
			srcTableName = value.Name
			break
		}
	}
	if srcTableName == "" {
		http.Error(w, fmt.Sprintf("Table not found"), http.StatusBadRequest)
		return
	}

	conv := sessionState.Conv

	if conv.SpSchema[spTableName].Parent == "" {
		http.Error(w, fmt.Sprintf("Table is not interleaved"), http.StatusBadRequest)
		return
	}
	spTable := conv.SpSchema[spTableName]

	var firstOrderPk ddl.IndexKey

	for _, pk := range spTable.Pks {
		if pk.Order == 1 {
			firstOrderPk = pk
			break
		}
	}

	spColId := conv.SpSchema[spTableName].ColDefs[firstOrderPk.Col].Id
	var srcCol schema.Column
	for _, col := range conv.SrcSchema[srcTableName].ColDefs {
		if col.Id == spColId {
			srcCol = col
			break
		}
	}
	interleavedFk, err := utilities.GetInterleavedFk(conv, srcTableName, srcCol.Name)
	if err != nil {
		http.Error(w, fmt.Sprintf("%v", err), http.StatusBadRequest)
		return
	}

	spFk, err := common.CvtForeignKeysHelper(conv, spTableName, srcTableName, interleavedFk, true)
	if err != nil {
		http.Error(w, fmt.Sprintf("Foreign key conversion fail"), http.StatusBadRequest)
		return
	}

	spFks := spTable.Fks
	spFks = append(spFks, spFk)
	spTable.Fks = spFks
	spTable.Parent = ""
	conv.SpSchema[spTableName] = spTable

	uniqueid.CopyUniqueIdToSpannerTable(conv, spTableName)

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
	tableId := r.FormValue("tableId")
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
	}

	table := ""
	for _, value := range sessionState.Conv.SrcSchema {
		if value.Id == tableId {
			table = value.Name
			break
		}
	}
	if table == "" {
		http.Error(w, fmt.Sprintf("Table not found"), http.StatusBadRequest)
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

	err := common.SrcTableToSpannerDDL(conv, toddl, sessionState.Conv.SrcSchema[table])
	if err != nil {
		http.Error(w, fmt.Sprintf("Restoring spanner table fail"), http.StatusBadRequest)
		return
	}
	conv.AddPrimaryKeys()
	for _, spTable := range conv.SpSchema {
		uniqueid.CopyUniqueIdToSpannerTable(conv, spTable.Name)
	}
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
	tableId := r.FormValue("tableId")
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
	}
	table := ""
	for _, value := range sessionState.Conv.SpSchema {
		if value.Id == tableId {
			table = value.Name
			break
		}
	}
	if table == "" {
		http.Error(w, fmt.Sprintf("Spanner table not found"), http.StatusBadRequest)
	}

	srcTable := ""
	for _, value := range sessionState.Conv.SrcSchema {
		if value.Id == tableId {
			srcTable = value.Name
			break
		}
	}
	if srcTable == "" {
		http.Error(w, fmt.Sprintf("Source table not found"), http.StatusBadRequest)
	}

	spSchema := sessionState.Conv.SpSchema
	toSource := sessionState.Conv.ToSource
	toSpanner := sessionState.Conv.ToSpanner
	issues := sessionState.Conv.Issues
	syntheticPkey := sessionState.Conv.SyntheticPKeys
	toSourceFkIdx := sessionState.Conv.Audit.ToSourceFkIdx
	toSpannerFkIdx := sessionState.Conv.Audit.ToSpannerFkIdx

	//remove deleted name from usedName
	usedNames := sessionState.Conv.UsedNames
	delete(usedNames, table)
	for _, index := range sessionState.Conv.SpSchema[table].Indexes {
		delete(usedNames, index.Name)
	}
	for _, fk := range sessionState.Conv.SpSchema[table].Fks {
		delete(usedNames, fk.Name)
	}

	delete(spSchema, table)
	delete(toSource, table)
	delete(toSpanner, srcTable)
	issues[srcTable] = map[string][]internal.SchemaIssue{}
	delete(syntheticPkey, table)
	delete(toSourceFkIdx, table)
	delete(toSpannerFkIdx, srcTable)

	//drop reference foreign key
	for tableName, spTable := range spSchema {
		fks := []ddl.Foreignkey{}
		for _, fk := range spTable.Fks {
			if fk.ReferTable != table {
				fks = append(fks, fk)
			} else {
				delete(usedNames, fk.Name)
			}

		}
		spTable.Fks = fks
		spSchema[tableName] = spTable
	}

	//remove interleave that are interleaved on the drop table as parent
	for tableName, spTable := range spSchema {
		if spTable.Parent == table {
			spTable.Parent = ""
			spSchema[tableName] = spTable
		}
	}

	//remove interleavable suggestion on droping the parent table
	for tableName, tableIssues := range issues {
		for colName, colIssues := range tableIssues {
			updatedColIssues := []internal.SchemaIssue{}
			for _, val := range colIssues {
				if val != internal.InterleavedOrder {
					updatedColIssues = append(updatedColIssues, val)
				}
			}
			if len(updatedColIssues) == 0 {
				delete(issues[tableName], colName)
			} else {
				issues[tableName][colName] = updatedColIssues
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

func dropForeignKey(w http.ResponseWriter, r *http.Request) {
	table := r.FormValue("table")
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	var dropDetail DropDetail
	if err = json.Unmarshal(reqBody, &dropDetail); err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if table == "" || dropDetail.Name == "" {
		http.Error(w, fmt.Sprintf("Table name or foreign key name is empty"), http.StatusBadRequest)
	}
	sp := sessionState.Conv.SpSchema[table]
	position := -1
	for i, fk := range sp.Fks {
		if dropDetail.Name == fk.Name {
			position = i
			break
		}
	}

	if position < 0 || position >= len(sp.Fks) {
		http.Error(w, fmt.Sprintf("No foreign key found at position %d", position), http.StatusBadRequest)
		return
	}

	// To remove the interleavable suggestions if they exist on dropping fk
	column := sp.Fks[position].Columns[0]
	schemaIssue := []internal.SchemaIssue{}
	for _, v := range sessionState.Conv.Issues[table][column] {
		if v != internal.InterleavedAddColumn && v != internal.InterleavedRenameColumn && v != internal.InterleavedNotInOrder {
			schemaIssue = append(schemaIssue, v)
		}
	}
	if _, ok := sessionState.Conv.Issues[table]; ok {
		sessionState.Conv.Issues[table][column] = schemaIssue
	}

	sp.Fks = utilities.RemoveFk(sp.Fks, position)
	sessionState.Conv.SpSchema[table] = sp
	session.UpdateSessionFile()

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

	srcTableName := ""
	for _, value := range sessionState.Conv.SrcSchema {
		if value.Id == tableId {
			srcTableName = value.Name
			break
		}
	}
	if srcTableName == "" {
		http.Error(w, fmt.Sprintf("Source Table not found"), http.StatusBadRequest)
		return
	}

	spTableName := ""
	for _, value := range sessionState.Conv.SpSchema {
		if value.Id == tableId {
			spTableName = value.Name
			break
		}
	}
	if spTableName == "" {
		http.Error(w, fmt.Sprintf("Spanner Table not found"), http.StatusBadRequest)
		return
	}

	var srcIndex schema.Index
	srcIndexFound := false
	for _, index := range sessionState.Conv.SrcSchema[srcTableName].Indexes {
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

	spIndex := common.CvtIndexHelper(conv, spTableName, srcTableName, srcIndex)
	spIndexes := conv.SpSchema[spTableName].Indexes
	spIndexes = append(spIndexes, spIndex)
	spTable := conv.SpSchema[spTableName]
	spTable.Indexes = spIndexes
	conv.SpSchema[spTableName] = spTable

	uniqueid.CopyUniqueIdToSpannerTable(conv, spTable.Name)
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
func renameForeignKeys(w http.ResponseWriter, r *http.Request) {
	table := r.FormValue("table")
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
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

	sp := sessionState.Conv.SpSchema[table]

	// Update session with renamed foreignkeys.
	newFKs := []ddl.Foreignkey{}
	for _, foreignKey := range sp.Fks {
		if newName, ok := renameMap[foreignKey.Name]; ok {
			foreignKey.Name = newName
		}
		newFKs = append(newFKs, foreignKey)
	}
	sp.Fks = newFKs

	sessionState.Conv.SpSchema[table] = sp
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
		if newName, ok := renameMap[index.Name]; ok {
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

// addIndexes checks the new names for spanner name validity, ensures the new names are already not used by existing tables
// secondary indexes or foreign key constraints. If above checks passed then new indexes are added to the schema else appropriate
// error thrown.
func addIndexes(w http.ResponseWriter, r *http.Request) {
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

	// Check new name for spanner name validity.
	newNames := []string{}
	newNamesMap := map[string]bool{}
	for _, value := range newIndexes {
		newNames = append(newNames, value.Name)
		newNamesMap[strings.ToLower(value.Name)] = true
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
	usedNames := sessionState.Conv.UsedNames
	index.CheckIndexSuggestion(newIndexes, sp)
	for i := 0; i < len(newIndexes); i++ {
		newIndexes[i].Id = uniqueid.GenerateIndexesId()
		usedNames[newIndexes[i].Name] = true
	}

	sp.Indexes = append(sp.Indexes, newIndexes...)
	sessionState.Conv.SpSchema[table] = sp
	session.UpdateSessionFile()
	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}
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
	if sessionState.Conv.Audit.StreamingStats.DataStreamName != "" {
		generatedResources.DataStreamJobName = sessionState.Conv.Audit.StreamingStats.DataStreamName
		generatedResources.DataStreamJobUrl = fmt.Sprintf("https://pantheon.corp.google.com/datastream/streams/locations/%v/instances/%v?project=%v", sessionState.Region, sessionState.Conv.Audit.StreamingStats.DataStreamName, sessionState.GCPProjectID)
	}
	if sessionState.Conv.Audit.StreamingStats.DataflowJobId != "" {
		generatedResources.DataflowJobName = sessionState.Conv.Audit.StreamingStats.DataflowJobId
		generatedResources.DataflowJobUrl = fmt.Sprintf("https://pantheon.corp.google.com/dataflow/jobs/%v/%v?project=%v", sessionState.Region, sessionState.Conv.Audit.StreamingStats.DataflowJobId, sessionState.GCPProjectID)
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
	} else {
		sourceProfileString = fmt.Sprintf("host=%v,port=%v,user=%v,password=%v,dbName=%v",
			sourceDBConnectionDetails.Host, sourceDBConnectionDetails.Port, sourceDBConnectionDetails.User,
			sourceDBConnectionDetails.Password, sessionState.DbName)
	}
	sessionState.SpannerDatabaseName = details.TargetDetails.TargetDB
	targetProfileString := fmt.Sprintf("project=%v,instance=%v,dbName=%v", sessionState.GCPProjectID, sessionState.SpannerInstanceID, details.TargetDetails.TargetDB)
	if details.MigrationType == helpers.LOW_DOWNTIME_MIGRATION {
		fileName := sessionState.Conv.Audit.MigrationRequestId + "-streaming.json"
		sessionState.Bucket, sessionState.RootPath, err = profile.GetBucket(sessionState.GCPProjectID, sessionState.Region, details.TargetDetails.TargetConnectionProfileName)
		if err != nil {
			return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", fmt.Errorf("error while getting target bucket: %v", err)
		}
		err = createStreamingCfgFile(sessionState, details.TargetDetails, fileName)
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
	targetProfile.TargetDb = targetProfile.ToLegacyTargetDb()
	return sourceProfile, targetProfile, ioHelper, dbName, nil
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

func createStreamingCfgFile(sessionState *session.SessionState, targetDetails targetDetails, fileName string) error {
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
			JobName:  "",
			Location: sessionState.Region,
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

		if ind.Table == newIndexes[0].Table && ind.Name == newIndexes[0].Name {

			index.RemoveIndexIssues(table, sp.Indexes[i])

			sp.Indexes[i].Keys = newIndexes[0].Keys
			sp.Indexes[i].Name = newIndexes[0].Name
			sp.Indexes[i].Table = newIndexes[0].Table
			sp.Indexes[i].Unique = newIndexes[0].Unique

			break
		}
	}

	for i, spIndex := range sp.Indexes {

		for j, srcIndex := range st.Indexes {

			for k, spIndexKey := range spIndex.Keys {

				for l, srcIndexKey := range srcIndex.Keys {

					if srcIndexKey.Column == spIndexKey.Col {

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

	var dropDetail DropDetail
	if err = json.Unmarshal(reqBody, &dropDetail); err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if table == "" || dropDetail.Name == "" {
		http.Error(w, fmt.Sprintf("Table name or position is empty"), http.StatusBadRequest)
	}
	sp := sessionState.Conv.SpSchema[table]
	position := -1
	for i, index := range sp.Indexes {
		if dropDetail.Name == index.Name {
			position = i
			break
		}
	}
	if position < 0 || position >= len(sp.Indexes) {
		http.Error(w, fmt.Sprintf("No secondary index found at position %d", position), http.StatusBadRequest)
		return
	}

	usedNames := sessionState.Conv.UsedNames
	delete(usedNames, sp.Indexes[position].Name)
	index.RemoveIndexIssues(table, sp.Indexes[position])

	sp.Indexes = utilities.RemoveSecondaryIndex(sp.Indexes, position)
	sessionState.Conv.SpSchema[table] = sp
	session.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
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
	sessionState.Conv.TargetDb = constants.TargetSpanner
	err2 := conversion.ReadSessionFile(sessionState.Conv, sessionState.SessionFile)
	if err2 != nil {
		return fmt.Errorf("encountered error %w. rollback failed: %v", err, err2)
	}
	return err
}

func checkPrimaryKeyPrefix(table string, refTable string, fk ddl.Foreignkey, tableInterleaveStatus *TableInterleaveStatus) bool {

	sessionState := session.GetSessionState()
	childPks := sessionState.Conv.SpSchema[table].Pks
	parentPks := sessionState.Conv.SpSchema[refTable].Pks

	childPkCols := []string{}
	for _, k := range childPks {
		childPkCols = append(childPkCols, k.Col)
	}

	interleaved := []ddl.IndexKey{}

	for i := 0; i < len(parentPks); i++ {

		for j := 0; j < len(childPks); j++ {

			for k := 0; k < len(fk.ReferColumns); k++ {

				if parentPks[i].Col == childPks[j].Col && parentPks[i].Col == fk.ReferColumns[k] && childPks[j].Col == fk.ReferColumns[k] {

					interleaved = append(interleaved, parentPks[i])
				}
			}

		}

	}

	diff := []ddl.IndexKey{}

	if len(interleaved) == 0 {

		for i := 0; i < len(parentPks); i++ {

			for j := 0; j < len(childPks); j++ {

				if parentPks[i].Col != childPks[j].Col {

					diff = append(diff, parentPks[i])
				}

			}
		}

	}

	canInterleavedOnAdd := []string{}
	canInterleavedOnRename := []string{}
	for i := 0; i < len(diff); i++ {

		parentColIndex := utilities.IsColumnPresent(fk.ReferColumns, diff[i].Col)
		if parentColIndex == -1 {
			continue
		}
		childColIndex := utilities.IsColumnPresent(childPkCols, fk.Columns[parentColIndex])
		if childColIndex == -1 {
			canInterleavedOnAdd = append(canInterleavedOnAdd, fk.Columns[parentColIndex])
		} else {
			canInterleavedOnRename = append(canInterleavedOnRename, fk.Columns[parentColIndex])
		}
	}

	if len(canInterleavedOnRename) > 0 {
		updateInterleaveSuggestion(canInterleavedOnRename, table, internal.InterleavedRenameColumn)
	} else if len(canInterleavedOnAdd) > 0 {
		updateInterleaveSuggestion(canInterleavedOnAdd, table, internal.InterleavedAddColumn)
	}

	if len(interleaved) > 0 {
		return true
	}

	return false
}

func updateInterleaveSuggestion(columns []string, table string, issue internal.SchemaIssue) {
	for i := 0; i < len(columns); i++ {

		sessionState := session.GetSessionState()

		schemaissue := []internal.SchemaIssue{}

		schemaissue = sessionState.Conv.Issues[table][columns[i]]

		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)

		schemaissue = append(schemaissue, issue)

		if len(schemaissue) > 0 {

			if sessionState.Conv.Issues[table] == nil {

				s := map[string][]internal.SchemaIssue{
					columns[i]: schemaissue,
				}
				sessionState.Conv.Issues[table] = s
			} else {
				sessionState.Conv.Issues[table][columns[i]] = schemaissue
			}
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
	T     string
	Brief string
}

type GeneratedResources struct {
	DatabaseName      string
	DatabaseUrl       string
	BucketName        string
	BucketUrl         string
	DataStreamJobName string
	DataStreamJobUrl  string
	DataflowJobName   string
	DataflowJobUrl    string
}

func addTypeToList(convertedType string, spType string, issues []internal.SchemaIssue, l []typeIssue) []typeIssue {
	if convertedType == spType {
		if len(issues) > 0 {
			var briefs []string
			for _, issue := range issues {
				briefs = append(briefs, internal.IssueDB[issue].Brief)
			}
			l = append(l, typeIssue{T: spType, Brief: fmt.Sprintf(strings.Join(briefs, ", "))})
		} else {
			l = append(l, typeIssue{T: spType})
		}
	}
	return l
}

func init() {
	sessionState := session.GetSessionState()

	uniqueid.InitObjectId()

	// Initialize mysqlTypeMap.
	for _, srcType := range []string{"bool", "boolean", "varchar", "char", "text", "tinytext", "mediumtext", "longtext", "set", "enum", "json", "bit", "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "double", "float", "numeric", "decimal", "date", "datetime", "timestamp", "time", "year", "geometrycollection", "multipoint", "multilinestring", "multipolygon", "point", "linestring", "polygon", "geometry"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			ty, issues := typemap.ToSpannerTypeMySQL(srcType, spType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		if srcType == "tinyint" {
			l = append(l, typeIssue{T: ddl.Bool, Brief: "Only tinyint(1) can be converted to BOOL, for any other mods it will be converted to INT64"})
		}
		mysqlTypeMap[srcType] = l
	}
	// Initialize postgresTypeMap.
	for _, srcType := range []string{"bool", "boolean", "bigserial", "bpchar", "character", "bytea", "date", "float8", "double precision", "float4", "real", "int8", "bigint", "int4", "integer", "int2", "smallint", "numeric", "serial", "text", "timestamptz", "timestamp with time zone", "timestamp", "timestamp without time zone", "varchar", "character varying"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			ty, issues := typemap.ToSpannerTypePostgres(srcType, spType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		postgresTypeMap[srcType] = l
	}

	// Initialize sqlserverTypeMap.
	for _, srcType := range []string{"int", "tinyint", "smallint", "bigint", "bit", "float", "real", "numeric", "decimal", "money", "smallmoney", "char", "nchar", "varchar", "nvarchar", "text", "ntext", "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp", "rowversion", "binary", "varbinary", "image", "xml", "geography", "geometry", "uniqueidentifier", "sql_variant", "hierarchyid"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			ty, issues := typemap.ToSpannerTypeSQLserver(srcType, spType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		sqlserverTypeMap[srcType] = l
	}

	// Initialize oracleTypeMap.
	for _, srcType := range []string{"NUMBER", "BFILE", "BLOB", "CHAR", "CLOB", "DATE", "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT", "LONG", "RAW", "LONG RAW", "NCHAR", "NVARCHAR2", "VARCHAR", "VARCHAR2", "NCLOB", "ROWID", "UROWID", "XMLTYPE", "TIMESTAMP", "INTERVAL", "SDO_GEOMETRY"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			ty, issues := oracle.ToSpannerTypeWeb(sessionState.Conv, spType, srcType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		oracleTypeMap[srcType] = l
	}

	sessionState.Conv = internal.MakeConv()
	config := config.TryInitializeSpannerConfig()
	session.SetSessionStorageConnectionState(config.GCPProjectID, config.SpannerInstanceID)
}

// App connects to the web app v2.
func App(logLevel string) {
	err := logger.InitializeLogger(logLevel)
	if err != nil {
		log.Fatal("Error initialising webapp, did you specify a valid log-level? [DEBUG, INFO, WARN, ERROR, FATAL]")
	}
	addr := ":8080"
	router := getRoutes()
	fmt.Println("Harbourbridge UI started at:", fmt.Sprintf("http://localhost%s", addr))
	log.Fatal(http.ListenAndServe(addr, handlers.CORS(handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}), handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS"}), handlers.AllowedOrigins([]string{"*"}))(router)))
}
