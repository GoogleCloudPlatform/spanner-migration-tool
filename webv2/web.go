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

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/sources/mysql"
	"github.com/cloudspannerecosystem/harbourbridge/sources/oracle"
	"github.com/cloudspannerecosystem/harbourbridge/sources/postgres"
	"github.com/cloudspannerecosystem/harbourbridge/sources/sqlserver"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/config"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/typemap"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/handlers"

	index "github.com/cloudspannerecosystem/harbourbridge/webv2/index"
	primarykey "github.com/cloudspannerecosystem/harbourbridge/webv2/primarykey"

	uniqueid "github.com/cloudspannerecosystem/harbourbridge/webv2/uniqueid"

	updatesessionfiles "github.com/cloudspannerecosystem/harbourbridge/webv2/updatesessionfiles"

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
	// schema and user is same in oralce.
	if config.Driver == constants.ORACLE {
		sessionState.DbName = config.User
	}
	sessionState.Driver = config.Driver
	sessionState.SessionFile = ""
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
	f, err := os.Open(dc.FilePath)
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
	index.IndexSuggestion()

	sessionState.SessionMetadata = sessionMetadata
	sessionState.Driver = dc.Driver
	sessionState.DbName = ""
	sessionState.SessionFile = ""
	sessionState.SourceDB = nil

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
	err = conversion.ReadSessionFile(conv, s.FilePath)
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
		DatabaseName: strings.TrimRight(filepath.Base(s.FilePath), filepath.Ext(s.FilePath)),
	}

	sessionState.Conv = conv

	uniqueid.AssignUniqueId(conv)

	sessionState.Conv = conv

	primarykey.DetectHotspot()
	index.IndexSuggestion()

	sessionState.SessionMetadata = sessionMetadata
	sessionState.Driver = s.Driver
	sessionState.SessionFile = s.FilePath

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionMetadata,
		Conv:            *conv,
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
				utilities.UpdateType(typeMap[srcColDef.Type.Name], t, col, srcTable, w)
			}
		}
	}
	updatesessionfiles.UpdateSessionFile()

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

	index.IndexSuggestion()
	updatesessionfiles.UpdateSessionFile()
	w.WriteHeader(http.StatusOK)

	if update {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tableInterleaveStatus": tableInterleaveStatus,
			"sessionState":          sessionState.Conv})
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
				sp.Parent = refTable
				if update {
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
				schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)

				schemaissue = append(schemaissue, internal.InterleavedOrder)

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

				schemaissue = append(schemaissue, internal.InterleavedNotInOrder)

				sessionState.Conv.Issues[table][column] = schemaissue

			}

		}

	}

	return tableInterleaveStatus
}

func restoreTable(w http.ResponseWriter, r *http.Request) {
	table := r.FormValue("table")
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if table == "" {
		http.Error(w, fmt.Sprintf("Table name is empty"), http.StatusBadRequest)
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

	uniqueid.CopyUniqueIdToSpannerTable(conv, conv.ToSpanner[table].Name)
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
	table := r.FormValue("table")
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if table == "" {
		http.Error(w, fmt.Sprintf("Table name is empty"), http.StatusBadRequest)
	}
	spSchema := sessionState.Conv.SpSchema
	toSource := sessionState.Conv.ToSource
	toSpanner := sessionState.Conv.ToSpanner
	issues := sessionState.Conv.Issues

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
	delete(toSpanner, table)
	delete(issues, table)

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

	sessionState.Conv.SpSchema = spSchema
	sessionState.Conv.ToSource = toSource
	sessionState.Conv.ToSpanner = toSource
	sessionState.Conv.Issues = issues
	sessionState.Conv.UsedNames = usedNames

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
	sp.Fks = utilities.RemoveFk(sp.Fks, position)
	sessionState.Conv.SpSchema[table] = sp
	updatesessionfiles.UpdateSessionFile()

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
	updatesessionfiles.UpdateSessionFile()

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
	updatesessionfiles.UpdateSessionFile()
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

	index.CheckIndexSuggestion(newIndexes, sp)
	for i := 0; i < len(newIndexes); i++ {
		newIndexes[i].Id = uniqueid.GenerateIndexesId()
	}

	sp.Indexes = append(sp.Indexes, newIndexes...)

	sessionState.Conv.SpSchema[table] = sp
	updatesessionfiles.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
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

	sessionState := session.GetSessionState()
	sp := sessionState.Conv.SpSchema[table]

	st := sessionState.Conv.SrcSchema[table]

	for i, index := range sp.Indexes {

		if index.Table == newIndexes[0].Table && index.Name == newIndexes[0].Name {

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

	updatesessionfiles.UpdateSessionFile()

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

	index.RemoveIndexIssues(table, sp.Indexes[position])

	sp.Indexes = utilities.RemoveSecondaryIndex(sp.Indexes, position)
	sessionState.Conv.SpSchema[table] = sp
	updatesessionfiles.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
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

	caninterleaved := []string{}
	for i := 0; i < len(diff); i++ {

		str := utilities.IsColumnPresent(fk.ReferColumns, diff[i].Col)

		caninterleaved = append(caninterleaved, str)
	}

	if len(caninterleaved) > 0 {

		for i := 0; i < len(caninterleaved); i++ {

			sessionState := session.GetSessionState()

			schemaissue := []internal.SchemaIssue{}

			schemaissue = sessionState.Conv.Issues[table][caninterleaved[i]]

			schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
			schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
			schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)

			schemaissue = append(schemaissue, internal.InterleavedAddColumn)

			if len(schemaissue) > 0 {

				if sessionState.Conv.Issues[table][caninterleaved[i]] == nil {

					s := map[string][]internal.SchemaIssue{
						caninterleaved[i]: schemaissue,
					}
					sessionState.Conv.Issues = map[string]map[string][]internal.SchemaIssue{}

					sessionState.Conv.Issues[table] = s

				} else {
					sessionState.Conv.Issues[table][caninterleaved[i]] = schemaissue
				}

			}

		}
	}

	if len(interleaved) > 0 {
		return true
	}

	return false
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
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric} {
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
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric} {
			ty, issues := typemap.ToSpannerTypePostgres(srcType, spType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		postgresTypeMap[srcType] = l
	}

	// Initialize sqlserverTypeMap.
	for _, srcType := range []string{"int", "tinyint", "smallint", "bigint", "bit", "float", "real", "numeric", "decimal", "money", "smallmoney", "char", "nchar", "varchar", "nvarchar", "text", "ntext", "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp", "rowversion", "binary", "varbinary", "image", "xml", "geography", "geometry", "uniqueidentifier", "sql_variant", "hierarchyid"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric} {
			ty, issues := typemap.ToSpannerTypeSQLserver(srcType, spType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		sqlserverTypeMap[srcType] = l
	}

	// Initialize oracleTypeMap.
	for _, srcType := range []string{"NUMBER", "BFILE", "BLOB", "CHAR", "CLOB", "DATE", "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT", "LONG", "RAW", "LONG RAW", "NCHAR", "NVARCHAR2", "VARCHAR", "VARCHAR2", "NCLOB", "ROWID", "UROWID", "XMLTYPE", "TIMESTAMP", "INTERVAL", "SDO_GEOMETRY"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric} {
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
func App() {
	addr := ":8080"
	router := getRoutes()
	log.Printf("Starting server at port 8080\n")
	log.Fatal(http.ListenAndServe(addr, handlers.CORS(handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}), handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS"}), handlers.AllowedOrigins([]string{"*"}))(router)))
}
