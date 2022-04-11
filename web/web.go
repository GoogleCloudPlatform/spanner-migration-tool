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

// Package web defines web APIs to be used with harbourbridge frontend.
// Apart from schema conversion, this package involves API to update
// converted schema.
package web

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
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
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/handlers"
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
		http.Error(w, fmt.Sprintf("SQL connection error : %v", err), http.StatusInternalServerError)
		return
	}
	// Open doesn't open a connection. Validate database connection.
	err = sourceDB.Ping()
	if err != nil {
		http.Error(w, fmt.Sprintf("Connection Error: %v. Check Configuration again.", err), http.StatusInternalServerError)
		return
	}
	sessionState.sourceDB = sourceDB
	sessionState.dbName = config.Database
	// schema and user is same in oralce.
	if config.Driver == constants.ORACLE {
		sessionState.dbName = config.User
	}
	sessionState.driver = config.Driver
	sessionState.sessionFile = ""
	w.WriteHeader(http.StatusOK)
}

// convertSchemaSQL converts source database to Spanner when using
// with postgres and mysql driver.
func convertSchemaSQL(w http.ResponseWriter, r *http.Request) {
	if sessionState.sourceDB == nil || sessionState.dbName == "" || sessionState.driver == "" {
		http.Error(w, fmt.Sprintf("Database is not configured or Database connection is lost. Please set configuration and connect to database."), http.StatusNotFound)
		return
	}
	conv := internal.MakeConv()
	// Setting target db to spanner by default.
	conv.TargetDb = constants.TargetSpanner
	var err error
	switch sessionState.driver {
	case constants.MYSQL:
		err = common.ProcessSchema(conv, mysql.InfoSchemaImpl{DbName: sessionState.dbName, Db: sessionState.sourceDB})
	case constants.POSTGRES:
		err = common.ProcessSchema(conv, postgres.InfoSchemaImpl{Db: sessionState.sourceDB})
	case constants.SQLSERVER:
		err = common.ProcessSchema(conv, sqlserver.InfoSchemaImpl{DbName: sessionState.dbName, Db: sessionState.sourceDB})
	case constants.ORACLE:
		err = common.ProcessSchema(conv, oracle.InfoSchemaImpl{DbName: strings.ToUpper(sessionState.dbName), Db: sessionState.sourceDB})
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", sessionState.driver), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Schema Conversion Error : %v", err), http.StatusNotFound)
		return
	}
	sessionState.conv = conv
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(conv)
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
		http.Error(w, fmt.Sprintf("failed to open dump file %v : %v", dc.FilePath, err), http.StatusNotFound)
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
	sessionState.conv = conv
	sessionState.driver = dc.Driver
	sessionState.dbName = ""
	sessionState.sessionFile = ""
	sessionState.sourceDB = nil
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(conv)
}

// getDDL returns the Spanner DDL for each table in alphabetical order.
// Unlike internal/convert.go's GetDDL, it does not print tables in a way that
// respects the parent/child ordering of interleaved tables, also foreign keys
// and secondary indexes are skipped. This means that getDDL cannot be used to
// build DDL to send to Spanner.
func getDDL(w http.ResponseWriter, r *http.Request) {
	c := ddl.Config{Comments: true, ProtectIds: false}
	var tables []string
	for t := range sessionState.conv.SpSchema {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	ddl := make(map[string]string)
	for _, t := range tables {
		ddl[t] = sessionState.conv.SpSchema[t].PrintCreateTable(c)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ddl)
}

// getSummary returns table wise summary of conversion.
func getSummary(w http.ResponseWriter, r *http.Request) {
	reports := internal.AnalyzeTables(sessionState.conv, nil)
	summary := make(map[string]string)
	for _, t := range reports {
		var body strings.Builder
		for _, x := range t.Body {
			body.WriteString(x.Heading + "\n")
			for i, l := range x.Lines {
				body.WriteString(fmt.Sprintf("%d) %s.\n\n", i+1, l))
			}
		}
		summary[t.SrcTable] = body.String()
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(summary)
}

// getOverview returns the overview of conversion.
func getOverview(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
	bufWriter := bufio.NewWriter(&buf)
	internal.GenerateReport(sessionState.driver, sessionState.conv, bufWriter, nil, false, false)
	bufWriter.Flush()
	overview := buf.String()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(overview)
}

// getTypeMap returns the source to Spanner typemap only for the
// source types used in current conversion.
func getTypeMap(w http.ResponseWriter, r *http.Request) {
	if sessionState.conv == nil || sessionState.driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	var typeMap map[string][]typeIssue
	switch sessionState.driver {
	case constants.MYSQL, constants.MYSQLDUMP:
		typeMap = mysqlTypeMap
	case constants.POSTGRES, constants.PGDUMP:
		typeMap = postgresTypeMap
	case constants.SQLSERVER:
		typeMap = sqlserverTypeMap
	case constants.ORACLE:
		typeMap = oracleTypeMap
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", sessionState.driver), http.StatusBadRequest)
		return
	}
	// Filter typeMap so it contains just the types SrcSchema uses.
	filteredTypeMap := make(map[string][]typeIssue)
	for _, srcTable := range sessionState.conv.SrcSchema {
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
	err = json.Unmarshal(reqBody, &typeMap)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	// Redo source-to-Spanner typeMap using t (the mapping specified in the http request).
	// We drive this process by iterating over the Spanner schema because we want to preserve all
	// other customizations that have been performed via the UI (dropping columns, renaming columns
	// etc). In particular, note that we can't just blindly redo schema conversion (using an appropriate
	// version of 'toDDL' with the new typeMap).
	for t, spSchema := range sessionState.conv.SpSchema {
		for col, _ := range spSchema.ColDefs {
			srcTable := sessionState.conv.ToSource[t].Name
			srcCol := sessionState.conv.ToSource[t].Cols[col]
			srcColDef := sessionState.conv.SrcSchema[srcTable].ColDefs[srcCol]
			// If the srcCol's type is in the map, then recalculate the Spanner type
			// for this column using the map. Otherwise, leave the ColDef for this
			// column as is. Note that per-column type overrides could be lost in
			// this process -- the mapping in typeMap always takes precendence.
			if _, found := typeMap[srcColDef.Type.Name]; found {
				updateType(typeMap[srcColDef.Type.Name], t, col, srcTable, w)
			}
		}
	}
	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionState.conv)
}

// Actions to be performed on a column.
// (1) Removed: true/false
// (2) Rename: New name or empty string
// (3) PK: "ADDED", "REMOVED" or ""
// (4) NotNull: "ADDED", "REMOVED" or ""
// (5) ToType: New type or empty string
type updateCol struct {
	Removed bool   `json:"Removed"`
	Rename  string `json:"Rename"`
	PK      string `json:"PK"`
	NotNull string `json:"NotNull"`
	ToType  string `json:"ToType"`
}

type updateTable struct {
	UpdateCols map[string]updateCol `json:"UpdateCols"`
}

// updateTableSchema updates the Spanner schema.
// Following actions can be performed on a specified table:
// (1) Remove column
// (2) Rename column
// (3) Add or Remove Primary Key
// (4) Add or Remove NotNull constraint
// (5) Update Spanner type
func updateTableSchema(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var t updateTable
	table := r.FormValue("table")
	err = json.Unmarshal(reqBody, &t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	srcTableName := sessionState.conv.ToSource[table].Name
	for colName, v := range t.UpdateCols {
		if v.Removed {
			status, err := canRemoveColumn(colName, table)
			if err != nil {
				err = rollback(err)
				http.Error(w, fmt.Sprintf("%v", err), status)
				return
			}
			removeColumn(table, colName, srcTableName)
			continue
		}
		if v.Rename != "" && v.Rename != colName {
			if status, err := canRenameOrChangeType(colName, table); err != nil {
				err = rollback(err)
				http.Error(w, fmt.Sprintf("%v", err), status)
				return
			}
			renameColumn(v.Rename, table, colName, srcTableName)
			colName = v.Rename
		}
		if v.PK != "" {
			http.Error(w, "HarbourBridge currently doesn't support editing primary keys", http.StatusNotImplemented)
			return
		}

		if v.ToType != "" {
			typeChange, err := isTypeChanged(v.ToType, table, colName, srcTableName)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {
				if status, err := canRenameOrChangeType(colName, table); err != nil {
					err = rollback(err)
					http.Error(w, fmt.Sprintf("%v", err), status)
					return
				}
				updateType(v.ToType, table, colName, srcTableName, w)
			}
		}
		if v.NotNull != "" {
			updateNotNull(v.NotNull, table, colName)
		}
	}
	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionState.conv)
}

// getConversionRate returns table wise color coded conversion rate.
func getConversionRate(w http.ResponseWriter, r *http.Request) {
	reports := internal.AnalyzeTables(sessionState.conv, nil)
	rate := make(map[string]string)
	for _, t := range reports {
		rate[t.SpTable] = rateSchema(t.Cols, t.Warnings, t.SyntheticPKey != "")
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(rate)
}

// getSchemaFile generates schema file and returns file path.
func getSchemaFile(w http.ResponseWriter, r *http.Request) {
	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	var err error
	now := time.Now()
	filePrefix, err := getFilePrefix(now)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not get file prefix : %v", err), http.StatusInternalServerError)
	}
	schemaFileName := "frontend/" + filePrefix + "schema.txt"
	conversion.WriteSchemaFile(sessionState.conv, now, schemaFileName, ioHelper.Out)
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
	filePrefix, err := getFilePrefix(now)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not get file prefix : %v", err), http.StatusInternalServerError)
	}
	reportFileName := "frontend/" + filePrefix + "report.txt"
	conversion.Report(sessionState.driver, nil, ioHelper.BytesRead, "", sessionState.conv, reportFileName, ioHelper.Out)
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
	if sessionState.conv == nil || sessionState.driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if table == "" {
		http.Error(w, fmt.Sprintf("Table name is empty"), http.StatusBadRequest)
	}
	tableInterleaveStatus := parentTableHelper(table, update)
	updateSessionFile()
	w.WriteHeader(http.StatusOK)

	if update {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tableInterleaveStatus": tableInterleaveStatus,
			"sessionState":          sessionState.conv})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tableInterleaveStatus": tableInterleaveStatus,
		})
	}
}

func parentTableHelper(table string, update bool) *TableInterleaveStatus {
	tableInterleaveStatus := &TableInterleaveStatus{Possible: true}
	if _, found := sessionState.conv.SyntheticPKeys[table]; found {
		tableInterleaveStatus.Possible = false
		tableInterleaveStatus.Comment = "Has synthetic pk"
	}
	if tableInterleaveStatus.Possible {
		// Search this table's foreign keys for a suitable parent table.
		// If there are several possible parent tables, we pick the first one.
		// TODO: Allow users to pick which parent to use if more than one.
		for i, fk := range sessionState.conv.SpSchema[table].Fks {
			refTable := fk.ReferTable
			if _, found := sessionState.conv.SyntheticPKeys[refTable]; found {
				continue
			}

			if checkPrimaryKeyPrefix(table, refTable, fk, tableInterleaveStatus) {
				tableInterleaveStatus.Parent = refTable
				if update {
					sp := sessionState.conv.SpSchema[table]
					sp.Parent = refTable
					sp.Fks = removeFk(sp.Fks, i)
					sessionState.conv.SpSchema[table] = sp
				}
				break
			}
		}
		if tableInterleaveStatus.Parent == "" {
			tableInterleaveStatus.Possible = false
			tableInterleaveStatus.Comment = "No valid prefix"
		}
	}
	return tableInterleaveStatus
}

func dropForeignKey(w http.ResponseWriter, r *http.Request) {
	table := r.FormValue("table")
	pos := r.FormValue("pos")
	if sessionState.conv == nil || sessionState.driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if table == "" || pos == "" {
		http.Error(w, fmt.Sprintf("Table name or position is empty"), http.StatusBadRequest)
	}
	sp := sessionState.conv.SpSchema[table]
	position, err := strconv.Atoi(pos)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error converting position to integer"), http.StatusBadRequest)
		return
	}
	if position < 0 || position >= len(sp.Fks) {
		http.Error(w, fmt.Sprintf("No foreign key found at position %d", position), http.StatusBadRequest)
		return
	}
	sp.Fks = removeFk(sp.Fks, position)
	sessionState.conv.SpSchema[table] = sp
	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionState.conv)
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

	if sessionState.conv == nil || sessionState.driver == "" {
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

	if ok, invalidNames := checkSpannerNamesValidity(newNames); !ok {
		http.Error(w, fmt.Sprintf("Following names are not valid Spanner identifiers: %s", strings.Join(invalidNames, ",")), http.StatusBadRequest)
		return
	}

	// Check that the new names are not already used by existing tables, secondary indexes or foreign key constraints.
	if ok, err := canRename(newNames, table); !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sp := sessionState.conv.SpSchema[table]

	// Update session with renamed foreignkeys.
	newFKs := []ddl.Foreignkey{}
	for _, foreignKey := range sp.Fks {
		if newName, ok := renameMap[foreignKey.Name]; ok {
			foreignKey.Name = newName
		}
		newFKs = append(newFKs, foreignKey)
	}
	sp.Fks = newFKs

	sessionState.conv.SpSchema[table] = sp
	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionState.conv)
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

	if ok, invalidNames := checkSpannerNamesValidity(newNames); !ok {
		http.Error(w, fmt.Sprintf("Following names are not valid Spanner identifiers: %s", strings.Join(invalidNames, ",")), http.StatusBadRequest)
		return
	}

	// Check that the new names are not already used by existing tables, secondary indexes or foreign key constraints.
	if ok, err := canRename(newNames, table); !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sp := sessionState.conv.SpSchema[table]

	// Update session with renamed secondary indexes.
	newIndexes := []ddl.CreateIndex{}
	for _, index := range sp.Indexes {
		if newName, ok := renameMap[index.Name]; ok {
			index.Name = newName
		}
		newIndexes = append(newIndexes, index)
	}
	sp.Indexes = newIndexes

	sessionState.conv.SpSchema[table] = sp
	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionState.conv)
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
	if ok, invalidNames := checkSpannerNamesValidity(newNames); !ok {
		http.Error(w, fmt.Sprintf("Following names are not valid Spanner identifiers: %s", strings.Join(invalidNames, ",")), http.StatusBadRequest)
		return
	}

	// Check that the new names are not already used by existing tables, secondary indexes or foreign key constraints.
	if ok, err := canRename(newNames, table); !ok {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sp := sessionState.conv.SpSchema[table]
	sp.Indexes = append(sp.Indexes, newIndexes...)

	sessionState.conv.SpSchema[table] = sp
	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionState.conv)
}

func checkSpannerNamesValidity(input []string) (bool, []string) {
	status := true
	var invalidNewNames []string
	for _, changed := range input {
		if _, status := internal.FixName(changed); status {
			status = false
			invalidNewNames = append(invalidNewNames, changed)
		}
	}
	return status, invalidNewNames
}

func canRename(names []string, table string) (bool, error) {
	namesMap := map[string]bool{}
	// Check that this name isn't already used by another table.
	for _, name := range names {
		namesMap[name] = true
		if _, ok := sessionState.conv.SpSchema[name]; ok {
			return false, fmt.Errorf("new name : '%s' is used by another table", name)
		}
	}

	// Check that this name isn't already used by another foreign key.
	for _, sp := range sessionState.conv.SpSchema {
		for _, foreignKey := range sp.Fks {
			if _, ok := namesMap[foreignKey.Name]; ok {
				return false, fmt.Errorf("new name : '%s' is used by another foreign key in table : '%s'", foreignKey.Name, sp.Name)
			}

		}
	}

	// Check that this name isn't already used by another secondary index.
	for _, sp := range sessionState.conv.SpSchema {
		for _, index := range sp.Indexes {
			if _, ok := namesMap[index.Name]; ok {
				return false, fmt.Errorf("new name : '%s' is used by another index in table : '%s'", index.Name, sp.Name)
			}
		}
	}
	return true, nil
}

func dropSecondaryIndex(w http.ResponseWriter, r *http.Request) {
	table := r.FormValue("table")
	pos := r.FormValue("pos")
	if sessionState.conv == nil || sessionState.driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	if table == "" || pos == "" {
		http.Error(w, fmt.Sprintf("Table name or position is empty"), http.StatusBadRequest)
	}
	sp := sessionState.conv.SpSchema[table]
	position, err := strconv.Atoi(pos)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error converting position to integer"), http.StatusBadRequest)
		return
	}
	if position < 0 || position >= len(sp.Indexes) {
		http.Error(w, fmt.Sprintf("No secondary index found at position %d", position), http.StatusBadRequest)
		return
	}
	sp.Indexes = removeSecondaryIndex(sp.Indexes, position)
	sessionState.conv.SpSchema[table] = sp
	updateSessionFile()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sessionState.conv)
}

// updateSessionFile updates the content of session file with
// latest sessionState.conv while also dumping schemas and report.
func updateSessionFile() error {
	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	_, err := conversion.WriteConvGeneratedFiles(sessionState.conv, sessionState.dbName, sessionState.driver, ioHelper.BytesRead, ioHelper.Out)
	if err != nil {
		return fmt.Errorf("encountered error %w. Cannot write files", err)
	}
	return nil
}

// rollback is used to get previous state of conversion in case
// some unexpected error occurs during update operations.
func rollback(err error) error {
	if sessionState.sessionFile == "" {
		return fmt.Errorf("encountered error %w. rollback failed because we don't have a session file", err)
	}
	sessionState.conv = internal.MakeConv()
	sessionState.conv.TargetDb = constants.TargetSpanner
	err2 := conversion.ReadSessionFile(sessionState.conv, sessionState.sessionFile)
	if err2 != nil {
		return fmt.Errorf("encountered error %w. rollback failed: %v", err, err2)
	}
	return err
}

func isPartOfPK(col, table string) bool {
	for _, pk := range sessionState.conv.SpSchema[table].Pks {
		if pk.Col == col {
			return true
		}
	}
	return false
}

func isParent(table string) (bool, string) {
	for _, spSchema := range sessionState.conv.SpSchema {
		if spSchema.Parent == table {
			return true, spSchema.Name
		}
	}
	return false, ""
}

func isPartOfSecondaryIndex(col, table string) (bool, string) {
	for _, index := range sessionState.conv.SpSchema[table].Indexes {
		for _, key := range index.Keys {
			if key.Col == col {
				return true, index.Name
			}
		}
	}
	return false, ""
}

func isPartOfFK(col, table string) bool {
	for _, fk := range sessionState.conv.SpSchema[table].Fks {
		for _, column := range fk.Columns {
			if column == col {
				return true
			}
		}
	}
	return false
}

// TODO: create a map to store referenced column to get
// this information in O(1).
// TODO:(searce) can have foreign key constraints between columns of the same table, as well as between same column on a given table.
func isReferencedByFK(col, table string) (bool, string) {
	for _, spSchema := range sessionState.conv.SpSchema {
		if table != spSchema.Name {
			for _, fk := range spSchema.Fks {
				if fk.ReferTable == table {
					for _, column := range fk.ReferColumns {
						if column == col {
							return true, spSchema.Name
						}
					}
				}
			}
		}
	}
	return false, ""
}

func canRemoveColumn(colName, table string) (int, error) {
	if isPartOfPK := isPartOfPK(colName, table); isPartOfPK {
		return http.StatusBadRequest, fmt.Errorf("column is part of primary key")
	}
	if isPartOfSecondaryIndex, _ := isPartOfSecondaryIndex(colName, table); isPartOfSecondaryIndex {
		return http.StatusPreconditionFailed, fmt.Errorf("column is part of secondary index, remove secondary index before making the update")
	}
	isPartOfFK := isPartOfFK(colName, table)
	isReferencedByFK, _ := isReferencedByFK(colName, table)
	if isPartOfFK || isReferencedByFK {
		return http.StatusPreconditionFailed, fmt.Errorf("column is part of foreign key relation, remove foreign key constraint before making the update")
	}
	return http.StatusOK, nil
}

func canRenameOrChangeType(colName, table string) (int, error) {
	isPartOfPK := isPartOfPK(colName, table)
	isParent, childSchema := isParent(table)
	isChild := sessionState.conv.SpSchema[table].Parent != ""
	if isPartOfPK && (isParent || isChild) {
		return http.StatusBadRequest, fmt.Errorf("column : '%s' in table : '%s' is part of parent-child relation with schema : '%s'", colName, table, childSchema)
	}
	if isPartOfSecondaryIndex, indexName := isPartOfSecondaryIndex(colName, table); isPartOfSecondaryIndex {
		return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of secondary index : '%s', remove secondary index before making the update",
			colName, table, indexName)
	}
	isPartOfFK := isPartOfFK(colName, table)
	isReferencedByFK, relationTable := isReferencedByFK(colName, table)
	if isPartOfFK || isReferencedByFK {
		if isReferencedByFK {
			return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of foreign key relation with table : '%s', remove foreign key constraint before making the update",
				colName, table, relationTable)
		}
		return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of foreign keys, remove foreign key constraint before making the update",
			colName, table)
	}
	return http.StatusOK, nil
}

func checkPrimaryKeyPrefix(table string, refTable string, fk ddl.Foreignkey, tableInterleaveStatus *TableInterleaveStatus) bool {
	childPks := sessionState.conv.SpSchema[table].Pks
	parentPks := sessionState.conv.SpSchema[refTable].Pks
	if len(childPks) >= len(parentPks) {
		for i, pk := range parentPks {
			if i >= len(fk.ReferColumns) || pk.Col != fk.ReferColumns[i] || pk.Col != childPks[i].Col || fk.Columns[i] != fk.ReferColumns[i] {
				return false
			}
		}
	} else {
		return false
	}
	return true
}

func isUniqueName(name string) bool {
	for table, _ := range sessionState.conv.SpSchema {
		if table == name {
			return false
		}
	}
	for _, spSchema := range sessionState.conv.SpSchema {
		for _, fk := range spSchema.Fks {
			if fk.Name == name {
				return false
			}
		}
		for _, index := range spSchema.Indexes {
			if index.Name == name {
				return false
			}
		}
	}
	return true
}

func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func removePk(slice []ddl.IndexKey, s int) []ddl.IndexKey {
	return append(slice[:s], slice[s+1:]...)
}

func removeFk(slice []ddl.Foreignkey, s int) []ddl.Foreignkey {
	return append(slice[:s], slice[s+1:]...)
}

func removeSecondaryIndex(slice []ddl.CreateIndex, s int) []ddl.CreateIndex {
	return append(slice[:s], slice[s+1:]...)
}

func removeColumn(table string, colName string, srcTableName string) {
	sp := sessionState.conv.SpSchema[table]
	for i, col := range sp.ColNames {
		if col == colName {
			sp.ColNames = remove(sp.ColNames, i)
			break
		}
	}
	delete(sp.ColDefs, colName)
	for i, pk := range sp.Pks {
		if pk.Col == colName {
			sp.Pks = removePk(sp.Pks, i)
			break
		}
	}
	srcColName := sessionState.conv.ToSource[table].Cols[colName]
	delete(sessionState.conv.ToSource[table].Cols, colName)
	delete(sessionState.conv.ToSpanner[srcTableName].Cols, srcColName)
	delete(sessionState.conv.Issues[srcTableName], srcColName)
	sessionState.conv.SpSchema[table] = sp
}

func renameColumn(newName, table, colName, srcTableName string) {
	sp := sessionState.conv.SpSchema[table]
	for i, col := range sp.ColNames {
		if col == colName {
			sp.ColNames[i] = newName
			break
		}
	}
	if _, found := sp.ColDefs[colName]; found {
		sp.ColDefs[newName] = ddl.ColumnDef{
			Name:    newName,
			T:       sp.ColDefs[colName].T,
			NotNull: sp.ColDefs[colName].NotNull,
			Comment: sp.ColDefs[colName].Comment,
		}
		delete(sp.ColDefs, colName)
	}
	for i, pk := range sp.Pks {
		if pk.Col == colName {
			sp.Pks[i].Col = newName
			break
		}
	}
	srcColName := sessionState.conv.ToSource[table].Cols[colName]
	sessionState.conv.ToSpanner[srcTableName].Cols[srcColName] = newName
	sessionState.conv.ToSource[table].Cols[newName] = srcColName
	delete(sessionState.conv.ToSource[table].Cols, colName)
	sessionState.conv.SpSchema[table] = sp
}

func updateType(newType, table, colName, srcTableName string, w http.ResponseWriter) {
	sp, ty, err := getType(newType, table, colName, srcTableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	colDef := sp.ColDefs[colName]
	colDef.T = ty
	sp.ColDefs[colName] = colDef
}

func isTypeChanged(newType, table, colName, srcTableName string) (bool, error) {
	sp, ty, err := getType(newType, table, colName, srcTableName)
	if err != nil {
		return false, err
	}
	colDef := sp.ColDefs[colName]
	return !reflect.DeepEqual(colDef.T, ty), nil
}

func getType(newType, table, colName string, srcTableName string) (ddl.CreateTable, ddl.Type, error) {
	sp := sessionState.conv.SpSchema[table]
	srcColName := sessionState.conv.ToSource[table].Cols[colName]
	srcCol := sessionState.conv.SrcSchema[srcTableName].ColDefs[srcColName]
	var ty ddl.Type
	var issues []internal.SchemaIssue
	switch sessionState.driver {
	case constants.MYSQL, constants.MYSQLDUMP:
		ty, issues = toSpannerTypeMySQL(srcCol.Type.Name, newType, srcCol.Type.Mods)
	case constants.PGDUMP, constants.POSTGRES:
		ty, issues = toSpannerTypePostgres(srcCol.Type.Name, newType, srcCol.Type.Mods)
	case constants.SQLSERVER:
		ty, issues = toSpannerTypeSQLserver(srcCol.Type.Name, newType, srcCol.Type.Mods)
	case constants.ORACLE:
		ty, issues = oracle.ToSpannerTypeWeb(sessionState.conv, newType, srcCol.Type.Name, srcCol.Type.Mods)
	default:
		return sp, ty, fmt.Errorf("driver : '%s' is not supported", sessionState.driver)
	}
	if len(srcCol.Type.ArrayBounds) > 1 {
		ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
		issues = append(issues, internal.MultiDimensionalArray)
	}
	if srcCol.Ignored.Default {
		issues = append(issues, internal.DefaultValue)
	}
	if srcCol.Ignored.AutoIncrement {
		issues = append(issues, internal.AutoIncrement)
	}
	if sessionState.conv.Issues != nil && len(issues) > 0 {
		sessionState.conv.Issues[srcTableName][srcCol.Name] = issues
	}
	ty.IsArray = len(srcCol.Type.ArrayBounds) == 1
	return sp, ty, nil
}

func updateNotNull(notNullChange, table, colName string) {
	sp := sessionState.conv.SpSchema[table]
	switch notNullChange {
	case "ADDED":
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = true
		sp.ColDefs[colName] = spColDef
	case "REMOVED":
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = false
		sp.ColDefs[colName] = spColDef
	}
}

func rateSchema(cols, warnings int64, missingPKey bool) string {
	good := func(total, badCount int64) bool { return badCount < total/20 }
	ok := func(total, badCount int64) bool { return badCount < total/3 }
	switch {
	case cols == 0:
		return "GRAY"
	case warnings == 0 && !missingPKey:
		return "GREEN"
	case warnings == 0 && missingPKey:
		return "BLUE"
	case good(cols, warnings) && !missingPKey:
		return "BLUE"
	case good(cols, warnings) && missingPKey:
		return "BLUE"
	case ok(cols, warnings) && !missingPKey:
		return "YELLOW"
	case ok(cols, warnings) && missingPKey:
		return "YELLOW"
	case !missingPKey:
		return "ORANGE"
	default:
		return "ORANGE"
	}
}

func getFilePrefix(now time.Time) (string, error) {
	dbName := sessionState.dbName
	var err error
	if dbName == "" {
		dbName, err = utils.GetDatabaseName(sessionState.driver, now)
		if err != nil {
			return "", fmt.Errorf("Can not create database name : %v", err)
		}
	}
	return dbName + ".", nil
}

// SessionState stores information for the current migration session.
type SessionState struct {
	sourceDB    *sql.DB        // Connection to source database in case of direct connection
	dbName      string         // Name of source database
	driver      string         // Name of HarbourBridge driver in use
	conv        *internal.Conv // Current conversion state
	sessionFile string         // Path to session file
}

// sessionState maintains the current state of the session, and is used to
// track state from one request to the next. Session state is global:
// all requests see the same session state.
var sessionState SessionState

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
	//
	getConfigFromEnv()

	// Initialize mysqlTypeMap.
	for _, srcType := range []string{"bool", "boolean", "varchar", "char", "text", "tinytext", "mediumtext", "longtext", "set", "enum", "json", "bit", "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "double", "float", "numeric", "decimal", "date", "datetime", "timestamp", "time", "year", "geometrycollection", "multipoint", "multilinestring", "multipolygon", "point", "linestring", "polygon", "geometry"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric} {
			ty, issues := toSpannerTypeMySQL(srcType, spType, []int64{})
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
			ty, issues := toSpannerTypePostgres(srcType, spType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		postgresTypeMap[srcType] = l
	}

	// Initialize sqlserverTypeMap.
	for _, srcType := range []string{"int", "tinyint", "smallint", "bigint", "bit", "float", "real", "numeric", "decimal", "money", "smallmoney", "char", "nchar", "varchar", "nvarchar", "text", "ntext", "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp", "rowversion", "binary", "varbinary", "image", "xml", "geography", "geometry", "uniqueidentifier", "sql_variant", "hierarchyid"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric} {
			ty, issues := toSpannerTypeSQLserver(srcType, spType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		sqlserverTypeMap[srcType] = l
	}

	// Initialize oracleTypeMap.
	for _, srcType := range []string{"NUMBER", "BFILE", "BLOB", "CHAR", "CLOB", "DATE", "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT", "LONG", "RAW", "LONG RAW", "NCHAR", "NVARCHAR2", "VARCHAR", "VARCHAR2", "NCLOB", "ROWID", "UROWID", "XMLTYPE", "TIMESTAMP", "INTERVAL", "SDO_GEOMETRY"} {
		var l []typeIssue
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric} {
			ty, issues := oracle.ToSpannerTypeWeb(sessionState.conv, spType, srcType, []int64{})
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		oracleTypeMap[srcType] = l
	}

	sessionState.conv = internal.MakeConv()
}

// App connects to the web app.
func App() {
	addr := ":8080"
	router := getRoutes()
	log.Printf("Starting server at port 8080\n")
	log.Fatal(http.ListenAndServe(addr, handlers.CORS(handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}), handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS"}), handlers.AllowedOrigins([]string{"*"}))(router)))
}
