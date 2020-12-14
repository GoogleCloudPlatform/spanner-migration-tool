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
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	//"harbourbridge-web/models"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/mysql"
	"github.com/cloudspannerecosystem/harbourbridge/postgres"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/handlers"
	_ "github.com/lib/pq"
	toposort "github.com/stevenle/topsort"
)

// TODO(searce):
// 1) Test cases for APIs
// 2) API for saving/updating table-level changes.
// 3) API for showing logs
// 4) Split all routing to an route.go file
// 5) API for downloading the schema file, ddl file and summary report file.
// 6) Update schema conv after setting global datatypes and return conv. (setTypeMap)
// 7) Add rateConversion() in schema conversion, ddl and report APIs.
// 8) Add an overview in summary report API

func homeLink(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to Harbourbridge!")
	w.WriteHeader(http.StatusOK)
}

func databaseConnection(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), 500)
		return
	}
	var config DriverConfig
	err = json.Unmarshal(reqBody, &config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), 400)
		return
	}
	var dataSourceName string
	switch config.Driver {
	case "postgres":
		dataSourceName = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", config.Host, config.Port, config.User, config.Password, config.Database)
	case "mysql":
		dataSourceName = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", config.User, config.Password, config.Host, config.Port, config.Database)
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", config.Driver), 400)
		return
	}
	sourceDB, err := sql.Open(config.Driver, dataSourceName)
	if err != nil {
		http.Error(w, fmt.Sprintf("SQL connection error : %v", err), 500)
		return
	}
	// Open doesn't open a connection. Validate DSN data:
	err = sourceDB.Ping()
	if err != nil {
		http.Error(w, fmt.Sprintf("Connection Error: %v. Check Configuration again.", err), 500)
		return
	}
	app.sourceDB = sourceDB
	app.dbName = config.Database
	app.driver = config.Driver
	w.WriteHeader(http.StatusOK)
}

func convertSchemaSQL(w http.ResponseWriter, r *http.Request) {
	if app.sourceDB == nil || app.dbName == "" || app.driver == "" {
		http.Error(w, fmt.Sprintf("Database is not configured or Database connection is lost. Please set configuration and connect to database."), 404)
		return
	}
	conv := internal.MakeConv()
	var err error
	switch app.driver {
	case "mysql":
		err = mysql.ProcessInfoSchema(conv, app.sourceDB, app.dbName)
	case "postgres":
		err = postgres.ProcessInfoSchema(conv, app.sourceDB)
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", app.driver), 400)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Schema Conversion Error : %v", err), 404)
		return
	}
	app.conv = conv
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(conv)
}

func convertSchemaDump(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), 500)
		return
	}
	var dc DumpConfig
	err = json.Unmarshal(reqBody, &dc)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), 400)
		return
	}
	f, err := os.Open(dc.FilePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to open the test data file: %v", err), 404)
		return
	}
	conv, err := conversion.SchemaConv(dc.Driver, &conversion.IOStreams{In: f, Out: os.Stdout})
	if err != nil {
		http.Error(w, fmt.Sprintf("Schema Conversion Error : %v", err), 404)
		return
	}
	app.conv = conv
	app.driver = dc.Driver
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(conv)
}

func getDDL(w http.ResponseWriter, r *http.Request) {
	c := ddl.Config{Comments: true, ProtectIds: false}
	var tables []string
	for t := range app.conv.SpSchema {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	ddl := make(map[string]string)
	for _, t := range tables {
		ddl[t] = app.conv.SpSchema[t].PrintCreateTable(app.conv.SpSchema[t].InterleaveInto, c)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ddl)
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
	//fmt.Fprintf(out, "Wrote session to file '%s'.\n", dbName+sessionFile)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(session)
}

func resumeSession(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), 500)
		return
	}
	var s Session
	err = json.Unmarshal(reqBody, &s)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), 400)
		return
	}
	f, err := os.Open(s.FilePath + s.FileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to open the session file: %v", err), 404)
		return
	}
	defer f.Close()
	sessionJSON, _ := ioutil.ReadAll(f)
	json.Unmarshal(sessionJSON, &app.conv)
	app.driver = s.Driver
	w.WriteHeader(http.StatusOK)
}

func getSummary(w http.ResponseWriter, r *http.Request) {
	reports := internal.AnalyzeTables(app.conv, nil)
	summary := make(map[string]string)
	for _, t := range reports {
		// h := fmt.Sprintf("Table %s", t.SrcTable)
		// if t.SrcTable != t.SpTable {
		// 	h = h + fmt.Sprintf(" (mapped to Spanner table %s)", t.SpTable)
		// }
		//w.WriteString(rateConversion(t.rows, t.badRows, t.cols, t.warnings, t.syntheticPKey != "", false))
		//w.WriteString("\n")
		var body string
		for _, x := range t.Body {
			body = body + x.Heading + "\n"
			for i, l := range x.Lines {
				body = body + fmt.Sprintf("%d) %s.\n", i+1, l) + "\n"
			}
		}
		summary[t.SrcTable] = body
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(summary)
}
func writeHeading(s string) string {
	return strings.Join([]string{
		"----------------------------\n",
		s, "\n",
		"----------------------------\n"}, "")
}
func writeStmtStats(driverName string, conv *internal.Conv) string {
	stmtstats := ""
	type stat struct {
		statement string
		count     int64
	}
	var l []stat
	for s, x := range conv.Stats.Statement {
		l = append(l, stat{s, x.Schema + x.Data + x.Skip + x.Error})
	}
	// Sort by alphabetical order of statements.
	sort.Slice(l, func(i, j int) bool {
		return l[i].statement < l[j].statement
	})
	stmtstats = stmtstats + writeHeading("Statements Processed")
	stmtstats = stmtstats + "Analysis of statements in " + driverName + " output, broken down by statement type.\n"
	stmtstats = stmtstats + "  schema: statements successfully processed for Spanner schema information.\n"
	stmtstats = stmtstats + "    data: statements successfully processed for data.\n"
	stmtstats = stmtstats + "    skip: statements not relevant for Spanner schema or data.\n"
	stmtstats = stmtstats + "   error: statements that could not be processed.\n"
	stmtstats = stmtstats + "  --------------------------------------\n"
	stmtstats = stmtstats + fmt.Sprintf("  %6s %6s %6s %6s  %s\n", "schema", "data", "skip", "error", "statement")
	stmtstats = stmtstats + "  --------------------------------------\n"
	for _, x := range l {
		s := conv.Stats.Statement[x.statement]
		stmtstats = stmtstats + fmt.Sprintf("  %6d %6d %6d %6d  %s\n", s.Schema, s.Data, s.Skip, s.Error, x.statement)
	}
	if driverName == "pg_dump" {
		stmtstats = stmtstats + "See github.com/lfittl/pg_query_go/nodes for definitions of statement types\n"
		stmtstats = stmtstats + "(lfittl/pg_query_go is the library we use for parsing pg_dump output).\n"
		stmtstats = stmtstats + "\n"
	} else if driverName == "mysqldump" {
		stmtstats = stmtstats + "See https://github.com/pingcap/parser for definitions of statement types\n"
		stmtstats = stmtstats + "(pingcap/parser is the library we use for parsing mysqldump output).\n"
		stmtstats = stmtstats + "\n"
	}
	return stmtstats
}
func getOverview(w http.ResponseWriter, r *http.Request) {
	reports := internal.AnalyzeTables(app.conv, nil)
	summary := internal.GenerateSummary(app.conv, reports, nil)
	overview := writeHeading("Summary of Conversion")
	overview = overview + summary + "\n"
	ignored := internal.IgnoredStatements(app.conv)
	if len(ignored) > 0 {
		overview = overview + fmt.Sprintf("Note that the following source DB statements "+
			"were detected but ignored: %s.\n\n",
			strings.Join(ignored, ", "))
	}
	statementsMsg := ""
	var isDump bool
	if strings.Contains(app.driver, "dump") {
		isDump = true
	}
	if isDump {
		statementsMsg = "stats on the " + app.driver + " statements processed, followed by "
	}
	overview = overview + "The remainder of this report provides " + statementsMsg +
		"a table-by-table listing of schema and data conversion details. " +
		"For background on the schema and data conversion process used, " +
		"and explanations of the terms and notes used in this " +
		"report, see HarbourBridge's README.\n\n"
	if isDump {
		overview = overview + writeStmtStats(app.driver, app.conv)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(overview)
}

type severity int

const (
	warning severity = iota
	note
)

func getTypeMap(w http.ResponseWriter, r *http.Request) {
	if app.conv == nil || app.driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to spanner."), 404)
		return
	}
	var editTypeMap map[string][]typeIssue
	switch app.driver {
	case "mysql", "mysqldump":
		editTypeMap = mysqlTypeMap
	case "postgres", "pg_dump":
		editTypeMap = postgresTypeMap
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", app.driver), 400)
		return
	}
	// return a list of type-mapping for only the data-types
	// that are used in source schema.
	typeMap := make(map[string][]typeIssue)
	for _, srcTable := range app.conv.SrcSchema {
		for _, colDef := range srcTable.ColDefs {
			if _, ok := typeMap[colDef.Type.Name]; ok {
				continue
			}
			typeMap[colDef.Type.Name] = editTypeMap[colDef.Type.Name]
		}
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(typeMap)
}

type setT map[string]string

func setTypeMapGlobal(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), 500)
		return
	}
	var t setT
	err = json.Unmarshal(reqBody, &t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), 400)
		return
	}

	for k, v := range app.conv.SpSchema {
		for kk, _ := range v.ColDefs {
			for tk, tv := range t {
				sourceTable := app.conv.ToSource[k].Name
				sourceCol := app.conv.ToSource[k].Cols[kk]
				srcCol := app.conv.SrcSchema[sourceTable].ColDefs[sourceCol]
				if srcCol.Type.Name == tk {
					var ty ddl.Type
					var issues []internal.SchemaIssue
					switch app.driver {
					case "mysql", "mysqldump":
						ty, issues = toSpannerTypeMySQL(app.conv, srcCol.Type.Name, tv, srcCol.Type.Mods)
					case "pg_dump", "postgres":
						ty, issues = toSpannerTypePostgres(app.conv, srcCol.Type.Name, tv, srcCol.Type.Mods)
					default:
						http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", app.driver), 400)
						return
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
					if len(issues) > 0 {
						app.conv.Issues[sourceTable][srcCol.Name] = issues
					}
					ty.IsArray = len(srcCol.Type.ArrayBounds) == 1
					tempSpSchema := app.conv.SpSchema[k]
					tempColDef := tempSpSchema.ColDefs[kk]
					tempColDef.T = ty
					tempSpSchema.ColDefs[kk] = tempColDef
					app.conv.SpSchema[k] = tempSpSchema
				}
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(app.conv)
}
func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}
func removePk(slice []ddl.IndexKey, s int) []ddl.IndexKey {
	return append(slice[:s], slice[s+1:]...)
}

func removeColumn(table string, colName string, srcTableName string) {
	sp := app.conv.SpSchema[table]
	for i, col := range sp.ColNames {
		if col == colName {
			sp.ColNames = remove(sp.ColNames, i)
			break
		}
	}
	if _, found := sp.ColDefs[colName]; found {
		delete(sp.ColDefs, colName)
	}
	for i, pk := range sp.Pks {
		if pk.Col == colName {
			sp.Pks = removePk(sp.Pks, i)
			break
		}
	}
	srcName := app.conv.ToSource[table].Cols[colName]
	delete(app.conv.ToSource[table].Cols, colName)
	delete(app.conv.ToSpanner[srcTableName].Cols, srcName)
	delete(app.conv.Issues[srcTableName], srcName)
	app.conv.SpSchema[table] = sp
}
func renameColumn(newName, table, colName, srcTableName string) {
	sp := app.conv.SpSchema[table]
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
	srcName := app.conv.ToSource[table].Cols[colName]
	app.conv.ToSpanner[srcTableName].Cols[srcName] = newName
	app.conv.ToSource[table].Cols[newName] = srcName
	delete(app.conv.ToSource[table].Cols, colName)
	app.conv.SpSchema[table] = sp
}

func pkChanged(pkChange, table, colName string) {
	sp := app.conv.SpSchema[table]
	if pkChange == "REMOVED" {
		for i, pk := range sp.Pks {
			if pk.Col == colName {
				sp.Pks = removePk(sp.Pks, i)
				break
			}
		}
	}
	if pkChange == "ADDED" {
		if sPk, found := app.conv.SyntheticPKeys[table]; found {
			for i, col := range sp.ColNames {
				if col == sPk.Col {
					sp.ColNames = remove(sp.ColNames, i)
					break
				}
			}
			if _, found := sp.ColDefs[sPk.Col]; found {
				delete(sp.ColDefs, sPk.Col)
			}
			for i, pk := range sp.Pks {
				if pk.Col == sPk.Col {
					sp.Pks = removePk(sp.Pks, i)
					break
				}
			}
		}
		sp.Pks = append(sp.Pks, ddl.IndexKey{Col: colName, Desc: false})
	}
	app.conv.SpSchema[table] = sp
}

func updateType(newType, table, newColName, srcTableName string, w http.ResponseWriter) {
	sp := app.conv.SpSchema[table]
	srcColName := app.conv.ToSource[table].Cols[newColName]
	srcCol := app.conv.SrcSchema[srcTableName].ColDefs[srcColName]
	var ty ddl.Type
	var issues []internal.SchemaIssue
	switch app.driver {
	case "mysql", "mysqldump":
		ty, issues = toSpannerTypeMySQL(app.conv, srcCol.Type.Name, newType, srcCol.Type.Mods)
	case "pg_dump", "postgres":
		ty, issues = toSpannerTypePostgres(app.conv, srcCol.Type.Name, newType, srcCol.Type.Mods)
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", app.driver), 400)
		return
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
	if len(issues) > 0 {
		app.conv.Issues[srcTableName][srcCol.Name] = issues
	}
	ty.IsArray = len(srcCol.Type.ArrayBounds) == 1
	tempColDef := sp.ColDefs[newColName]
	tempColDef.T = ty
	sp.ColDefs[newColName] = tempColDef
	app.conv.SpSchema[table] = sp
}

func updateNotNull(notNullChange, table, newColName string) {
	sp := app.conv.SpSchema[table]
	switch notNullChange {
	case "ADDED":
		spColDef := sp.ColDefs[newColName]
		spColDef.NotNull = true
		sp.ColDefs[newColName] = spColDef
	case "REMOVED":
		spColDef := sp.ColDefs[newColName]
		spColDef.NotNull = false
		sp.ColDefs[newColName] = spColDef
	default:
		//we skip this
	}
	app.conv.SpSchema[table] = sp
}

func setTypeMapTableLevel(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), 500)
		return
	}
	var t updateTable
	table := r.FormValue("table")
	err = json.Unmarshal(reqBody, &t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), 400)
		return
	}
	srcTableName := app.conv.ToSource[table].Name
	for colName, v := range t.UpdateCols {
		if v.Removed {
			removeColumn(table, colName, srcTableName)
			continue
		}
		newColName := colName
		if v.Rename != "" && v.Rename != colName {
			renameColumn(v.Rename, table, colName, srcTableName)
			newColName = v.Rename
		}
		if v.PK != "" {
			pkChanged(v.PK, table, newColName)
		}
		if v.ToType != "" {
			updateType(v.ToType, table, newColName, srcTableName, w)
		}
		if v.NotNull != "" {
			updateNotNull(v.NotNull, table, newColName)
		}
	}
	app.conv.AddPrimaryKeys()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(app.conv)
}

func rateSchema(cols, warnings int64, missingPKey bool) string {
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
		return "RED"
	default:
		return "RED"
	}
}
func good(total, badCount int64) bool {
	return badCount < total/20
}

func ok(total, badCount int64) bool {
	return badCount < total/3
}
func getConversionRate(w http.ResponseWriter, r *http.Request) {
	reports := internal.AnalyzeTables(app.conv, nil)
	rate := make(map[string]string)
	for _, t := range reports {
		rate[t.SpTable] = rateSchema(t.Cols, t.Warnings, t.SyntheticPKey != "")
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(rate)
}

type schemaAndReportFile struct {
	Report string
	Schema string
}

func getSchemaAndReportFile(w http.ResponseWriter, r *http.Request) {
	ioHelper := &conversion.IOStreams{In: os.Stdin, Out: os.Stdout}
	dbName := app.dbName
	var err error
	now := time.Now()
	if dbName == "" {
		dbName, err = conversion.GetDatabaseName(app.driver, now)
		if err != nil {
			http.Error(w, fmt.Sprintf("Can not create database name : %v", err), 500)
		}
	}
	filePrefix := dbName + "."
	if err != nil {
		fmt.Printf("\nCan't get database name: %v\n", err)
		panic(fmt.Errorf("can't get database name"))
	}
	reportFileName := "frontend/" + filePrefix + "report.txt"
	schemaFileName := "frontend/" + filePrefix + "schema.txt"
	response := &schemaAndReportFile{}
	conversion.WriteSchemaFile(app.conv, now, schemaFileName, ioHelper.Out)
	conversion.Report(app.driver, nil, ioHelper.BytesRead, "", app.conv, reportFileName, ioHelper.Out)
	reportAbsPath, err := filepath.Abs(reportFileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Can not create absolute path : %v", err), 500)
	}
	schemaAbsPath, err := filepath.Abs(schemaFileName)
	response.Report = reportAbsPath
	response.Schema = schemaAbsPath
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

type tableInterleaveStatus struct {
	Possible bool
	Parent   string
	Comment  string
}

func checkPrimaryKeyPrefix(table string, refTable string, tableInterleaveIssues *tableInterleaveStatus) {
	childPks := app.conv.SpSchema[table].Pks
	parentPks := app.conv.SpSchema[refTable].Pks
	var referedCols []string
	for _, fk := range app.conv.SpSchema[table].Fks {
		for _, col := range fk.ReferColumns {
			referedCols = append(referedCols, col)
		}
	}
	if len(childPks) > len(parentPks) {
		for i, pk := range parentPks {
			if pk.Col != referedCols[i] || pk.Col != childPks[i].Col {
				tableInterleaveIssues.Possible = false
				tableInterleaveIssues.Comment = "prefix key doesn't match"
				break
			}
		}
	} else {
		tableInterleaveIssues.Possible = false
		tableInterleaveIssues.Comment = "prefix key doesn't match"
	}

}

func checkCyclicDependency(table string, refTable string, tableInterleaveIssues *tableInterleaveStatus) {
	graph := toposort.NewGraph()
	for t := range app.conv.SpSchema {
		graph.AddNode(t)
	}
	for t := range app.conv.SpSchema {
		for _, fk := range app.conv.SpSchema[t].Fks {
			graph.AddEdge(t, fk.ReferTable)
		}
	}
	res, err := graph.TopSort(refTable)
	if err != nil {
		cycle := strings.Split(fmt.Sprintf("%v", err), ":")
		cycle = strings.Split(cycle[1], "->")
		for _, ctable := range cycle {
			t := strings.TrimSpace(ctable)
			if t == table {
				tableInterleaveIssues.Possible = false
				tableInterleaveIssues.Comment = "cyclic dependency"
				break
			}
		}
	}
	if tableInterleaveIssues.Possible == true {
		for _, visTables := range res {
			if visTables == table {
				tableInterleaveIssues.Possible = false
				tableInterleaveIssues.Comment = "cyclic dependency"
				break
			}
		}
	}
}

// Work in progress
func checkForInterleavedTables(w http.ResponseWriter, r *http.Request) {
	table := r.FormValue("table")
	if table == "" {
		http.Error(w, fmt.Sprintf("Table name is empty"), 400)
	}
	tableInterleaveIssues := &tableInterleaveStatus{Possible: true}
	tablesUsed := make(map[string]bool)
	var refTable string
	for _, fk := range app.conv.SpSchema[table].Fks {
		tablesUsed[fk.ReferTable] = true
		refTable = fk.ReferTable
	}
	if len(tablesUsed) != 1 {
		tableInterleaveIssues.Possible = false
		tableInterleaveIssues.Comment = "multiple or no parent"
	}
	if _, found := app.conv.SyntheticPKeys[table]; found {
		tableInterleaveIssues.Possible = false
		tableInterleaveIssues.Comment = "has synthetic pk"
	}
	if _, found := app.conv.SyntheticPKeys[refTable]; found {
		tableInterleaveIssues.Possible = false
		tableInterleaveIssues.Comment = "parent has synthetic pk"
	}
	if tableInterleaveIssues.Possible == true {
		checkPrimaryKeyPrefix(table, refTable, tableInterleaveIssues)
	}
	if tableInterleaveIssues.Possible == true {
		checkCyclicDependency(table, refTable, tableInterleaveIssues)
	}
	if tableInterleaveIssues.Possible == true {
		tableInterleaveIssues.Parent = refTable
		sp := app.conv.SpSchema[table]
		sp.InterleaveInto = refTable
		app.conv.SpSchema[table] = sp
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(tableInterleaveIssues)
}

type App struct {
	sourceDB *sql.DB
	dbName   string
	driver   string
	conv     *internal.Conv
}

var app App

func WebApp() {
	fmt.Println("-------------------")
	router := getRoutes()
	log.Fatal(http.ListenAndServe(":8080", handlers.CORS(handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization"}), handlers.AllowedMethods([]string{"GET", "POST", "PUT", "HEAD", "OPTIONS"}), handlers.AllowedOrigins([]string{"*"}))(router)))
}
