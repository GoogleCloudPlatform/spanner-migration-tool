package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/oracle"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/sqlserver"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/config"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/index"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/primarykey"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/types"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

var mysqlDefaultTypeMap = make(map[string]ddl.Type)
var postgresDefaultTypeMap = make(map[string]ddl.Type)
var sqlserverDefaultTypeMap = make(map[string]ddl.Type)
var oracleDefaultTypeMap = make(map[string]ddl.Type)

var mysqlTypeMap = make(map[string][]types.TypeIssue)
var postgresTypeMap = make(map[string][]types.TypeIssue)
var sqlserverTypeMap = make(map[string][]types.TypeIssue)
var oracleTypeMap = make(map[string][]types.TypeIssue)

var autoGenMap = make(map[string][]types.AutoGen)

func init() {
	sessionState := session.GetSessionState()
	utilities.InitObjectId()
	sessionState.Conv = internal.MakeConv()
	config := config.TryInitializeSpannerConfig()
	session.SetSessionStorageConnectionState(config.GCPProjectID, config.SpannerProjectID, config.SpannerInstanceID)
}

// ConvertSchemaSQL converts source database to Spanner when using
// with postgres and mysql driver.
func ConvertSchemaSQL(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	if sessionState.SourceDB == nil || sessionState.DbName == "" || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Database is not configured or Database connection is lost. Please set configuration and connect to database."), http.StatusNotFound)
		return
	}
	conv := internal.MakeConv()

	conv.SpDialect = sessionState.Dialect
	conv.IsSharded = sessionState.IsSharded
	var err error
	additionalSchemaAttributes := internal.AdditionalSchemaAttributes{
		IsSharded: sessionState.IsSharded,
	}
	processSchema := common.ProcessSchemaImpl{}
	switch sessionState.Driver {
	case constants.MYSQL:
		err = processSchema.ProcessSchema(conv, mysql.InfoSchemaImpl{DbName: sessionState.DbName, Db: sessionState.SourceDB}, common.DefaultWorkers, additionalSchemaAttributes, &common.SchemaToSpannerImpl{}, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
	case constants.POSTGRES:
		temp := false
		err = processSchema.ProcessSchema(conv, postgres.InfoSchemaImpl{Db: sessionState.SourceDB, IsSchemaUnique: &temp}, common.DefaultWorkers, additionalSchemaAttributes, &common.SchemaToSpannerImpl{}, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
	case constants.SQLSERVER:
		err = processSchema.ProcessSchema(conv, sqlserver.InfoSchemaImpl{DbName: sessionState.DbName, Db: sessionState.SourceDB}, common.DefaultWorkers, additionalSchemaAttributes, &common.SchemaToSpannerImpl{}, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
	case constants.ORACLE:
		err = processSchema.ProcessSchema(conv, oracle.InfoSchemaImpl{DbName: strings.ToUpper(sessionState.DbName), Db: sessionState.SourceDB}, common.DefaultWorkers, additionalSchemaAttributes, &common.SchemaToSpannerImpl{}, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", sessionState.Driver), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Schema Conversion Error : %v", err), http.StatusNotFound)
		return
	}

	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()

	sessionState.Conv = conv

	if sessionState.IsSharded {
		setShardIdColumnAsPrimaryKey(true)
		addShardIdColumnToForeignKeys(true)
		ruleId := internal.GenerateRuleId()
		rule := internal.Rule{
			Id:                ruleId,
			Name:              ruleId,
			Type:              constants.AddShardIdPrimaryKey,
			AssociatedObjects: "All Tables",
			Data: types.ShardIdPrimaryKey{
				AddedAtTheStart: true,
			},
			Enabled: true,
		}

		sessionState := session.GetSessionState()
		sessionState.Conv.Rules = append(sessionState.Conv.Rules, rule)
		session.UpdateSessionFile()
	}

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
		Conv:            *sessionState.Conv,
	}
	sessionState.SessionMetadata = sessionMetadata
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

// ConvertSchemaDump converts schema from dump file to Spanner schema for
// mysqldump and pg_dump driver.
func ConvertSchemaDump(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var dc types.ConvertFromDumpRequest
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
	n := profiles.NewSourceProfileImpl{}
	sourceProfile, _ := profiles.NewSourceProfile("", dc.Config.Driver, &n)
	sourceProfile.Driver = dc.Config.Driver
	schemaFromSource := conversion.SchemaFromSourceImpl{}
	conv, err := schemaFromSource.SchemaFromDump(sourceProfile.Driver, dc.SpannerDetails.Dialect, &utils.IOStreams{In: f, Out: os.Stdout}, &conversion.ProcessDumpByDialectImpl{})
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
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
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

// GetDDL returns the Spanner DDL for each table in alphabetical order.
// Unlike internal/convert.go's GetDDL, it does not print tables in a way that
// respects the parent/child ordering of interleaved tables.
// Though foreign keys and secondary indexes are displayed, getDDL cannot be used to
// build DDL to send to Spanner.
func GetDDL(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.RLock()
	defer sessionState.Conv.ConvLock.RUnlock()
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

func GetStandardTypeToPGSQLTypemap(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ddl.STANDARD_TYPE_TO_PGSQL_TYPEMAP)
}

func GetPGSQLToStandardTypeTypemap(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ddl.PGSQL_TO_STANDARD_TYPE_TYPEMAP)
}

func SpannerDefaultTypeMap(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()

	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, "Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner.", http.StatusNotFound)
		return
	}
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	initializeTypeMap()

	var typeMap map[string]ddl.Type
	switch sessionState.Driver {
	case constants.MYSQL, constants.MYSQLDUMP:
		typeMap = mysqlDefaultTypeMap
	case constants.POSTGRES, constants.PGDUMP:
		typeMap = postgresDefaultTypeMap
	case constants.SQLSERVER:
		typeMap = sqlserverDefaultTypeMap
	case constants.ORACLE:
		typeMap = oracleDefaultTypeMap
	default:
		http.Error(w, fmt.Sprintf("Driver : '%s' is not supported", sessionState.Driver), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(typeMap)
}

// GetTypeMap returns the source to Spanner typemap only for the
// source types used in current conversion.
func GetTypeMap(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	var typeMap map[string][]types.TypeIssue
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
	filteredTypeMap := make(map[string][]types.TypeIssue)
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

func GetAutoGenMap(w http.ResponseWriter, r *http.Request) {

	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return
	}
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	switch sessionState.Driver {
	case constants.MYSQL:
		initializeAutoGenMap()
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(autoGenMap)
}

// GetTableWithErrors checks the errors in the spanner schema
// and returns a list of tables with errors
func GetTableWithErrors(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.RLock()
	defer sessionState.Conv.ConvLock.RUnlock()
	var tableIdName []types.TableIdAndName
	for id, issues := range sessionState.Conv.SchemaIssues {
		for _, issue := range issues.TableLevelIssues {
			if reports.IssueDB[issue].Severity == reports.Errors {
				t := types.TableIdAndName{
					Id:   id,
					Name: sessionState.Conv.SpSchema[id].Name,
				}
				tableIdName = append(tableIdName, t)
			}
		}
	}
	sort.Slice(tableIdName, func(i, j int) bool {
		return tableIdName[i].Name < tableIdName[j].Name
	})
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(tableIdName)
}

func RestoreTables(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var tables internal.Tables
	err = json.Unmarshal(reqBody, &tables)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	var convm session.ConvWithMetadata
	for _, tableId := range tables.TableList {
		convm = restoreTableHelper(w, tableId)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func RestoreTable(w http.ResponseWriter, r *http.Request) {
	tableId := r.FormValue("table")
	convm := restoreTableHelper(w, tableId)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func DropTables(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var tables internal.Tables
	err = json.Unmarshal(reqBody, &tables)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	var convm session.ConvWithMetadata
	for _, tableId := range tables.TableList {
		convm = dropTableHelper(w, tableId)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func DropTable(w http.ResponseWriter, r *http.Request) {
	tableId := r.FormValue("table")
	convm := dropTableHelper(w, tableId)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func RestoreSecondaryIndex(w http.ResponseWriter, r *http.Request) {
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

	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
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

	spIndex := common.CvtIndexHelper(conv, tableId, srcIndex, conv.SpSchema[tableId].ColIds, conv.SpSchema[tableId].ColDefs)
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
func UpdateForeignKeys(w http.ResponseWriter, r *http.Request) {
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
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()

	newFKs := []ddl.Foreignkey{}
	if err = json.Unmarshal(reqBody, &newFKs); err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	// Check new name for spanner name validity.
	newNames := []string{}
	newNamesMap := map[string]bool{}
	for _, newFk := range newFKs {
		if len(newFk.Name) == 0 {
			continue
		}
		for _, oldFk := range sessionState.Conv.SpSchema[tableId].ForeignKeys {
			if newFk.Id == oldFk.Id && newFk.Name != oldFk.Name && newFk.Name != "" {
				newNames = append(newNames, strings.ToLower(newFk.Name))
			}
		}
	}

	for _, newFk := range newFKs {
		if len(newFk.Name) == 0 {
			continue
		}
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
		for i, updatedForeignkey := range newFKs {
			if foreignKey.Id == updatedForeignkey.Id && len(updatedForeignkey.ColIds) != 0 && updatedForeignkey.ReferTableId != "" {
				delete(usedNames, strings.ToLower(foreignKey.Name))
				foreignKey.Name = updatedForeignkey.Name
				updatedFKs = append(updatedFKs, foreignKey)
			}
			if foreignKey.Id == updatedForeignkey.Id && len(updatedForeignkey.ReferColumnIds) == 0 && updatedForeignkey.ReferTableId == "" {
				dropFkId := updatedForeignkey.Id

				// To remove the interleavable suggestions if they exist on dropping fk
				colId := sp.ForeignKeys[i].ColIds[0]
				schemaIssue := []internal.SchemaIssue{}
				for _, v := range sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] {
					if v != internal.InterleavedAddColumn && v != internal.InterleavedRenameColumn && v != internal.InterleavedNotInOrder && v != internal.InterleavedChangeColumnSize {
						schemaIssue = append(schemaIssue, v)
					}
				}
				if _, ok := sessionState.Conv.SchemaIssues[tableId]; ok {
					sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] = schemaIssue
				}
				var err error
				sp.ForeignKeys, err = utilities.RemoveFk(sp.ForeignKeys, dropFkId, sessionState.Conv.SrcSchema[tableId], tableId)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
				}
			}
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
func RenameIndexes(w http.ResponseWriter, r *http.Request) {
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

// setParentTable checks whether specified table can be interleaved, and updates the schema to convert foreign
// key to interleaved table if 'update' parameter is set to true. If 'update' parameter is set to false, then return
// whether the foreign key can be converted to interleave table without updating the schema.
func SetParentTable(w http.ResponseWriter, r *http.Request) {
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

	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	tableInterleaveStatus := parentTableHelper(tableId, update)

	if tableInterleaveStatus.Possible {

		childPks := sessionState.Conv.SpSchema[tableId].PrimaryKeys
		childindex := utilities.GetPrimaryKeyIndexFromOrder(childPks, 1)
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

func RemoveParentTable(w http.ResponseWriter, r *http.Request) {
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

	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	conv := sessionState.Conv

	if conv.SpSchema[tableId].ParentTable.Id == "" {
		http.Error(w, fmt.Sprintf("Table is not interleaved"), http.StatusBadRequest)
		return
	}
	spTable := conv.SpSchema[tableId]

	var firstOrderPk ddl.IndexKey
	order := 1

	isPresent, isAddedAtFirst := hasShardIdPrimaryKeyRule()
	if isAddedAtFirst {
		order = 2
	}

	for _, pk := range spTable.PrimaryKeys {
		if pk.Order == order {
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

	if isPresent {
		if isAddedAtFirst {
			spFk.ColIds = append([]string{spTable.ShardIdColumn}, spFk.ColIds...)
			spFk.ReferColumnIds = append([]string{sessionState.Conv.SpSchema[spTable.ParentTable.Id].ShardIdColumn}, spFk.ReferColumnIds...)
		} else {
			spFk.ColIds = append(spFk.ColIds, spTable.ShardIdColumn)
			spFk.ReferColumnIds = append(spFk.ReferColumnIds, sessionState.Conv.SpSchema[spTable.ParentTable.Id].ShardIdColumn)
		}
	}

	spFks := spTable.ForeignKeys
	spFks = append(spFks, spFk)
	spTable.ForeignKeys = spFks
	spTable.ParentTable.Id = ""
	spTable.ParentTable.OnDelete = ""
	conv.SpSchema[tableId] = spTable

	sessionState.Conv = conv

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)

}

func UpdateIndexes(w http.ResponseWriter, r *http.Request) {
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
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
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

func DropSecondaryIndex(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()

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

// GetConversionRate returns table wise color coded conversion rate.
func GetConversionRate(w http.ResponseWriter, r *http.Request) {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	smt_reports := reports.AnalyzeTables(sessionState.Conv, nil)
	rate := make(map[string]string)
	for _, t := range smt_reports {
		rate[t.SpTable], _ = reports.RateSchema(t.Cols, t.Warnings, t.Errors, t.SyntheticPKey != "", false)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(rate)
}

func restoreTableHelper(w http.ResponseWriter, tableId string) session.ConvWithMetadata {
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
	}

	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
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
	}

	err := common.SrcTableToSpannerDDL(conv, toddl, sessionState.Conv.SrcSchema[tableId])
	if err != nil {
		http.Error(w, fmt.Sprintf("Restoring spanner table fail"), http.StatusBadRequest)
	}
	conv.AddPrimaryKeys()
	if sessionState.IsSharded {
		conv.IsSharded = true
		conv.AddShardIdColumn()
		isPresent, isAddedAtFirst := hasShardIdPrimaryKeyRule()
		if isPresent {
			table := sessionState.Conv.SpSchema[tableId]
			setShardIdColumnAsPrimaryKeyPerTable(isAddedAtFirst, table)
			addShardIdToForeignKeyPerTable(isAddedAtFirst, table)
			addShardIdToReferencedTableFks(tableId, isAddedAtFirst)
		}
	}
	sessionState.Conv = conv
	primarykey.DetectHotspot()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}
	return convm
}

func parentTableHelper(tableId string, update bool) *types.TableInterleaveStatus {
	tableInterleaveStatus := &types.TableInterleaveStatus{
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
		onDelete := fk.OnDelete
		var err error

		if _, found := sessionState.Conv.SyntheticPKeys[refTableId]; found {
			continue
		}

		if checkPrimaryKeyPrefix(tableId, refTableId, fk, tableInterleaveStatus) {
			sp := sessionState.Conv.SpSchema[tableId]

			colIdNotInOrder := checkPrimaryKeyOrder(tableId, refTableId, fk)

			if update && sp.ParentTable.Id == "" && colIdNotInOrder == "" {
				usedNames := sessionState.Conv.UsedNames
				delete(usedNames, strings.ToLower(sp.ForeignKeys[i].Name))
				sp.ParentTable.Id = refTableId
				sp.ParentTable.OnDelete = onDelete
				sp.ForeignKeys, err = utilities.RemoveFk(sp.ForeignKeys, sp.ForeignKeys[i].Id, sessionState.Conv.SrcSchema[tableId], tableId)
				if err != nil {
					continue
				}
			}
			sessionState.Conv.SpSchema[tableId] = sp

			parentpks := sessionState.Conv.SpSchema[refTableId].PrimaryKeys
			if len(parentpks) >= 1 {
				if colIdNotInOrder == "" {

					schemaissue := []internal.SchemaIssue{}
					for _, column := range childPks {
						colId := column.ColId
						schemaissue = sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId]

						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
						schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedChangeColumnSize)

						sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colId] = schemaissue
					}

					tableInterleaveStatus.Possible = true
					tableInterleaveStatus.Parent = refTableId
					tableInterleaveStatus.OnDelete = onDelete
					tableInterleaveStatus.Comment = ""

				} else {

					schemaissue := []internal.SchemaIssue{}
					schemaissue = sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colIdNotInOrder]

					schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)
					schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
					schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)
					schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleavedChangeColumnSize)

					schemaissue = append(schemaissue, internal.InterleavedNotInOrder)

					sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[colIdNotInOrder] = schemaissue
				}
			}
		}
	}

	return tableInterleaveStatus
}

func checkPrimaryKeyOrder(tableId string, refTableId string, fk ddl.Foreignkey) string {
	sessionState := session.GetSessionState()
	childPks := sessionState.Conv.SpSchema[tableId].PrimaryKeys
	parentPks := sessionState.Conv.SpSchema[refTableId].PrimaryKeys
	childTable := sessionState.Conv.SpSchema[tableId]
	parentTable := sessionState.Conv.SpSchema[refTableId]
	for i := 0; i < len(parentPks); i++ {

		for j := 0; j < len(childPks); j++ {

			for k := 0; k < len(fk.ReferColumnIds); k++ {

				if childTable.ColDefs[fk.ColIds[k]].Name == parentTable.ColDefs[fk.ReferColumnIds[k]].Name &&
					parentTable.ColDefs[parentPks[i].ColId].Name == childTable.ColDefs[childPks[j].ColId].Name &&
					parentTable.ColDefs[parentPks[i].ColId].T.Name == childTable.ColDefs[childPks[j].ColId].T.Name &&
					parentTable.ColDefs[parentPks[i].ColId].T.Len == childTable.ColDefs[childPks[j].ColId].T.Len &&
					parentTable.ColDefs[parentPks[i].ColId].Name == parentTable.ColDefs[fk.ReferColumnIds[k]].Name &&
					childTable.ColDefs[childPks[j].ColId].Name == parentTable.ColDefs[fk.ReferColumnIds[k]].Name {
					if parentPks[i].Order != childPks[j].Order {
						return childPks[j].ColId
					}
				}
			}

		}

	}
	return ""
}

func checkPrimaryKeyPrefix(tableId string, refTableId string, fk ddl.Foreignkey, tableInterleaveStatus *types.TableInterleaveStatus) bool {

	sessionState := session.GetSessionState()
	childTable := sessionState.Conv.SpSchema[tableId]
	parentTable := sessionState.Conv.SpSchema[refTableId]
	childPks := sessionState.Conv.SpSchema[tableId].PrimaryKeys
	parentPks := sessionState.Conv.SpSchema[refTableId].PrimaryKeys
	possibleInterleave := false

	flag := false
	for _, key := range parentPks {
		flag = false
		for _, colId := range fk.ReferColumnIds {
			if key.ColId == colId {
				flag = true
			}
		}
		if !flag {
			break
		}
	}
	if flag {
		possibleInterleave = true
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

	if len(interleaved) == len(parentPks) {
		return true
	}

	diff := []ddl.IndexKey{}

	if len(interleaved) == 0 {

		for i := 0; i < len(parentPks); i++ {

			for j := 0; j < len(childPks); j++ {

				if parentTable.ColDefs[parentPks[i].ColId].Name != childTable.ColDefs[childPks[j].ColId].Name || parentTable.ColDefs[parentPks[i].ColId].T.Len != childTable.ColDefs[childPks[j].ColId].T.Len {
					diff = append(diff, parentPks[i])
				}

			}
		}

	}

	canInterleavedOnAdd := []string{}
	canInterleavedOnRename := []string{}
	canInterLeaveOnChangeInColumnSize := []string{}

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
			if parentTable.ColDefs[diff[i].ColId].Name == childTable.ColDefs[fk.ColIds[parentColIndex]].Name {
				canInterLeaveOnChangeInColumnSize = append(canInterLeaveOnChangeInColumnSize, fk.ColIds[parentColIndex])
			} else {
				canInterleavedOnRename = append(canInterleavedOnRename, fk.ColIds[parentColIndex])
			}

		}
	}

	if len(canInterLeaveOnChangeInColumnSize) > 0 {
		updateInterleaveSuggestion(canInterLeaveOnChangeInColumnSize, tableId, internal.InterleavedChangeColumnSize)
	} else if len(canInterleavedOnRename) > 0 {
		updateInterleaveSuggestion(canInterleavedOnRename, tableId, internal.InterleavedRenameColumn)
	} else if len(canInterleavedOnAdd) > 0 {
		updateInterleaveSuggestion(canInterleavedOnAdd, tableId, internal.InterleavedAddColumn)
	}

	return false
}

func updateInterleaveSuggestion(colIds []string, tableId string, issue internal.SchemaIssue) {
	sessionState := session.GetSessionState()

	for i := 0; i < len(colIds); i++ {

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
	sessionState := session.GetSessionState()

	for i := 0; i < len(colIds); i++ {

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

func hasShardIdPrimaryKeyRule() (bool, bool) {
	sessionState := session.GetSessionState()
	for _, rule := range sessionState.Conv.Rules {
		if rule.Type == constants.AddShardIdPrimaryKey {
			v := rule.Data.(types.ShardIdPrimaryKey)
			return true, v.AddedAtTheStart
		}
	}
	return false, false
}

func dropTableHelper(w http.ResponseWriter, tableId string) session.ConvWithMetadata {
	sessionState := session.GetSessionState()
	if sessionState.Conv == nil || sessionState.Driver == "" {
		http.Error(w, fmt.Sprintf("Schema is not converted or Driver is not configured properly. Please retry converting the database to Spanner."), http.StatusNotFound)
		return session.ConvWithMetadata{}
	}
	if tableId == "" {
		http.Error(w, fmt.Sprintf("Table Id is empty"), http.StatusBadRequest)
	}
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	spSchema := sessionState.Conv.SpSchema
	issues := sessionState.Conv.SchemaIssues
	syntheticPkey := sessionState.Conv.SyntheticPKeys

	//remove deleted name from usedName
	usedNames := sessionState.Conv.UsedNames
	delete(usedNames, strings.ToLower(sessionState.Conv.SpSchema[tableId].Name))
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
	for id, spTable := range spSchema {
		if spTable.ParentTable.Id == tableId {
			spTable.ParentTable.Id = ""
			spTable.ParentTable.OnDelete = ""
			spSchema[id] = spTable
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
	return convm
}

func addShardIdToReferencedTableFks(tableId string, isAddedAtFirst bool) {
	sessionState := session.GetSessionState()
	for _, table := range sessionState.Conv.SpSchema {
		for i, fk := range table.ForeignKeys {
			if fk.ReferTableId == tableId {
				referredTableShardIdColumn := sessionState.Conv.SpSchema[fk.ReferTableId].ShardIdColumn
				if isAddedAtFirst {
					fk.ColIds = append([]string{table.ShardIdColumn}, fk.ColIds...)
					fk.ReferColumnIds = append([]string{referredTableShardIdColumn}, fk.ReferColumnIds...)
				} else {
					fk.ColIds = append(fk.ColIds, table.ShardIdColumn)
					fk.ReferColumnIds = append(fk.ReferColumnIds, referredTableShardIdColumn)
				}
				sessionState.Conv.SpSchema[table.Id].ForeignKeys[i] = fk
			}
		}
	}
}

func initializeTypeMap() {
	sessionState := session.GetSessionState()
	var toddl common.ToDdl
	// Initialize mysqlTypeMap.
	toddl = mysql.InfoSchemaImpl{}.GetToDdl()
	for _, srcTypeName := range []string{"bool", "boolean", "varchar", "char", "text", "tinytext", "mediumtext", "longtext", "set", "enum", "json", "bit", "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "double", "float", "numeric", "decimal", "date", "datetime", "timestamp", "time", "year", "geometrycollection", "multipoint", "multilinestring", "multipolygon", "point", "linestring", "polygon", "geometry"} {
		var l []types.TypeIssue
		srcType := schema.MakeType()
		srcType.Name = srcTypeName
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			ty, issues := toddl.ToSpannerType(sessionState.Conv, spType, srcType)
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		if srcTypeName == "tinyint" {
			l = append(l, types.TypeIssue{T: ddl.Bool, Brief: "Only tinyint(1) can be converted to BOOL, for any other mods it will be converted to INT64"})
		}
		ty, _ := toddl.ToSpannerType(sessionState.Conv, "", srcType)
		mysqlDefaultTypeMap[srcTypeName] = ty
		mysqlTypeMap[srcTypeName] = l
	}
	// Initialize postgresTypeMap.
	toddl = postgres.InfoSchemaImpl{}.GetToDdl()
	for _, srcTypeName := range []string{"bool", "boolean", "bigserial", "bpchar", "character", "bytea", "date", "float8", "double precision", "float4", "real", "int8", "bigint", "int4", "integer", "int2", "smallint", "numeric", "serial", "text", "timestamptz", "timestamp with time zone", "timestamp", "timestamp without time zone", "varchar", "character varying", "path"} {
		var l []types.TypeIssue
		srcType := schema.MakeType()
		srcType.Name = srcTypeName
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			ty, issues := toddl.ToSpannerType(sessionState.Conv, spType, srcType)
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		ty, _ := toddl.ToSpannerType(sessionState.Conv, "", srcType)
		postgresDefaultTypeMap[srcTypeName] = ty
		postgresTypeMap[srcTypeName] = l
	}

	// Initialize sqlserverTypeMap.
	toddl = sqlserver.InfoSchemaImpl{}.GetToDdl()
	for _, srcTypeName := range []string{"int", "tinyint", "smallint", "bigint", "bit", "float", "real", "numeric", "decimal", "money", "smallmoney", "char", "nchar", "varchar", "nvarchar", "text", "ntext", "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time", "timestamp", "rowversion", "binary", "varbinary", "image", "xml", "geography", "geometry", "uniqueidentifier", "sql_variant", "hierarchyid"} {
		var l []types.TypeIssue
		srcType := schema.MakeType()
		srcType.Name = srcTypeName
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			ty, issues := toddl.ToSpannerType(sessionState.Conv, spType, srcType)
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		ty, _ := toddl.ToSpannerType(sessionState.Conv, "", srcType)
		sqlserverDefaultTypeMap[srcTypeName] = ty
		sqlserverTypeMap[srcTypeName] = l
	}

	// Initialize oracleTypeMap.
	toddl = oracle.InfoSchemaImpl{}.GetToDdl()
	for _, srcTypeName := range []string{"NUMBER", "BFILE", "BLOB", "CHAR", "CLOB", "DATE", "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT", "LONG", "RAW", "LONG RAW", "NCHAR", "NVARCHAR2", "VARCHAR", "VARCHAR2", "NCLOB", "ROWID", "UROWID", "XMLTYPE", "TIMESTAMP", "INTERVAL", "SDO_GEOMETRY"} {
		var l []types.TypeIssue
		srcType := schema.MakeType()
		srcType.Name = srcTypeName
		for _, spType := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
			ty, issues := toddl.ToSpannerType(sessionState.Conv, spType, srcType)
			l = addTypeToList(ty.Name, spType, issues, l)
		}
		ty, _ := toddl.ToSpannerType(sessionState.Conv, "", srcType)
		oracleDefaultTypeMap[srcTypeName] = ty
		oracleTypeMap[srcTypeName] = l
	}
}

func addTypeToList(convertedType string, spType string, issues []internal.SchemaIssue, l []types.TypeIssue) []types.TypeIssue {
	if convertedType == spType {
		if len(issues) > 0 {
			var briefs []string
			for _, issue := range issues {
				briefs = append(briefs, reports.IssueDB[issue].Brief)
			}
			l = append(l, types.TypeIssue{T: spType, Brief: fmt.Sprintf(strings.Join(briefs, ", "))})
		} else {
			l = append(l, types.TypeIssue{T: spType})
		}
	}
	return l
}

func setShardIdColumnAsPrimaryKey(isAddedAtFirst bool) {
	sessionState := session.GetSessionState()
	for _, table := range sessionState.Conv.SpSchema {
		setShardIdColumnAsPrimaryKeyPerTable(isAddedAtFirst, table)
	}
}

func setShardIdColumnAsPrimaryKeyPerTable(isAddedAtFirst bool, table ddl.CreateTable) {
	pkRequest := primarykey.PrimaryKeyRequest{
		TableId: table.Id,
		Columns: []ddl.IndexKey{},
	}
	increment := 0
	if isAddedAtFirst {
		increment = 1
		pkRequest.Columns = append(pkRequest.Columns, ddl.IndexKey{ColId: table.ShardIdColumn, Order: 1})
	}
	for index := range table.PrimaryKeys {
		pk := table.PrimaryKeys[index]
		pkRequest.Columns = append(pkRequest.Columns, ddl.IndexKey{ColId: pk.ColId, Order: pk.Order + increment, Desc: pk.Desc})
	}
	if !isAddedAtFirst {
		size := len(table.PrimaryKeys)
		pkRequest.Columns = append(pkRequest.Columns, ddl.IndexKey{ColId: table.ShardIdColumn, Order: size + 1})
	}
	primarykey.UpdatePrimaryKeyAndSessionFile(pkRequest)
}

func addShardIdColumnToForeignKeys(isAddedAtFirst bool) {
	sessionState := session.GetSessionState()
	for _, table := range sessionState.Conv.SpSchema {
		addShardIdToForeignKeyPerTable(isAddedAtFirst, table)
	}
}

func addShardIdToForeignKeyPerTable(isAddedAtFirst bool, table ddl.CreateTable) {
	sessionState := session.GetSessionState()
	for i, fk := range table.ForeignKeys {
		referredTableShardIdColumn := sessionState.Conv.SpSchema[fk.ReferTableId].ShardIdColumn
		if isAddedAtFirst {
			fk.ColIds = append([]string{table.ShardIdColumn}, fk.ColIds...)
			fk.ReferColumnIds = append([]string{referredTableShardIdColumn}, fk.ReferColumnIds...)
		} else {
			fk.ColIds = append(fk.ColIds, table.ShardIdColumn)
			fk.ReferColumnIds = append(fk.ReferColumnIds, referredTableShardIdColumn)
		}
		sessionState.Conv.SpSchema[table.Id].ForeignKeys[i] = fk
	}
}

func initializeAutoGenMap() {
	sessionState := session.GetSessionState()
	autoGenMap = make(map[string][]types.AutoGen)
	switch sessionState.Conv.SpDialect {
	case constants.DIALECT_POSTGRESQL:
		makePostgresDialectAutoGenMap(sessionState.Conv.SpSequences)
		return
	default:
		makeGoogleSqlDialectAutoGenMap(sessionState.Conv.SpSequences)
		return
	}
}

func makePostgresDialectAutoGenMap(sequences map[string]ddl.Sequence) {
	for _, srcTypeName := range []string{ddl.Bool, ddl.Date, ddl.Float64, ddl.Int64, ddl.PGBytea, ddl.PGFloat8, ddl.PGInt8, ddl.PGJSONB, ddl.PGTimestamptz, ddl.PGVarchar, ddl.Numeric} {
		autoGenMap[srcTypeName] = []types.AutoGen{
			{
				Name:           "",
				GenerationType: "",
			},
		}
	}
	autoGenMap[ddl.PGVarchar] = append(autoGenMap[ddl.PGVarchar],
		types.AutoGen{
			Name:           "UUID",
			GenerationType: "Pre-defined",
		})

	typesSupportingSequences := []string{ddl.Float64, ddl.Int64, ddl.PGFloat8, ddl.PGInt8}
	for _, seq := range sequences {
		for _, srcTypeName := range typesSupportingSequences {
			autoGenMap[srcTypeName] = append(autoGenMap[srcTypeName],
				types.AutoGen{
					Name:           seq.Name,
					GenerationType: "Sequence",
				})
		}
	}
}

func makeGoogleSqlDialectAutoGenMap(sequences map[string]ddl.Sequence) {
	for _, srcTypeName := range []string{ddl.Bool, ddl.Bytes, ddl.Date, ddl.Float64, ddl.Int64, ddl.String, ddl.Timestamp, ddl.Numeric, ddl.JSON} {
		autoGenMap[srcTypeName] = []types.AutoGen{
			{
				Name:           "",
				GenerationType: "",
			},
		}
	}
	autoGenMap[ddl.String] = append(autoGenMap[ddl.String],
		types.AutoGen{
			Name:           "UUID",
			GenerationType: "Pre-defined",
		})

	typesSupportingSequences := []string{ddl.Float64, ddl.Int64}
	for _, seq := range sequences {
		for _, srcTypeName := range typesSupportingSequences {
			autoGenMap[srcTypeName] = append(autoGenMap[srcTypeName],
				types.AutoGen{
					Name:           seq.Name,
					GenerationType: "Sequence",
				})
		}
	}
}
