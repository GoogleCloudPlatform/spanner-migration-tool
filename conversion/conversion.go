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

// Package conversion handles initial setup for the command line tool
// and web APIs.

// TODO:(searce) Organize code in go style format to make this file more readable.
// 			public constants first
// 			key public type definitions next (although often it makes sense to put them next to public functions that use them)
// 			then public functions (and relevant type definitions)
// 			and helper functions and other non-public definitions last (generally in order of importance)
package conversion

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	dydb "github.com/aws/aws-sdk-go/service/dynamodb"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/metrics"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/sources/csv"
	"github.com/cloudspannerecosystem/harbourbridge/sources/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/sources/mysql"
	"github.com/cloudspannerecosystem/harbourbridge/sources/oracle"
	"github.com/cloudspannerecosystem/harbourbridge/sources/postgres"
	"github.com/cloudspannerecosystem/harbourbridge/sources/sqlserver"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/writer"
)

var (
	// Set the maximum number of concurrent workers during foreign key creation.
	// This number should not be too high so as to not hit the AdminQuota limit.
	// AdminQuota limits are mentioned here: https://cloud.google.com/spanner/quotas#administrative_limits
	// If facing a quota limit error, consider reducing this value.
	MaxWorkers = 20
)

const migrationMetadataKey = "cloud-spanner-migration-metadata"

// SchemaConv performs the schema conversion
// The SourceProfile param provides the connection details to use the go SQL library.
func SchemaConv(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams) (*internal.Conv, error) {
	switch sourceProfile.Driver {
	case constants.POSTGRES, constants.MYSQL, constants.DYNAMODB, constants.SQLSERVER, constants.ORACLE:
		return schemaFromDatabase(sourceProfile, targetProfile)
	case constants.PGDUMP, constants.MYSQLDUMP:
		return schemaFromDump(sourceProfile.Driver, targetProfile.TargetDb, ioHelper)
	default:
		return nil, fmt.Errorf("schema conversion for driver %s not supported", sourceProfile.Driver)
	}
}

// DataConv performs the data conversion
// The SourceProfile param provides the connection details to use the go SQL library.
func DataConv(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, writeLimit int64) (*writer.BatchWriter, error) {
	config := writer.BatchWriterConfig{
		BytesLimit: 100 * 1000 * 1000,
		WriteLimit: writeLimit,
		RetryLimit: 1000,
		Verbose:    internal.Verbose(),
	}
	switch sourceProfile.Driver {
	case constants.POSTGRES, constants.MYSQL, constants.DYNAMODB, constants.SQLSERVER, constants.ORACLE:
		return dataFromDatabase(sourceProfile, config, client, conv)
	case constants.PGDUMP, constants.MYSQLDUMP:
		if conv.SpSchema.CheckInterleaved() {
			return nil, fmt.Errorf("harbourBridge does not currently support data conversion from dump files\nif the schema contains interleaved tables. Suggest using direct access to source database\ni.e. using drivers postgres and mysql")
		}
		return dataFromDump(sourceProfile.Driver, config, ioHelper, client, conv, dataOnly)
	case constants.CSV:
		return dataFromCSV(ctx, sourceProfile, targetProfile, config, conv, client)
	default:
		return nil, fmt.Errorf("data conversion for driver %s not supported", sourceProfile.Driver)
	}
}

func connectionConfig(sourceProfile profiles.SourceProfile) (interface{}, error) {
	switch sourceProfile.Driver {
	// For PG and MYSQL, When called as part of the subcommand flow, host/user/db etc will
	// never be empty as we error out right during source profile creation. If any of them
	// are empty, that means this was called through the legacy cmd flow and we create the
	// string using env vars.
	case constants.POSTGRES:
		pgConn := sourceProfile.Conn.Pg
		if !(pgConn.Host != "" && pgConn.User != "" && pgConn.Db != "") {
			return profiles.GeneratePGSQLConnectionStr()
		} else {
			return profiles.GetSQLConnectionStr(sourceProfile), nil
		}
	case constants.MYSQL:
		// If empty, this is called as part of the legacy mode witih global CLI flags.
		// When using source-profile mode is used, the sqlConnectionStr is already populated.
		mysqlConn := sourceProfile.Conn.Mysql
		if !(mysqlConn.Host != "" && mysqlConn.User != "" && mysqlConn.Db != "") {
			return profiles.GenerateMYSQLConnectionStr()
		} else {
			return profiles.GetSQLConnectionStr(sourceProfile), nil
		}
	// For Dynamodb, both legacy and new flows use env vars.
	case constants.DYNAMODB:
		return getDynamoDBClientConfig()
	case constants.SQLSERVER:
		return profiles.GetSQLConnectionStr(sourceProfile), nil
	case constants.ORACLE:
		return profiles.GetSQLConnectionStr(sourceProfile), nil
	default:
		return "", fmt.Errorf("driver %s not supported", sourceProfile.Driver)
	}
}

func getDbNameFromSQLConnectionStr(driver, sqlConnectionStr string) string {
	switch driver {
	case constants.POSTGRES:
		dbParam := strings.Split(sqlConnectionStr, " ")[4]
		return strings.Split(dbParam, "=")[1]
	case constants.MYSQL:
		return strings.Split(sqlConnectionStr, ")/")[1]
	case constants.SQLSERVER:
		splts := strings.Split(sqlConnectionStr, "?database=")
		return splts[len(splts)-1]
	case constants.ORACLE:
		// connection string formate : "oracle://user:password@104.108.154.85:1521/XE"
		substr := sqlConnectionStr[9:]
		dbName := strings.Split(substr, ":")[0]
		return dbName
	}
	return ""
}

func schemaFromDatabase(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (*internal.Conv, error) {
	conv := internal.MakeConv()
	conv.TargetDb = targetProfile.TargetDb
	infoSchema, err := GetInfoSchema(sourceProfile)
	if err != nil {
		return conv, err
	}
	return conv, common.ProcessSchema(conv, infoSchema)
}

func dataFromDatabase(sourceProfile profiles.SourceProfile, config writer.BatchWriterConfig, client *sp.Client, conv *internal.Conv) (*writer.BatchWriter, error) {
	infoSchema, err := GetInfoSchema(sourceProfile)
	if err != nil {
		return nil, err
	}
	common.SetRowStats(conv, infoSchema)
	totalRows := conv.Rows()
	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false)
	batchWriter := populateDataConv(conv, config, client, p)
	common.ProcessData(conv, infoSchema)
	batchWriter.Flush()
	return batchWriter, nil
}

func getDynamoDBClientConfig() (*aws.Config, error) {
	cfg := aws.Config{}
	endpointOverride := os.Getenv("DYNAMODB_ENDPOINT_OVERRIDE")
	if endpointOverride != "" {
		cfg.Endpoint = aws.String(endpointOverride)
	}
	return &cfg, nil
}

func schemaFromDump(driver string, targetDb string, ioHelper *utils.IOStreams) (*internal.Conv, error) {
	f, n, err := getSeekable(ioHelper.In)
	if err != nil {
		utils.PrintSeekError(driver, err, ioHelper.Out)
		return nil, fmt.Errorf("can't get seekable input file")
	}
	ioHelper.SeekableIn = f
	ioHelper.BytesRead = n
	conv := internal.MakeConv()
	conv.TargetDb = targetDb
	p := internal.NewProgress(n, "Generating schema", internal.Verbose(), false)
	r := internal.NewReader(bufio.NewReader(f), p)
	conv.SetSchemaMode() // Build schema and ignore data in dump.
	conv.SetDataSink(nil)
	err = ProcessDump(driver, conv, r)
	if err != nil {
		fmt.Fprintf(ioHelper.Out, "Failed to parse the data file: %v", err)
		return nil, fmt.Errorf("failed to parse the data file")
	}
	p.Done()
	return conv, nil
}

func dataFromDump(driver string, config writer.BatchWriterConfig, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool) (*writer.BatchWriter, error) {
	// TODO: refactor of the way we handle getSeekable
	// to avoid the code duplication here
	if !dataOnly {
		_, err := ioHelper.SeekableIn.Seek(0, 0)
		if err != nil {
			fmt.Printf("\nCan't seek to start of file (preparation for second pass): %v\n", err)
			return nil, fmt.Errorf("can't seek to start of file")
		}
	} else {
		// Note: input file is kept seekable to plan for future
		// changes in showing progress for data migration.
		f, n, err := getSeekable(ioHelper.In)
		if err != nil {
			utils.PrintSeekError(driver, err, ioHelper.Out)
			return nil, fmt.Errorf("can't get seekable input file")
		}
		ioHelper.SeekableIn = f
		ioHelper.BytesRead = n
	}
	totalRows := conv.Rows()

	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false)
	r := internal.NewReader(bufio.NewReader(ioHelper.SeekableIn), nil)
	batchWriter := populateDataConv(conv, config, client, p)
	ProcessDump(driver, conv, r)
	batchWriter.Flush()
	p.Done()

	return batchWriter, nil
}

func dataFromCSV(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client) (*writer.BatchWriter, error) {
	if targetProfile.Conn.Sp.Dbname == "" {
		return nil, fmt.Errorf("dbName is mandatory in target-profile for csv source")
	}
	conv.TargetDb = targetProfile.ToLegacyTargetDb()
	dialect, err := targetProfile.FetchTargetDialect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch dialect: %v", err)
	}

	if utils.DialectToTarget(dialect) != conv.TargetDb {
		return nil, fmt.Errorf("dialect specified in target profile does not match spanner dialect")
	}

	delimiterStr := sourceProfile.Csv.Delimiter
	if len(delimiterStr) != 1 {
		return nil, fmt.Errorf("delimiter should only be a single character long, found '%s'", delimiterStr)
	}

	delimiter := rune(delimiterStr[0])

	err = utils.ReadSpannerSchema(ctx, conv, client)
	if err != nil {
		return nil, fmt.Errorf("error trying to read and convert spanner schema: %v", err)
	}

	tables, err := csv.GetCSVFiles(conv, sourceProfile)
	if err != nil {
		return nil, fmt.Errorf("error finding csv files: %v", err)
	}

	// Find the number of rows in each csv file for generating stats.
	err = csv.SetRowStats(conv, tables, delimiter)
	if err != nil {
		return nil, err
	}

	totalRows := conv.Rows()
	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false)
	batchWriter := populateDataConv(conv, config, client, p)
	err = csv.ProcessCSV(conv, tables, sourceProfile.Csv.NullStr, delimiter)
	if err != nil {
		return nil, fmt.Errorf("can't process csv: %v", err)
	}
	batchWriter.Flush()
	p.Done()
	return batchWriter, nil
}

func populateDataConv(conv *internal.Conv, config writer.BatchWriterConfig, client *sp.Client, progress *internal.Progress) *writer.BatchWriter {
	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		migrationData := metrics.GetMigrationData(conv, "", "", constants.DataConv)
		serializedMigrationData, _ := proto.Marshal(migrationData)
		migrationMetadataValue := base64.StdEncoding.EncodeToString(serializedMigrationData)
		_, err := client.Apply(metadata.AppendToOutgoingContext(context.Background(), migrationMetadataKey, migrationMetadataValue), m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		progress.MaybeReport(atomic.LoadInt64(&rows))
		return nil
	}
	batchWriter := writer.NewBatchWriter(config)
	conv.SetDataMode()
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			batchWriter.AddRow(table, cols, vals)
		})
	conv.DataFlush = func() {
		batchWriter.Flush()
	}
	return batchWriter
}

// Report generates a report of schema and data conversion.
func Report(driver string, badWrites map[string]int64, BytesRead int64, banner string, conv *internal.Conv, reportFileName string, out *os.File) {
	f, err := os.Create(reportFileName)
	if err != nil {
		fmt.Fprintf(out, "Can't write out report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(out, "Writing report to stdout\n")
		f = out
	} else {
		defer f.Close()
	}
	w := bufio.NewWriter(f)
	w.WriteString(banner)

	summary := internal.GenerateReport(driver, conv, w, badWrites, true, true)
	w.Flush()
	var isDump bool
	if strings.Contains(driver, "dump") {
		isDump = true
	}
	if isDump {
		fmt.Fprintf(out, "Processed %d bytes of %s data (%d statements, %d rows of data, %d errors, %d unexpected conditions).\n",
			BytesRead, driver, conv.Statements(), conv.Rows(), conv.StatementErrors(), conv.Unexpecteds())
	} else {
		fmt.Fprintf(out, "Processed source database via %s driver (%d rows of data, %d unexpected conditions).\n",
			driver, conv.Rows(), conv.Unexpecteds())
	}
	// We've already written summary to f (as part of GenerateReport).
	// In the case where f is stdout, don't write a duplicate copy.
	if f != out {
		fmt.Fprint(out, summary)
		fmt.Fprintf(out, "See file '%s' for details of the schema and data conversions.\n", reportFileName)
	}
}

// getSeekable returns a seekable file (with same content as f) and the size of the content (in bytes).
func getSeekable(f *os.File) (*os.File, int64, error) {
	_, err := f.Seek(0, 0)
	if err == nil { // Stdin is seekable, let's just use that. This happens when you run 'cmd < file'.
		n, err := utils.GetFileSize(f)
		return f, n, err
	}
	internal.VerbosePrintln("Creating a tmp file with a copy of stdin because stdin is not seekable.")

	// Create file in os.TempDir. Its not clear this is a good idea e.g. if the
	// pg_dump/mysqldump output is large (tens of GBs) and os.TempDir points to a directory
	// (such as /tmp) that's configured with a small amount of disk space.
	// To workaround such limits on Unix, set $TMPDIR to a directory with lots
	// of disk space.
	fcopy, err := ioutil.TempFile("", "harbourbridge.data")
	if err != nil {
		return nil, 0, err
	}
	syscall.Unlink(fcopy.Name()) // File will be deleted when this process exits.
	_, err = io.Copy(fcopy, f)
	if err != nil {
		return nil, 0, fmt.Errorf("can't write stdin to tmp file: %w", err)
	}
	_, err = fcopy.Seek(0, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("can't reset file offset: %w", err)
	}
	n, _ := utils.GetFileSize(fcopy)
	return fcopy, n, nil
}

// VerifyDb checks whether the db exists and if it does, verifies if the schema is what we currently support.
// Timeout limit for request is set to 2 minutes to handle unsupported spanner API endpoints.
func VerifyDb(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string) (dbExists bool, err error) {
	requestTimeout := 2 * time.Minute
	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	dbExists, err = CheckExistingDb(reqCtx, adminClient, dbURI)
	if err != nil {
		return dbExists, err
	}
	if dbExists {
		err = ValidateDDL(ctx, adminClient, dbURI)
	}
	return dbExists, err
}

// CheckExistingDb checks whether the database with dbURI exists or not. If request is not completed within
// context deadline then it will return an context error.
func CheckExistingDb(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string) (bool, error) {
	_, err := adminClient.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: dbURI})
	if err != nil {
		if err == ctx.Err() {
			return false, fmt.Errorf("spanner API endpoint unavailable: make sure that spanner api endpoint is configured properly")
		}
		if utils.ContainsAny(strings.ToLower(err.Error()), []string{"database not found"}) {
			return false, nil
		}
		return false, fmt.Errorf("can't get database info: %s", err)
	}
	return true, nil
}

// ValidateDDL verifies if an existing DB's ddl follows what is supported by harbourbridge. Currently,
// we only support empty schema when db already exists.
func ValidateDDL(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string) error {
	dbDdl, err := adminClient.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{Database: dbURI})
	if err != nil {
		return fmt.Errorf("can't fetch database ddl: %v", err)
	}
	if len(dbDdl.Statements) != 0 {
		return fmt.Errorf("harbourBridge supports writing to existing databases only if they have an empty schema")
	}
	return nil
}

// CreatesOrUpdatesDatabase updates an existing Spanner database or creates a new one if one does not exist.
func CreateOrUpdateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI, driver, targetDb string, conv *internal.Conv, out *os.File) error {
	dbExists, err := VerifyDb(ctx, adminClient, dbURI)
	if err != nil {
		return err
	}
	// Adding migration metadata to the outgoing context.
	migrationData := metrics.GetMigrationData(conv, driver, targetDb, constants.SchemaConv)
	serializedMigrationData, _ := proto.Marshal(migrationData)
	migrationMetadataValue := base64.StdEncoding.EncodeToString(serializedMigrationData)
	ctx = metadata.AppendToOutgoingContext(ctx, migrationMetadataKey, migrationMetadataValue)
	if dbExists {
		err := UpdateDatabase(ctx, adminClient, dbURI, conv, out)
		if err != nil {
			return fmt.Errorf("can't update database schema: %v", err)
		}
	} else {
		err := CreateDatabase(ctx, adminClient, dbURI, conv, out)
		if err != nil {
			return fmt.Errorf("can't create database: %v", err)
		}
	}
	return nil
}

// CreateDatabase returns a newly create Spanner DB.
// It automatically determines an appropriate project, selects a
// Spanner instance to use, generates a new Spanner DB name,
// and call into the Spanner admin interface to create the new DB.
func CreateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File) error {
	project, instance, dbName := utils.ParseDbURI(dbURI)
	fmt.Fprintf(out, "Creating new database %s in instance %s with default permissions ... \n", dbName, instance)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	req := &adminpb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	}
	if conv.TargetDb == constants.TargetExperimentalPostgres {
		// TargetExperimentalPostgres doesn't support:
		// a) backticks around the database name, and
		// b) DDL statements as part of a CreateDatabase operation (so schema
		// must be set using a separate UpdateDatabase operation).
		req.CreateStatement = "CREATE DATABASE \"" + dbName + "\""
		req.DatabaseDialect = adminpb.DatabaseDialect_POSTGRESQL
	} else {
		req.CreateStatement = "CREATE DATABASE `" + dbName + "`"
		req.ExtraStatements = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false, TargetDb: conv.TargetDb})
	}

	op, err := adminClient.CreateDatabase(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build CreateDatabaseRequest: %w", utils.AnalyzeError(err, dbURI))
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("createDatabase call failed: %w", utils.AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Created database successfully.\n")

	if conv.TargetDb == constants.TargetExperimentalPostgres {
		// Update schema separately for PG databases.
		return UpdateDatabase(ctx, adminClient, dbURI, conv, out)
	}
	return nil
}

// UpdateDatabase updates an existing spanner database.
func UpdateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File) error {
	fmt.Fprintf(out, "Updating schema for %s with default permissions ... \n", dbURI)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	schema := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false, TargetDb: conv.TargetDb})
	req := &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbURI,
		Statements: schema,
	}
	op, err := adminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build UpdateDatabaseDdlRequest: %w", utils.AnalyzeError(err, dbURI))
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("UpdateDatabaseDdl call failed: %w", utils.AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Updated schema successfully.\n")
	return nil
}

// UpdateDDLForeignKeys updates the Spanner database with foreign key
// constraints using ALTER TABLE statements.
func UpdateDDLForeignKeys(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File) error {
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	fkStmts := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: false, ForeignKeys: true, TargetDb: conv.TargetDb})
	if len(fkStmts) == 0 {
		return nil
	}
	if len(fkStmts) > 50 {
		fmt.Println(`
Warning: Large number of foreign keys detected. Spanner can take a long amount of 
time to create foreign keys (over 5 mins per batch of Foreign Keys even with no data). 
Harbourbridge does not have control over a single foreign key creation time. The number 
of concurrent Foreign Key Creation Requests sent to spanner can be increased by 
tweaking the MaxWorkers variable (https://github.com/cloudspannerecosystem/harbourbridge/blob/master/conversion/conversion.go#L89).
However, setting it to a very high value might lead to exceeding the admin quota limit.
Recommended value is between 20-30.`)
	}
	msg := fmt.Sprintf("Updating schema of database %s with foreign key constraints ...", dbURI)
	p := internal.NewProgress(int64(len(fkStmts)), msg, internal.Verbose(), true)

	workers := make(chan int, MaxWorkers)
	for i := 1; i <= MaxWorkers; i++ {
		workers <- i
	}
	var progressMutex sync.Mutex
	progress := int64(0)

	// We dispatch parallel foreign key create requests to ensure the backfill runs in parallel to reduce overall time.
	// This cuts down the time taken to a third (approx) compared to Serial and Batched creation. We also do not want to create
	// too many requests and get throttled due to network or hitting catalog memory limits.
	// Ensure atmost `MaxWorkers` go routines run in parallel that each update the ddl with one foreign key statement.
	for _, fkStmt := range fkStmts {
		workerID := <-workers
		go func(fkStmt string, workerID int) {
			defer func() {
				// Locking the progress reporting otherwise progress results displayed could be in random order.
				progressMutex.Lock()
				progress++
				p.MaybeReport(progress)
				progressMutex.Unlock()
				workers <- workerID
			}()
			internal.VerbosePrintf("Submitting new FK create request: %s\n", fkStmt)
			op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
				Database:   dbURI,
				Statements: []string{fkStmt},
			})
			if err != nil {
				fmt.Printf("Cannot submit request for create foreign key with statement: %s\n due to error: %s. Skipping this foreign key...\n", fkStmt, err)
				conv.Unexpected(fmt.Sprintf("Can't add foreign key with statement %s: %s", fkStmt, err))
				return
			}
			if err := op.Wait(ctx); err != nil {
				fmt.Printf("Can't add foreign key with statement: %s\n due to error: %s. Skipping this foreign key...\n", fkStmt, err)
				conv.Unexpected(fmt.Sprintf("Can't add foreign key with statement %s: %s", fkStmt, err))
				return
			}
			internal.VerbosePrintln("Updated schema with statement: " + fkStmt)
		}(fkStmt, workerID)
	}
	// Wait for all the goroutines to finish.
	for i := 1; i <= MaxWorkers; i++ {
		<-workers
	}
	p.Done()
	return nil
}

// WriteSchemaFile writes DDL statements in a file. It includes CREATE TABLE
// statements and ALTER TABLE statements to add foreign keys.
// The parameter name should end with a .txt.
func WriteSchemaFile(conv *internal.Conv, now time.Time, name string, out *os.File) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create schema file %s: %v\n", name, err)
		return
	}

	// The schema file we write out below is optimized for reading. It includes comments, foreign keys
	// and doesn't add backticks around table and column names. This file is
	// intended for explanatory and documentation purposes, and is not strictly
	// legal Cloud Spanner DDL (Cloud Spanner doesn't currently support comments).
	spDDL := conv.SpSchema.GetDDL(ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true, TargetDb: conv.TargetDb})
	if len(spDDL) == 0 {
		spDDL = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l := []string{
		fmt.Sprintf("-- Schema generated %s\n", now.Format("2006-01-02 15:04:05")),
		strings.Join(spDDL, ";\n\n"),
		"\n",
	}
	if _, err := f.WriteString(strings.Join(l, "")); err != nil {
		fmt.Fprintf(out, "Can't write out schema file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote schema to file '%s'.\n", name)

	// Convert <file_name>.<ext> to <file_name>.ddl.<ext>.
	nameSplit := strings.Split(name, ".")
	nameSplit = append(nameSplit[:len(nameSplit)-1], "ddl", nameSplit[len(nameSplit)-1])
	name = strings.Join(nameSplit, ".")
	f, err = os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create legal schema ddl file %s: %v\n", name, err)
		return
	}

	// We change 'Comments' to false and 'ProtectIds' to true below to write out a
	// schema file that is a legal Cloud Spanner DDL.
	spDDL = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: true, TargetDb: conv.TargetDb})
	if len(spDDL) == 0 {
		spDDL = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l = []string{
		strings.Join(spDDL, ";\n\n"),
		"\n",
	}
	if _, err = f.WriteString(strings.Join(l, "")); err != nil {
		fmt.Fprintf(out, "Can't write out legal schema ddl file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote legal schema ddl to file '%s'.\n", name)
}

// WriteSessionFile writes conv struct to a file in JSON format.
func WriteSessionFile(conv *internal.Conv, name string, out *os.File) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create session file %s: %v\n", name, err)
		return
	}
	// Session file will basically contain 'conv' struct in JSON format.
	// It contains all the information for schema and data conversion state.
	convJSON, err := json.MarshalIndent(conv, "", " ")
	if err != nil {
		fmt.Fprintf(out, "Can't encode session state to JSON: %v\n", err)
		return
	}
	if _, err := f.Write(convJSON); err != nil {
		fmt.Fprintf(out, "Can't write out session file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote session to file '%s'.\n", name)
}

// WriteConvGeneratedFiles creates a directory labeled downloads with the current timestamp
// where it writes the sessionfile, report summary and DDLs then returns the directory where it writes.
func WriteConvGeneratedFiles(conv *internal.Conv, dbName string, driver string, BytesRead int64, out *os.File) (string, error) {
	now := time.Now()
	dirPath := "harbour_bridge_output/" + dbName + "/"
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		fmt.Fprintf(out, "Can't create directory %s: %v\n", dirPath, err)
		return "", err
	}
	schemaFileName := dirPath + dbName + "_schema.txt"
	WriteSchemaFile(conv, now, schemaFileName, out)
	reportFileName := dirPath + dbName + "_report.txt"
	Report(driver, nil, BytesRead, "", conv, reportFileName, out)
	sessionFileName := dirPath + dbName + ".session.json"
	WriteSessionFile(conv, sessionFileName, out)
	return dirPath, nil
}

// ReadSessionFile reads a session JSON file and
// unmarshal it's content into *internal.Conv.
func ReadSessionFile(conv *internal.Conv, sessionJSON string) error {
	s, err := ioutil.ReadFile(sessionJSON)
	if err != nil {
		return err
	}
	err = json.Unmarshal(s, &conv)
	if err != nil {
		return err
	}
	return nil
}

// WriteBadData prints summary stats about bad rows and writes detailed info
// to file 'name'.
func WriteBadData(bw *writer.BatchWriter, conv *internal.Conv, banner, name string, out *os.File) {
	badConversions := conv.BadRows()
	badWrites := utils.SumMapValues(bw.DroppedRowsByTable())
	if badConversions == 0 && badWrites == 0 {
		os.Remove(name) // Cleanup bad-data file from previous run.
		return
	}
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
		return
	}
	f.WriteString(banner)
	maxRows := 100
	if badConversions > 0 {
		l := conv.SampleBadRows(maxRows)
		if int64(len(l)) < badConversions {
			f.WriteString("A sample of rows that generated conversion errors:\n")
		} else {
			f.WriteString("Rows that generated conversion errors:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	if badWrites > 0 {
		l := bw.SampleBadRows(maxRows)
		if int64(len(l)) < badWrites {
			f.WriteString("A sample of rows that successfully converted but couldn't be written to Spanner:\n")
		} else {
			f.WriteString("Rows that successfully converted but couldn't be written to Spanner:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	fmt.Fprintf(out, "See file '%s' for details of bad rows\n", name)
}

// ProcessDump invokes process dump function from a sql package based on driver selected.
func ProcessDump(driver string, conv *internal.Conv, r *internal.Reader) error {
	switch driver {
	case constants.MYSQLDUMP:
		return common.ProcessDbDump(conv, r, mysql.DbDumpImpl{})
	case constants.PGDUMP:
		return common.ProcessDbDump(conv, r, postgres.DbDumpImpl{})
	default:
		return fmt.Errorf("process dump for driver %s not supported", driver)
	}
}

func GetInfoSchema(sourceProfile profiles.SourceProfile) (common.InfoSchema, error) {
	connectionConfig, err := connectionConfig(sourceProfile)
	if err != nil {
		return nil, err
	}
	driver := sourceProfile.Driver
	switch driver {
	case constants.MYSQL:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return mysql.InfoSchemaImpl{DbName: dbName, Db: db}, nil
	case constants.POSTGRES:
		db, err := sql.Open(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return postgres.InfoSchemaImpl{Db: db}, nil
	case constants.DYNAMODB:
		mySession := session.Must(session.NewSession())
		dydbClient := dydb.New(mySession, connectionConfig.(*aws.Config))
		return dynamodb.InfoSchemaImpl{DynamoClient: dydbClient, SampleSize: profiles.GetSchemaSampleSize(sourceProfile)}, nil
	case constants.SQLSERVER:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return sqlserver.InfoSchemaImpl{DbName: dbName, Db: db}, nil
	case constants.ORACLE:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return oracle.InfoSchemaImpl{DbName: strings.ToUpper(dbName), Db: db}, nil
	default:
		return nil, fmt.Errorf("driver %s not supported", driver)
	}
}
