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
//
//	public constants first
//	key public type definitions next (although often it makes sense to put them next to public functions that use them)
//	then public functions (and relevant type definitions)
//	and helper functions and other non-public definitions last (generally in order of importance)
package conversion

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	datastream "cloud.google.com/go/datastream/apiv1"
	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/metrics"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"go.uber.org/zap"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var (
	// Set the maximum number of concurrent workers during foreign key creation.
	// This number should not be too high so as to not hit the AdminQuota limit.
	// AdminQuota limits are mentioned here: https://cloud.google.com/spanner/quotas#administrative_limits
	// If facing a quota limit error, consider reducing this value.
	MaxWorkers       = 50
	once             sync.Once
	datastreamClient *datastream.Client
)

func getDatastreamClient(ctx context.Context) *datastream.Client {
	if datastreamClient == nil {
		once.Do(func() {
			datastreamClient, _ = datastream.NewClient(ctx)
		})
		return datastreamClient
	}
	return datastreamClient
}

type ConvInterface interface {
	SchemaConv(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, s SchemaFromSourceInterface) (*internal.Conv, error)
	DataConv(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, writeLimit int64, s SchemaFromSourceInterface) (*writer.BatchWriter, error) 
}
type ConvImpl struct {}

// SchemaConv performs the schema conversion
// The SourceProfile param provides the connection details to use the go SQL library.
func (ci *ConvImpl) SchemaConv(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, s SchemaFromSourceInterface) (*internal.Conv, error) {
	switch sourceProfile.Driver {
	case constants.POSTGRES, constants.MYSQL, constants.DYNAMODB, constants.SQLSERVER, constants.ORACLE:
		return s.schemaFromDatabase(sourceProfile, targetProfile, &GetInfoImpl{}, &common.SchemaToSpannerImpl{}, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
	case constants.PGDUMP, constants.MYSQLDUMP:
		return s.SchemaFromDump(sourceProfile.Driver, targetProfile.Conn.Sp.Dialect, ioHelper, &common.UtilsOrderImpl{}, &common.SchemaToSpannerImpl{}, &ProcessDumpByDialectImpl{})
	default:
		return nil, fmt.Errorf("schema conversion for driver %s not supported", sourceProfile.Driver)
	}
}

// DataConv performs the data conversion
// The SourceProfile param provides the connection details to use the go SQL library.
func (ci *ConvImpl) DataConv(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, writeLimit int64, s DataFromSourceInterface) (*writer.BatchWriter, error) {
	config := writer.BatchWriterConfig{
		BytesLimit: 100 * 1000 * 1000,
		WriteLimit: writeLimit,
		RetryLimit: 1000,
		Verbose:    internal.Verbose(),
	}
	switch sourceProfile.Driver {
	case constants.POSTGRES, constants.MYSQL, constants.DYNAMODB, constants.SQLSERVER, constants.ORACLE:
		return s.dataFromDatabase(ctx, sourceProfile, targetProfile, config, conv, client, &GetInfoImpl{}, &DataFromDatabaseImpl{}, &SnapshotMigrationImpl{})
	case constants.PGDUMP, constants.MYSQLDUMP:
		if conv.SpSchema.CheckInterleaved() {
			return nil, fmt.Errorf("spanner migration tool does not currently support data conversion from dump files\nif the schema contains interleaved tables. Suggest using direct access to source database\ni.e. using drivers postgres and mysql")
		}
		return s.dataFromDump(sourceProfile.Driver, config, ioHelper, client, conv, dataOnly, &common.UtilsOrderImpl{}, &common.SchemaToSpannerImpl{}, &ProcessDumpByDialectImpl{}, &PopulateDataConvImpl{})
	case constants.CSV:
		return s.dataFromCSV(ctx, sourceProfile, targetProfile, config, conv, client, &PopulateDataConvImpl{}, &csv.CsvImpl{})
	default:
		return nil, fmt.Errorf("data conversion for driver %s not supported", sourceProfile.Driver)
	}
}


// Report generates a report of schema and data conversion.
func Report(driver string, badWrites map[string]int64, BytesRead int64, banner string, conv *internal.Conv, reportFileName string, dbName string, out *os.File) {

	//Write the structured report file
	structuredReportFileName := fmt.Sprintf("%s.%s", reportFileName, "structured_report.json")
	structuredReport := reports.GenerateStructuredReport(driver, dbName, conv, badWrites, true, true)
	fBytes, _ := json.MarshalIndent(structuredReport, "", " ")
	f, err := os.Create(structuredReportFileName)
	if err != nil {
		fmt.Fprintf(out, "Can't write out structured report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(out, "Writing report to stdout\n")
		f = out
	} else {
		defer f.Close()
	}
	f.Write(fBytes)

	//Write the text report file from the structured report
	textReportFileName := fmt.Sprintf("%s.%s", reportFileName, "report.txt")
	f, err = os.Create(textReportFileName)
	if err != nil {
		fmt.Fprintf(out, "Can't write out report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(out, "Writing report to stdout\n")
		f = out
	} else {
		defer f.Close()
	}
	w := bufio.NewWriter(f)
	w.WriteString(banner)
	reports.GenerateTextReport(structuredReport, w)
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
		fmt.Fprint(out, structuredReport.Summary.Text)
		fmt.Fprintf(out, "See file '%s' for details of the schema and data conversions.\n", reportFileName)
	}
}

// CreatesOrUpdatesDatabase updates an existing Spanner database or creates a new one if one does not exist.
func CreateOrUpdateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI, driver string, conv *internal.Conv, out *os.File, migrationType string) error {
	dbExists, err := VerifyDb(ctx, adminClient, dbURI)
	if err != nil {
		return err
	}
	if !conv.Audit.SkipMetricsPopulation {
		// Adding migration metadata to the outgoing context.
		migrationData := metrics.GetMigrationData(conv, driver, constants.SchemaConv)
		serializedMigrationData, _ := proto.Marshal(migrationData)
		migrationMetadataValue := base64.StdEncoding.EncodeToString(serializedMigrationData)
		ctx = metadata.AppendToOutgoingContext(ctx, constants.MigrationMetadataKey, migrationMetadataValue)
	}
	if dbExists {
		if conv.SpDialect != constants.DIALECT_POSTGRESQL && migrationType == constants.DATAFLOW_MIGRATION {
			return fmt.Errorf("spanner migration tool does not support minimal downtime schema/schema-and-data migrations to an existing database")
		}
		err := UpdateDatabase(ctx, adminClient, dbURI, conv, out, driver)
		if err != nil {
			return fmt.Errorf("can't update database schema: %v", err)
		}
	} else {
		err := CreateDatabase(ctx, adminClient, dbURI, conv, out, driver, migrationType)
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
func CreateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string, migrationType string) error {
	project, instance, dbName := utils.ParseDbURI(dbURI)
	fmt.Fprintf(out, "Creating new database %s in instance %s with default permissions ... \n", dbName, instance)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	req := &adminpb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	}
	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		// PostgreSQL dialect doesn't support:
		// a) backticks around the database name, and
		// b) DDL statements as part of a CreateDatabase operation (so schema
		// must be set using a separate UpdateDatabase operation).
		req.CreateStatement = "CREATE DATABASE \"" + dbName + "\""
		req.DatabaseDialect = adminpb.DatabaseDialect_POSTGRESQL
	} else {
		req.CreateStatement = "CREATE DATABASE `" + dbName + "`"
		if migrationType == constants.DATAFLOW_MIGRATION {
			req.ExtraStatements = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver})
		} else {
			req.ExtraStatements = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false, SpDialect: conv.SpDialect, Source: driver})
		}

	}

	op, err := adminClient.CreateDatabase(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build CreateDatabaseRequest: %w", utils.AnalyzeError(err, dbURI))
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("createDatabase call failed: %w", utils.AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Created database successfully.\n")

	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		// Update schema separately for PG databases.
		return UpdateDatabase(ctx, adminClient, dbURI, conv, out, driver)
	}
	return nil
}

// UpdateDatabase updates an existing spanner database.
func UpdateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string) error {
	fmt.Fprintf(out, "Updating schema for %s with default permissions ... \n", dbURI)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	schema := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false, SpDialect: conv.SpDialect, Source: driver})
	req := &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbURI,
		Statements: schema,
	}
	// Update queries for postgres as target db return response after more
	// than 1 min for large schemas, therefore, timeout is specified as 5 minutes
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
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
func UpdateDDLForeignKeys(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string, migrationType string) error {

	if conv.SpDialect != constants.DIALECT_POSTGRESQL && migrationType == constants.DATAFLOW_MIGRATION {
		//foreign keys were applied as part of CreateDatabase
		return nil
	}

	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	fkStmts := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: false, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver})
	if len(fkStmts) == 0 {
		return nil
	}
	if len(fkStmts) > 50 {
		fmt.Println(`
Warning: Large number of foreign keys detected. Spanner can take a long amount of 
time to create foreign keys (over 5 mins per batch of Foreign Keys even with no data). 
Spanner migration tool does not have control over a single foreign key creation time. The number 
of concurrent Foreign Key Creation Requests sent to spanner can be increased by 
tweaking the MaxWorkers variable (https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/conversion/conversion.go#L89).
However, setting it to a very high value might lead to exceeding the admin quota limit. Spanner migration tool tries to stay under the
admin quota limit by spreading the FK creation requests over time.`)
	}
	msg := fmt.Sprintf("Updating schema of database %s with foreign key constraints ...", dbURI)
	conv.Audit.Progress = *internal.NewProgress(int64(len(fkStmts)), msg, internal.Verbose(), true, int(internal.ForeignKeyUpdateInProgress))

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
				conv.Audit.Progress.MaybeReport(progress)
				progressMutex.Unlock()
				workers <- workerID
			}()
			internal.VerbosePrintf("Submitting new FK create request: %s\n", fkStmt)
			logger.Log.Debug("Submitting new FK create request", zap.String("fkStmt", fkStmt))

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
			logger.Log.Debug("Updated schema with statement", zap.String("fkStmt", fkStmt))
		}(fkStmt, workerID)
		// Send out an FK creation request every second, with total of maxWorkers request being present in a batch.
		time.Sleep(time.Second)
	}
	// Wait for all the goroutines to finish.
	for i := 1; i <= MaxWorkers; i++ {
		<-workers
	}
	conv.Audit.Progress.UpdateProgress("Foreign key update complete.", 100, internal.ForeignKeyUpdateComplete)
	conv.Audit.Progress.Done()
	return nil
}
