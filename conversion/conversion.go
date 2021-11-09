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
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	dydb "github.com/aws/aws-sdk-go/service/dynamodb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/sources/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/sources/mysql"
	"github.com/cloudspannerecosystem/harbourbridge/sources/postgres"
	"github.com/cloudspannerecosystem/harbourbridge/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

var (
	// Set the maximum number of concurrent workers during foreign key creation.
	// This number should not be too high so as to not hit the AdminQuota limit.
	// AdminQuota limits are mentioned here: https://cloud.google.com/spanner/quotas#administrative_limits
	// If facing a quota limit error, consider reducing this value.
	MaxWorkers = 20
)

func SchemaConv(driver string, targetDb string, ioHelper *IOStreams, schemaSampleSize int64) (*internal.Conv, error) {
	switch driver {
	case constants.POSTGRES, constants.MYSQL:
		return schemaFromSQL(driver, targetDb)
	case constants.PGDUMP, constants.MYSQLDUMP:
		return schemaFromDump(driver, targetDb, ioHelper)
	case constants.DYNAMODB:
		return schemaFromDynamoDB(schemaSampleSize)
	default:
		return nil, fmt.Errorf("schema conversion for driver %s not supported", driver)
	}
}

func DataConv(driver string, ioHelper *IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool) (*spanner.BatchWriter, error) {
	config := spanner.BatchWriterConfig{
		BytesLimit: 100 * 1000 * 1000,
		WriteLimit: 40,
		RetryLimit: 1000,
		Verbose:    internal.Verbose(),
	}
	switch driver {
	case constants.POSTGRES, constants.MYSQL:
		return dataFromSQL(driver, config, client, conv)
	case constants.PGDUMP, constants.MYSQLDUMP:
		if conv.SpSchema.CheckInterleaved() {
			return nil, fmt.Errorf("HarbourBridge does not currently support data conversion from dump files\nif the schema contains interleaved tables. Suggest using direct access to source database\ni.e. using drivers postgres and mysql.")
		}
		return dataFromDump(driver, config, ioHelper, client, conv, dataOnly)
	case constants.DYNAMODB:
		return dataFromDynamoDB(config, client, conv)
	default:
		return nil, fmt.Errorf("data conversion for driver %s not supported", driver)
	}
}

func driverConfig(driver string) (string, error) {
	switch driver {
	case constants.POSTGRES:
		return pgDriverConfig()
	case constants.MYSQL:
		return mysqlDriverConfig()
	default:
		return "", fmt.Errorf("Driver %s not supported", driver)
	}
}

func pgDriverConfig() (string, error) {
	server := os.Getenv("PGHOST")
	port := os.Getenv("PGPORT")
	user := os.Getenv("PGUSER")
	dbname := os.Getenv("PGDATABASE")
	if server == "" || port == "" || user == "" || dbname == "" {
		fmt.Printf("Please specify host, port, user and database using PGHOST, PGPORT, PGUSER and PGDATABASE environment variables\n")
		return "", fmt.Errorf("Could not connect to source database")
	}
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = getPassword()
	}
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", server, port, user, password, dbname), nil
}

func mysqlDriverConfig() (string, error) {
	server := os.Getenv("MYSQLHOST")
	port := os.Getenv("MYSQLPORT")
	user := os.Getenv("MYSQLUSER")
	dbname := os.Getenv("MYSQLDATABASE")
	if server == "" || port == "" || user == "" || dbname == "" {
		fmt.Printf("Please specify host, port, user and database using MYSQLHOST, MYSQLPORT, MYSQLUSER and MYSQLDATABASE environment variables\n")
		return "", fmt.Errorf("Could not connect to source database")
	}
	password := os.Getenv("MYSQLPWD")
	if password == "" {
		password = getPassword()
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, server, port, dbname), nil
}

func schemaFromSQL(driver string, targetDb string) (*internal.Conv, error) {
	driverConfig, err := driverConfig(driver)
	if err != nil {
		return nil, err
	}
	sourceDB, err := sql.Open(driver, driverConfig)
	if err != nil {
		return nil, err
	}
	conv := internal.MakeConv()
	conv.TargetDb = targetDb
	err = ProcessInfoSchema(driver, conv, sourceDB)
	if err != nil {
		return nil, err
	}
	return conv, nil
}

func dataFromSQL(driver string, config spanner.BatchWriterConfig, client *sp.Client, conv *internal.Conv) (*spanner.BatchWriter, error) {
	// TODO: Refactor to avoid redundant calls to driverConfig and
	// Open in schemaFromSQL and dataFromSQL. Also refactor to
	// share code with dataFromPgDump. Use single transaction for
	// reading schema and data from source db to get consistent
	// dump.
	driverConfig, err := driverConfig(driver)
	if err != nil {
		return nil, err
	}
	sourceDB, err := sql.Open(driver, driverConfig)
	if err != nil {
		return nil, err
	}
	err = SetRowStats(driver, conv, sourceDB)
	if err != nil {
		return nil, err
	}
	totalRows := conv.Rows()
	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false)
	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		_, err := client.Apply(context.Background(), m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		p.MaybeReport(atomic.LoadInt64(&rows))
		return nil
	}
	writer := spanner.NewBatchWriter(config)
	conv.SetDataMode()
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			writer.AddRow(table, cols, vals)
		})
	err = ProcessSQLData(driver, conv, sourceDB)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	return writer, nil
}

func getDynamoDBClientConfig() *aws.Config {
	cfg := aws.Config{}
	endpointOverride := os.Getenv("DYNAMODB_ENDPOINT_OVERRIDE")
	if endpointOverride != "" {
		cfg.Endpoint = aws.String(endpointOverride)
	}
	return &cfg
}

func schemaFromDynamoDB(sampleSize int64) (*internal.Conv, error) {
	conv := internal.MakeConv()
	mySession := session.Must(session.NewSession())
	dydbClient := dydb.New(mySession, getDynamoDBClientConfig())
	err := dynamodb.ProcessSchema(conv, dydbClient, []string{}, sampleSize)
	if err != nil {
		return nil, err
	}
	return conv, nil
}

func dataFromDynamoDB(config spanner.BatchWriterConfig, client *sp.Client, conv *internal.Conv) (*spanner.BatchWriter, error) {
	mySession := session.Must(session.NewSession())
	dydbClient := dydb.New(mySession, getDynamoDBClientConfig())
	dynamodb.SetRowStats(conv, dydbClient)
	totalRows := conv.Rows()
	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false)

	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		_, err := client.Apply(context.Background(), m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		p.MaybeReport(atomic.LoadInt64(&rows))
		return nil
	}
	writer := spanner.NewBatchWriter(config)
	conv.SetDataMode()
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			writer.AddRow(table, cols, vals)
		})

	err := dynamodb.ProcessData(conv, dydbClient)
	if err != nil {
		return nil, err
	}
	writer.Flush()
	return writer, nil
}

type IOStreams struct {
	In, SeekableIn, Out *os.File
	BytesRead           int64
}

// NewIOStreams returns a new IOStreams struct such that input stream is set
// to open file descriptor for dumpFile if driver is PGDUMP or MYSQLDUMP.
// Input stream defaults to stdin. Output stream is always set to stdout.
func NewIOStreams(driver string, dumpFile string) IOStreams {
	io := IOStreams{In: os.Stdin, Out: os.Stdout}
	if (driver == constants.PGDUMP || driver == constants.MYSQLDUMP) && dumpFile != "" {
		fmt.Printf("\nLoading dump file from path: %s\n", dumpFile)
		f, err := os.Open(dumpFile)
		if err != nil {
			fmt.Printf("\nError reading dump file: %v err:%v\n", dumpFile, err)
			log.Fatal(err)
		}
		io.In = f
	}
	return io
}

func schemaFromDump(driver string, targetDb string, ioHelper *IOStreams) (*internal.Conv, error) {
	f, n, err := getSeekable(ioHelper.In)
	if err != nil {
		printSeekError(driver, err, ioHelper.Out)
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

func dataFromDump(driver string, config spanner.BatchWriterConfig, ioHelper *IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool) (*spanner.BatchWriter, error) {
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
			printSeekError(driver, err, ioHelper.Out)
			return nil, fmt.Errorf("can't get seekable input file")
		}
		ioHelper.SeekableIn = f
		ioHelper.BytesRead = n
	}
	totalRows := conv.Rows()

	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false)
	r := internal.NewReader(bufio.NewReader(ioHelper.SeekableIn), nil)
	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		_, err := client.Apply(context.Background(), m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		p.MaybeReport(atomic.LoadInt64(&rows))
		return nil
	}
	writer := spanner.NewBatchWriter(config)
	conv.SetDataMode() // Process data in dump; schema is unchanged.
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			writer.AddRow(table, cols, vals)
		})
	ProcessDump(driver, conv, r)
	writer.Flush()
	p.Done()

	return writer, nil
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
		n, err := getSize(f)
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
	n, err := getSize(fcopy)
	return fcopy, n, nil
}

// VerifyDb checks whether the db exists and if it does, verifies if the schema is what we currently support.
func VerifyDb(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string) (dbExists bool, err error) {
	dbExists, err = CheckExistingDb(ctx, adminClient, dbURI)
	if err != nil {
		return dbExists, err
	}
	if dbExists {
		err = ValidateDDL(ctx, adminClient, dbURI)
	}
	return dbExists, err
}

// CheckExistingDb checks whether the database with dbURI exists or not.
func CheckExistingDb(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string) (bool, error) {
	_, err := adminClient.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: dbURI})
	if err != nil {
		if containsAny(strings.ToLower(err.Error()), []string{"database not found"}) {
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
		return fmt.Errorf("HarbourBridge supports writing to existing databases only if they have an empty schema")
	}
	return nil
}

func CreateOrUpdateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File) error {
	dbExists, err := VerifyDb(ctx, adminClient, dbURI)
	if err != nil {
		return err
	}
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
	project, instance, dbName := parseDbURI(dbURI)
	fmt.Fprintf(out, "Creating new database %s in instance %s with default permissions ... \n", dbName, instance)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	schema := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false})
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: "CREATE DATABASE `" + dbName + "`",
		ExtraStatements: schema,
	})
	if err != nil {
		return fmt.Errorf("can't build CreateDatabaseRequest: %w", AnalyzeError(err, dbURI))
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("createDatabase call failed: %w", AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Created database successfully.\n")
	return nil
}

func UpdateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File) error {
	fmt.Fprintf(out, "Updating schema for %s with default permissions ... \n", dbURI)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbURI,
		Statements: conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false}),
	})
	if err != nil {
		return fmt.Errorf("can't build UpdateDatabaseDdlRequest: %w", AnalyzeError(err, dbURI))
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("UpdateDatabaseDdl call failed: %w", AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Updated schema successfully.\n")
	return nil
}

// parseURI parses an unknown URI string that could be a database, instance or project URI.
func parseURI(URI string) (project, instance, dbName string) {
	project, instance, dbName = "", "", ""
	if strings.Contains(URI, "databases") {
		project, instance, dbName = parseDbURI(URI)
	} else if strings.Contains(URI, "instances") {
		project, instance = parseInstanceURI(URI)
	} else if strings.Contains(URI, "projects") {
		project = parseProjectURI(URI)
	}
	return
}

func parseDbURI(dbURI string) (project, instance, dbName string) {
	split := strings.Split(dbURI, "/databases/")
	project, instance = parseInstanceURI(split[0])
	dbName = split[1]
	return
}

func parseInstanceURI(instanceURI string) (project, instance string) {
	split := strings.Split(instanceURI, "/instances/")
	project = parseProjectURI(split[0])
	instance = split[1]
	return
}

func parseProjectURI(projectURI string) (project string) {
	split := strings.Split(projectURI, "/")
	project = split[1]
	return
}

// UpdateDDLForeignKeys updates the Spanner database with foreign key
// constraints using ALTER TABLE statements.
func UpdateDDLForeignKeys(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File) error {
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	fkStmts := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: false, ForeignKeys: true})
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
		workerId := <-workers
		go func(fkStmt string, workerId int) {
			defer func() {
				// Locking the progress reporting otherwise progress results displayed could be in random order.
				progressMutex.Lock()
				progress++
				p.MaybeReport(progress)
				progressMutex.Unlock()
				workers <- workerId
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
		}(fkStmt, workerId)
	}
	// Wait for all the goroutines to finish.
	for i := 1; i <= MaxWorkers; i++ {
		<-workers
	}
	p.Done()
	return nil
}

// GetProject returns the cloud project we should use for accessing Spanner.
// Use environment variable GCLOUD_PROJECT if it is set.
// Otherwise, use the default project returned from gcloud.
func GetProject() (string, error) {
	project := os.Getenv("GCLOUD_PROJECT")
	if project != "" {
		return project, nil
	}
	cmd := exec.Command("gcloud", "config", "list", "--format", "value(core.project)")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("call to gcloud to get project failed: %w", err)
	}
	project = strings.TrimSpace(string(out))
	return project, nil
}

// GetInstance returns the Spanner instance we should use for creating DBs.
// If the user specified instance (via flag 'instance') then use that.
// Otherwise try to deduce the instance using gcloud.
func GetInstance(ctx context.Context, project string, out *os.File) (string, error) {
	l, err := getInstances(ctx, project)
	if err != nil {
		return "", err
	}
	if len(l) == 0 {
		fmt.Fprintf(out, "Could not find any Spanner instances for project %s\n", project)
		return "", fmt.Errorf("no Spanner instances for %s", project)
	}

	// Note: we could ask for user input to select/confirm which Spanner
	// instance to use, but that interacts poorly with piping pg_dump/mysqldump data
	// to the tool via stdin.
	if len(l) == 1 {
		fmt.Fprintf(out, "Using only available Spanner instance: %s\n", l[0])
		return l[0], nil
	}
	fmt.Fprintf(out, "Available Spanner instances:\n")
	for i, x := range l {
		fmt.Fprintf(out, " %d) %s\n", i+1, x)
	}
	fmt.Fprintf(out, "Please pick one of the available instances and set the flag '--instance'\n\n")
	return "", fmt.Errorf("auto-selection of instance failed: project %s has more than one Spanner instance. "+
		"Please use the flag '--instance' to select an instance", project)
}

func getInstances(ctx context.Context, project string) ([]string, error) {
	instanceClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return nil, AnalyzeError(err, fmt.Sprintf("projects/%s", project))
	}
	it := instanceClient.ListInstances(ctx, &instancepb.ListInstancesRequest{Parent: fmt.Sprintf("projects/%s", project)})
	var l []string
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, AnalyzeError(err, fmt.Sprintf("projects/%s", project))
		}
		l = append(l, strings.TrimPrefix(resp.Name, fmt.Sprintf("projects/%s/instances/", project)))
	}
	return l, nil
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
	spDDL := conv.SpSchema.GetDDL(ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true})
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
	spDDL = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: true})
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
func WriteBadData(bw *spanner.BatchWriter, conv *internal.Conv, banner, name string, out *os.File) {
	badConversions := conv.BadRows()
	badWrites := sum(bw.DroppedRowsByTable())
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

// GetDatabaseName generates database name with driver_date prefix.
func GetDatabaseName(driver string, now time.Time) (string, error) {
	return generateName(fmt.Sprintf("%s_%s", driver, now.Format("2006-01-02")))
}

func getPassword() string {
	fmt.Print("Enter Password: ")
	bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Println("\nCoudln't read password")
		return ""
	}
	fmt.Printf("\n")
	return strings.TrimSpace(string(bytePassword))
}

// AnalyzeError inspects an error returned from Cloud Spanner and adds information
// about potential root causes e.g. authentication issues.
func AnalyzeError(err error, URI string) error {
	project, instance, _ := parseURI(URI)
	e := strings.ToLower(err.Error())
	if containsAny(e, []string{"unauthenticated", "cannot fetch token", "default credentials"}) {
		return fmt.Errorf("%w.\n"+`
Possible cause: credentials are mis-configured. Do you need to run

  gcloud auth application-default login

or configure environment variable GOOGLE_APPLICATION_CREDENTIALS.
See https://cloud.google.com/docs/authentication/getting-started.
`, err)
	}
	if containsAny(e, []string{"instance not found"}) && instance != "" {
		return fmt.Errorf("%w.\n"+`
Possible cause: Spanner instance specified via instance option does not exist.
Please check that '%s' is correct and that it is a valid Spanner
instance for project %s.
`, err, instance, project)
	}
	return err
}

// PrintPermissionsWarning prints permission warning.
func PrintPermissionsWarning(driver string, out *os.File) {
	fmt.Fprintf(out,
		`
WARNING: Please check that permissions for this Spanner instance are
appropriate. Spanner manages access control at the database level, and the
database created by HarbourBridge will inherit default permissions from this
instance. All data written to Spanner will be visible to anyone who can
access the created database. Note that `+driver+` table-level and row-level
ACLs are dropped during conversion since they are not supported by Spanner.

`)
}

func printSeekError(driver string, err error, out *os.File) {
	fmt.Fprintf(out, "\nCan't get seekable input file: %v\n", err)
	fmt.Fprintf(out, "Likely cause: not enough space in %s.\n", os.TempDir())
	fmt.Fprintf(out, "Try writing "+driver+" output to a file first i.e.\n")
	fmt.Fprintf(out, " "+driver+" > tmpfile\n")
	fmt.Fprintf(out, "  harbourbridge < tmpfile\n")
}

func containsAny(s string, l []string) bool {
	for _, a := range l {
		if strings.Contains(s, a) {
			return true
		}
	}
	return false
}

func generateName(prefix string) (string, error) {
	b := make([]byte, 4)
	_, err := rand.Read(b)
	if err != nil {
		return "", fmt.Errorf("error generating name: %w", err)

	}
	return fmt.Sprintf("%s_%x-%x", prefix, b[0:2], b[2:4]), nil
}

// NewSpannerClient returns a new Spanner client.
// It respects SPANNER_API_ENDPOINT.
func NewSpannerClient(ctx context.Context, db string) (*sp.Client, error) {
	if endpoint := os.Getenv("SPANNER_API_ENDPOINT"); endpoint != "" {
		return sp.NewClient(ctx, db, option.WithEndpoint(endpoint))
	}
	return sp.NewClient(ctx, db)
}

// GetClient returns a new Spanner client.  It uses the background context.
func GetClient(ctx context.Context, db string) (*sp.Client, error) {
	return NewSpannerClient(ctx, db)
}

// NewDatabaseAdminClient returns a new db-admin client.
// It respects SPANNER_API_ENDPOINT.
func NewDatabaseAdminClient(ctx context.Context) (*database.DatabaseAdminClient, error) {
	if endpoint := os.Getenv("SPANNER_API_ENDPOINT"); endpoint != "" {
		return database.NewDatabaseAdminClient(ctx, option.WithEndpoint(endpoint))
	}
	return database.NewDatabaseAdminClient(ctx)
}

// NewInstanceAdminClient returns a new instance-admin client.
// It respects SPANNER_API_ENDPOINT.
func NewInstanceAdminClient(ctx context.Context) (*instance.InstanceAdminClient, error) {
	if endpoint := os.Getenv("SPANNER_API_ENDPOINT"); endpoint != "" {
		return instance.NewInstanceAdminClient(ctx, option.WithEndpoint(endpoint))
	}
	return instance.NewInstanceAdminClient(ctx)
}

func getSize(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("can't stat file: %w", err)
	}
	return info.Size(), nil
}

// SetupLogFile configures the file used for logs.
// By default we just drop logs on the floor. To enable them (e.g. to debug
// Cloud Spanner client library issues), set logfile to a non-empty filename.
// Note: this tool itself doesn't generate logs, but some of the libraries it
// uses do. If we don't set the log file, we see a number of unhelpful and
// unactionable logs spamming stdout, which is annoying and confusing.
func SetupLogFile() (*os.File, error) {
	// To enable debug logs, set logfile to a non-empty filename.
	logfile := ""
	if logfile == "" {
		log.SetOutput(ioutil.Discard)
		return nil, nil
	}
	f, err := os.Create(logfile)
	if err != nil {
		return nil, err
	}
	log.SetOutput(f)
	return f, nil
}

// Close closes file.
func Close(f *os.File) {
	if f != nil {
		f.Close()
	}
}

func sum(m map[string]int64) int64 {
	n := int64(0)
	for _, c := range m {
		n += c
	}
	return n
}

// GetBanner prints banner message after command line process is finished.
func GetBanner(now time.Time, db string) string {
	return fmt.Sprintf("Generated at %s for db %s\n\n", now.Format("2006-01-02 15:04:05"), db)
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

// ProcessInfoSchema invokes process infoschema function from a sql package based on driver selected.
func ProcessInfoSchema(driver string, conv *internal.Conv, db *sql.DB) error {
	switch driver {
	case constants.MYSQL:
		return common.ProcessInfoSchema(conv, db, mysql.InfoSchemaImpl{os.Getenv("MYSQLDATABASE")})
	case constants.POSTGRES:
		return common.ProcessInfoSchema(conv, db, postgres.InfoSchemaImpl{})
	default:
		return fmt.Errorf("schema conversion for driver %s not supported", driver)
	}
}

// SetRowStats invokes SetRowStats function from a sql package based on driver selected.
func SetRowStats(driver string, conv *internal.Conv, db *sql.DB) error {
	switch driver {
	case constants.MYSQL:
		common.SetRowStats(conv, db, mysql.InfoSchemaImpl{os.Getenv("MYSQLDATABASE")})
	case constants.POSTGRES:
		common.SetRowStats(conv, db, postgres.InfoSchemaImpl{})
	default:
		return fmt.Errorf("Could not set rows stats for '%s' driver", driver)
	}
	return nil
}

// ProcessSQLData invokes ProcessSQLData function from a sql package based on driver selected.
func ProcessSQLData(driver string, conv *internal.Conv, db *sql.DB) error {
	switch driver {
	//TODO - move this logic into a factory within the sources dir
	case constants.MYSQL:
		common.ProcessSQLData(conv, db, mysql.InfoSchemaImpl{os.Getenv("MYSQLDATABASE")})
	case constants.POSTGRES:
		common.ProcessSQLData(conv, db, postgres.InfoSchemaImpl{})
	default:
		return fmt.Errorf("Data conversion for driver %s is not supported", driver)
	}
	return nil
}
