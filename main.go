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

// Package main implements HarbourBridge, a stand-alone tool for Cloud Spanner
// evaluation, using data from an existing PostgreSQL database. See README.md
// for details.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	_ "github.com/lib/pq"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

var (
	badDataFile      = "dropped.txt"
	schemaFile       = "schema.txt"
	reportFile       = "report.txt"
	dbNameOverride   string
	instanceOverride string
	filePrefix       = ""
	driverName       = ""
	verbose          bool
	fromPgDump       bool
)

func init() {
	flag.StringVar(&dbNameOverride, "dbname", "", "dbname: name to use for Spanner DB")
	flag.StringVar(&instanceOverride, "instance", "", "instance: Spanner instance to use")
	flag.StringVar(&filePrefix, "prefix", "", "prefix: file prefix for generated files")
	flag.StringVar(&driverName, "driver", "", "driver name: experimental flag for accessing source DB via database/sql driver (only accepted value is \"postgres\")")
	flag.BoolVar(&verbose, "v", false, "verbose: print additional output")
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, `Note: input is always read from stdin.
Sample usage:
  pg_dump mydb | %s
  %s < my_pg_dump_file
`, os.Args[0], os.Args[0])
}

func main() {
	flag.Usage = usage
	flag.Parse()
	internal.VerboseInit(verbose)
	lf, err := setupLogFile()
	if err != nil {
		fmt.Printf("\nCan't set up log file: %v\n", err)
		panic(fmt.Errorf("can't set up log file"))
	}
	defer close(lf)

	project, err := getProject()
	if err != nil {
		fmt.Printf("\nCan't get project: %v\n", err)
		panic(fmt.Errorf("can't get project"))
	}
	fmt.Printf("Using project: %s\n", project)

	instance := instanceOverride
	if instance == "" {
		instance, err = getInstance(project)
		if err != nil {
			fmt.Printf("\nCan't get instance: %v\n", err)
			panic(fmt.Errorf("can't get instance"))
		}
	}
	fmt.Printf("Using Spanner instance: %s\n", instance)
	printPermissionsWarning()

	now := time.Now()
	dbName := dbNameOverride
	if dbName == "" {
		dbName, err = getDatabaseName(now)
		if err != nil {
			fmt.Printf("\nCan't get database name: %v\n", err)
			panic(fmt.Errorf("can't get database name"))
		}
	}

	// If filePrefix not explicitly set, use dbName.
	if filePrefix == "" {
		filePrefix = dbName + "."
	}

	// If driverName specified, access source DB via database/sql
	// driver. Otherwise read pgdump data from stdin.
	fromPgDump = driverName == ""
	if fromPgDump {
		err = pgDumpToSpanner(project, instance, dbName, nil, filePrefix, now)
	} else {
		err = sourceToSpanner(driverName, project, instance, dbName, filePrefix, now)
	}

	if err != nil {
		panic(err)
	}
}

// sourceToSpanner migrates a source DB (accessed via a database/sql
// driver) to Spanner.  The source DB must support the
// information_schema standard (for information about tables, views,
// columns etc). Data is accessed via SELECT statements.
// TODO: add data conversion.
func sourceToSpanner(driver, projectID, instanceID, dbName string, outputFilePrefix string, now time.Time) error {
	fmt.Println(`###################################################################
Accessing a source DB via an database/sql driver is an experimental
feature that is not fully implemented. Currently we only do schema
conversion and we only support PostgreSQL.
###################################################################`)
	if driver != "postgres" {
		return fmt.Errorf("Driver %s not supported", driver)
	}
	server := os.Getenv("PGHOST")
	port := os.Getenv("PGPORT")
	user := os.Getenv("PGUSER")
	dbname := os.Getenv("PGDATABASE")
	if server == "" || port == "" || user == "" || dbname == "" {
		fmt.Printf("Please specify host, port, user and database using PGHOST, PGPORT, PGUSER and PGDATABASE environment variables\n")
		return fmt.Errorf("Could not connect to source database")
	}
	password := getPassword()
	sourceDB, err := sql.Open(driver, fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", server, port, user, password, dbname))
	if err != nil {
		return err
	}
	conv := internal.MakeConv()
	err = internal.ProcessInfoSchema(conv, sourceDB)
	if err != nil {
		return err
	}
	writeSchemaFile(conv, now, outputFilePrefix+schemaFile)
	spannerDB, err := createDatabase(projectID, instanceID, dbName, conv)
	if err != nil {
		fmt.Printf("\nCan't create database: %v\n", err)
		return fmt.Errorf("can't create database")
	}
	badWrites := make(map[string]int64) // Empty bad writes since no data conversion yet.
	banner := getBanner(now, spannerDB)
	report(badWrites, 0, banner, conv, outputFilePrefix+reportFile)
	return nil
}

type ioStreams struct {
	in, out *os.File
}

var ioHelper = &ioStreams{os.Stdin, os.Stdout}

func pgDumpToSpanner(projectID, instanceID, dbName string, helper *ioStreams, outputFilePrefix string, now time.Time) error {
	if helper != nil {
		ioHelper = helper
	}

	f, n, err := getSeekable(ioHelper.in)
	if err != nil {
		printSeekError(err)
		return fmt.Errorf("can't get seekable input file")
	}
	defer f.Close()
	conv := internal.MakeConv()

	err = firstPass(f, n, conv)
	if err != nil {
		fmt.Fprintf(helper.out, "Failed to parse the data file: %v", err)
		return fmt.Errorf("failed to parse the data file")
	}
	writeSchemaFile(conv, now, outputFilePrefix+schemaFile)
	db, err := createDatabase(projectID, instanceID, dbName, conv)
	if err != nil {
		fmt.Printf("\nCan't create database: %v\n", err)
		return fmt.Errorf("can't create database")
	}
	client, err := getClient(db)
	if err != nil {
		fmt.Printf("\nCan't create client for db %s: %v\n", db, err)
		return fmt.Errorf("can't create Spanner client")
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Printf("\nCan't seek to start of file (preparation for second pass): %v\n", err)
		return fmt.Errorf("can't seek to start of file")
	}
	rows := conv.Rows()
	bw := secondPass(f, client, conv, rows)
	banner := getBanner(now, db)
	// TODO(hengfeng): When we refactor `process` into a separate module, and
	// the parameters will capture everything we need from main.
	report(bw.DroppedRowsByTable(), n, banner, conv, outputFilePrefix+reportFile)
	writeBadData(bw, conv, banner, outputFilePrefix+badDataFile)
	return nil
}

func report(badWrites map[string]int64, bytesRead int64, banner string, conv *internal.Conv, reportFileName string) {
	f, err := os.Create(reportFileName)
	if err != nil {
		fmt.Fprintf(ioHelper.out, "Can't write out report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(ioHelper.out, "Writing report to stdout\n")
		f = ioHelper.out
	} else {
		defer f.Close()
	}
	w := bufio.NewWriter(f)
	w.WriteString(banner)
	summary := internal.GenerateReport(fromPgDump, conv, w, badWrites)
	w.Flush()
	if fromPgDump {
		fmt.Fprintf(ioHelper.out, "Processed %d bytes of pg_dump data (%d statements, %d rows of data, %d errors, %d unexpected conditions).\n",
			bytesRead, conv.Statements(), conv.Rows(), conv.StatementErrors(), conv.Unexpecteds())
	} else {
		fmt.Fprintf(ioHelper.out, "Processed source database via %s driver (%d rows of data, %d unexpected conditions).\n",
			driverName, conv.Rows(), conv.Unexpecteds())
	}
	// We've already written summary to f (as part of GenerateReport).
	// In the case where f is stdout, don't write a duplicate copy.
	if f != ioHelper.out {
		fmt.Fprint(ioHelper.out, summary)
		fmt.Fprintf(ioHelper.out, "See file '%s' for details of the schema and data conversions.\n", reportFileName)
	}
}

func firstPass(f *os.File, fileSize int64, conv *internal.Conv) error {
	p := internal.NewProgress(fileSize, "Generating schema", internal.Verbose())
	r := internal.NewReader(bufio.NewReader(f), p)
	conv.SetSchemaMode() // Build schema and ignore data in pg_dump.
	conv.SetDataSink(nil)
	err := internal.ProcessPgDump(conv, r)
	if err != nil {
		return err
	}
	p.Done()
	return nil
}

func secondPass(f *os.File, client *sp.Client, conv *internal.Conv, totalRows int64) *spanner.BatchWriter {
	p := internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose())
	r := internal.NewReader(bufio.NewReader(f), nil)
	rows := int64(0)
	config := spanner.BatchWriterConfig{
		BytesLimit: 100 * 1000 * 1000,
		WriteLimit: 40,
		RetryLimit: 1000,
		Verbose:    internal.Verbose(),
		Write: func(m []*sp.Mutation) error {
			_, err := client.Apply(context.Background(), m)
			if err != nil {
				return err
			}
			atomic.AddInt64(&rows, int64(len(m)))
			p.MaybeReport(atomic.LoadInt64(&rows))
			return nil
		},
	}
	writer := spanner.NewBatchWriter(config)
	conv.SetDataMode() // Process data in pg_dump; schema is unchanged.
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			writer.AddRow(table, cols, vals)
		})
	internal.ProcessPgDump(conv, r)
	writer.Flush()
	p.Done()
	return writer
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
	// pg_dump output is large (tens of GBs) and os.TempDir points to a directory
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

// createDatabase returns a newly create Spanner DB.
// It automatically determines an appropriate project, selects a
// Spanner instance to use, generates a new Spanner DB name,
// and call into the Spanner admin interface to create the new DB.
func createDatabase(project, instance, dbName string, conv *internal.Conv) (string, error) {
	fmt.Fprintf(ioHelper.out, "Creating new database %s in instance %s with default permissions ... ", dbName, instance)
	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", fmt.Errorf("can't create admin client: %w", analyzeError(err, project, instance))
	}
	defer adminClient.Close()
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	schema := conv.GetDDL(ddl.Config{Comments: false, ProtectIds: true})
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: "CREATE DATABASE `" + dbName + "`",
		ExtraStatements: schema,
	})
	if err != nil {
		return "", fmt.Errorf("can't build CreateDatabaseRequest: %w", analyzeError(err, project, instance))
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("createDatabase call failed: %w", analyzeError(err, project, instance))
	}
	fmt.Fprintf(ioHelper.out, "done.\n")
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName), nil
}

// getProject returns the cloud project we should use for accessing Spanner.
// Use environment variable GCLOUD_PROJECT if it is set.
// Otherwise, use the default project returned from gcloud.
func getProject() (string, error) {
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

// getInstance returns the Spanner instance we should use for creating DBs.
// If the user specified instance (via flag 'instance') then use that.
// Otherwise try to deduce the instance using gcloud.
func getInstance(project string) (string, error) {
	l, err := getInstances(project)
	if err != nil {
		return "", err
	}
	if len(l) == 0 {
		fmt.Fprintf(ioHelper.out, "Could not find any Spanner instances for project %s\n", project)
		return "", fmt.Errorf("no Spanner instances for %s", project)
	}
	// Note: we could ask for user input to select/confirm which Spanner
	// instance to use, but that interacts poorly with piping pg_dump data
	// to the tool via stdin.
	if len(l) == 1 {
		fmt.Fprintf(ioHelper.out, "Using only available Spanner instance: %s\n", l[0])
		return l[0], nil
	}
	fmt.Fprintf(ioHelper.out, "Available Spanner instances:\n")
	for i, x := range l {
		fmt.Fprintf(ioHelper.out, " %d) %s\n", i+1, x)
	}
	fmt.Fprintf(ioHelper.out, "Please pick one of the available instances and set the flag '--instance'\n\n")
	return "", fmt.Errorf("auto-selection of instance failed: project %s has more than one Spanner instance. "+
		"Please use the flag '--instance' to select an instance", project)
}

func getInstances(project string) ([]string, error) {
	ctx := context.Background()
	instanceClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return nil, analyzeError(err, project, "")
	}
	it := instanceClient.ListInstances(ctx, &instancepb.ListInstancesRequest{Parent: fmt.Sprintf("projects/%s", project)})
	var l []string
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, analyzeError(err, project, "")
		}
		l = append(l, strings.TrimPrefix(resp.Name, fmt.Sprintf("projects/%s/instances/", project)))
	}
	return l, nil
}

func writeSchemaFile(conv *internal.Conv, now time.Time, name string) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(ioHelper.out, "Can't create schema file %s: %v\n", name, err)
		return
	}
	// The schema file we write out includes comments, and doesn't add backticks
	// around table and column names. This file is intended for explanatory
	// and documentation purposes, and is not strictly legal Cloud Spanner DDL
	// (Cloud Spanner doesn't currently support comments). Change 'Comments'
	// to false and 'ProtectIds' to true to write out a schema file that is
	// legal Cloud Spanner DDL.
	ddl := conv.GetDDL(ddl.Config{Comments: true, ProtectIds: false})
	if len(ddl) == 0 {
		ddl = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l := []string{
		fmt.Sprintf("-- Schema generated %s\n", now.Format("2006-01-02 15:04:05")),
		strings.Join(ddl, ";\n\n"),
		"\n",
	}
	if _, err := f.WriteString(strings.Join(l, "")); err != nil {
		fmt.Fprintf(ioHelper.out, "Can't write out schema file: %v\n", err)
		return
	}
	fmt.Fprintf(ioHelper.out, "Wrote schema to file '%s'.\n", name)
}

// writeBadData prints summary stats about bad rows and writes detailed info
// to file 'name'.
func writeBadData(bw *spanner.BatchWriter, conv *internal.Conv, banner, name string) {
	badConversions := conv.BadRows()
	badWrites := sum(bw.DroppedRowsByTable())
	if badConversions == 0 && badWrites == 0 {
		os.Remove(name) // Cleanup bad-data file from previous run.
		return
	}
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(ioHelper.out, "Can't write out bad data file: %v\n", err)
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
				fmt.Fprintf(ioHelper.out, "Can't write out bad data file: %v\n", err)
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
				fmt.Fprintf(ioHelper.out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	fmt.Fprintf(ioHelper.out, "See file '%s' for details of bad rows\n", name)
}

func getDatabaseName(now time.Time) (string, error) {
	return generateName(fmt.Sprintf("pg_dump_%s", now.Format("2006-01-02")))
}

func getPassword() string {
	password := os.Getenv("PGPASSWORD")
	if password != "" {
		return password
	}
	fmt.Print("Enter Password: ")
	bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Println("\nCoudln't read password")
		return ""
	}
	fmt.Printf("\n")
	return strings.TrimSpace(string(bytePassword))
}

// analyzeError inspects an error returned from Cloud Spanner and adds information
// about potential root causes e.g. authentication issues.
func analyzeError(err error, project, instance string) error {
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

func printPermissionsWarning() {
	fmt.Fprintf(ioHelper.out,
		`
WARNING: Please check that permissions for this Spanner instance are
appropriate. Spanner manages access control at the database level, and the
database created by HarbourBridge will inherit default permissions from this
instance. All data written to Spanner will be visible to anyone who can
access the created database. Note that PostgreSQL table-level and row-level
ACLs are dropped during conversion since they are not supported by Spanner.

`)
}

func printSeekError(err error) {
	fmt.Fprintf(ioHelper.out, "\nCan't get seekable input file: %v\n", err)
	fmt.Fprintf(ioHelper.out, "Likely cause: not enough space in %s.\n", os.TempDir())
	fmt.Fprintf(ioHelper.out, "Try writing pg_dump output to a file first i.e.\n")
	fmt.Fprintf(ioHelper.out, "  pg_dump > tmpfile\n")
	fmt.Fprintf(ioHelper.out, "  harbourbridge < tmpfile\n")
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

func getClient(db string) (*sp.Client, error) {
	ctx := context.Background()
	return sp.NewClient(ctx, db)
}

func getSize(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("can't stat file: %w", err)
	}
	return info.Size(), nil
}

// setupLogfile configures the file used for logs.
// By default we just drop logs on the floor. To enable them (e.g. to debug
// Cloud Spanner client library issues), set logfile to a non-empty filename.
// Note: this tool itself doesn't generate logs, but some of the libraries it
// uses do. If we don't set the log file, we see a number of unhelpful and
// unactionable logs spamming stdout, which is annoying and confusing.
func setupLogFile() (*os.File, error) {
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

func close(f *os.File) {
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

func getBanner(now time.Time, db string) string {
	return fmt.Sprintf("Generated at %s for db %s\n\n", now.Format("2006-01-02 15:04:05"), db)
}
