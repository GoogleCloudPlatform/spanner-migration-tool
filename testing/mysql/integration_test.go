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

package mysql_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/testing/common"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"

	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID  string
	instanceID string

	ctx           context.Context
	databaseAdmin *database.DatabaseAdminClient
)

func TestMain(m *testing.M) {
	cleanup := initIntegrationTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func initIntegrationTests() (cleanup func()) {
	projectID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}

	if projectID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if instanceID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID is missing")
		return noop
	}

	var err error
	databaseAdmin, err = database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatalf("cannot create databaseAdmin client: %v", err)
	}

	return func() {
		databaseAdmin.Close()
	}
}

func dropDatabase(t *testing.T, dbURI string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// Drop the testing database.
	if err := databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbURI}); err != nil {
		t.Fatalf("failed to drop testing database %v: %v", dbURI, err)
	}
}

func prepareIntegrationTest(t *testing.T) string {
	tmpdir, err := ioutil.TempDir(".", "int-test-")
	if err != nil {
		log.Fatal(err)
	}
	return tmpdir
}

func TestIntegration_MYSQLDUMP_Command(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(constants.MYSQLDUMP, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	dataFilepath := "../../test_data/mysqldump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")

	args := fmt.Sprintf("-driver %s -prefix %s -instance %s -dbname %s < %s", constants.MYSQLDUMP, filePrefix, instanceID, dbName, dataFilepath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_MYSQL_SchemaAndDataSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "mysql-dc-schema-and-data"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName+".")

	host, user, db_name, password := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLDATABASE"), os.Getenv("MYSQLPWD")
	envVars := common.ClearEnvVariables([]string{"MYSQLHOST", "MYSQLUSER", "MYSQLDATABASE", "MYSQLPWD"})
	args := fmt.Sprintf("schema-and-data -source=%s -prefix=%s -source-profile='host=%s,user=%s,db_name=%s,password=%s' -target-profile='instance=%s,dbname=%s'", constants.MYSQL, filePrefix, host, user, db_name, password, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	common.RestoreEnvVariables(envVars)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_MYSQL_Command(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(constants.MYSQL, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName+".")

	args := fmt.Sprintf("-driver %s -prefix %s -instance %s -dbname %s", constants.MYSQL, filePrefix, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_MySQLInterleaveTable_DataOnlyWithSessionFile(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test_interleave_table_data"
	sessionFile := "../../test_data/session_test.json"

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runDataOnlySubcommandForSessionFile(t, dbName, dbURI, sessionFile)
	defer dropDatabase(t, dbURI)
	checkResults(t, dbURI)
}

func runSchemaOnly(t *testing.T, dbName, filePrefix, sessionFile, dumpFilePath string) {
	args := fmt.Sprintf("-driver mysqldump -schema-only -dbname %s -prefix %s < %s", dbName, filePrefix, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runDataOnly(t *testing.T, dbName, dbURI, filePrefix, sessionFile, dumpFilePath string) {
	args := fmt.Sprintf("-driver mysqldump -data-only -instance %s -dbname %s -prefix %s -session %s < %s", instanceID, dbName, filePrefix, sessionFile, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_MySQLDUMP_SchemaOnly(t *testing.T) {
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-schema-only-mode"
	dumpFilePath := "../../test_data/mysqldump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")
	sessionFile := fmt.Sprintf("%ssession.json", filePrefix)
	runSchemaOnly(t, dbName, filePrefix, sessionFile, dumpFilePath)
	if _, err := os.Stat(fmt.Sprintf("%sreport.txt", filePrefix)); os.IsNotExist(err) {
		t.Fatalf("report file not generated during schema-only test")
	}
	if _, err := os.Stat(fmt.Sprintf("%sschema.ddl.txt", filePrefix)); os.IsNotExist(err) {
		t.Fatalf("legal ddl file not generated during schema-only test")
	}
	if _, err := os.Stat(fmt.Sprintf("%sschema.txt", filePrefix)); os.IsNotExist(err) {
		t.Fatalf("readable schema file not generated during schema-only test")
	}
	if _, err := os.Stat(sessionFile); os.IsNotExist(err) {
		t.Fatalf("session file not generated during schema-only test")
	}
}

func TestIntegration_MySQLDUMP_DataOnly(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-data-only-mode"
	dumpFilePath := "../../test_data/mysqldump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")
	sessionFile := fmt.Sprintf("%ssession.json", filePrefix)
	runSchemaOnly(t, dbName, filePrefix, sessionFile, dumpFilePath)

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runDataOnly(t, dbName, dbURI, filePrefix, sessionFile, dumpFilePath)
	defer dropDatabase(t, dbURI)
	checkResults(t, dbURI)
}

func runSchemaSubcommand(t *testing.T, dbName, filePrefix, sessionFile, dumpFilePath string) {
	args := fmt.Sprintf("schema -prefix %s -source=mysql -target-profile='dbname=%s' < %s", filePrefix, dbName, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runDataSubcommand(t *testing.T, dbName, dbURI, filePrefix, sessionFile, dumpFilePath string) {
	args := fmt.Sprintf("data -source=mysql -prefix %s -session %s -target-profile='instance=%s,dbname=%s' < %s", filePrefix, sessionFile, instanceID, dbName, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runSchemaAndDataSubcommand(t *testing.T, dbName, dbURI, filePrefix, dumpFilePath string) {
	args := fmt.Sprintf("schema-and-data -source=mysql -prefix %s -target-profile='instance=%s,dbname=%s' < %s", filePrefix, instanceID, dbName, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runDataOnlySubcommandForSessionFile(t *testing.T, dbName, dbURI, sessionFile string) {
	host, user, password := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLPWD")
	args := fmt.Sprintf("data -source=mysql -session %s -source-profile='host=%s,user=%s,db_name=%s,password=%s' -target-profile='instance=%s,dbname=%s' ", sessionFile, host, user, dbName, password, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_MySQLDUMP_SchemaSubcommand(t *testing.T) {
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-schema-subcommand"
	dumpFilePath := "../../test_data/mysqldump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")
	sessionFile := fmt.Sprintf("%ssession.json", filePrefix)
	runSchemaSubcommand(t, dbName, filePrefix, sessionFile, dumpFilePath)
	if _, err := os.Stat(fmt.Sprintf("%sreport.txt", filePrefix)); os.IsNotExist(err) {
		t.Fatalf("report file not generated during schema-only test")
	}
	if _, err := os.Stat(fmt.Sprintf("%sschema.ddl.txt", filePrefix)); os.IsNotExist(err) {
		t.Fatalf("legal ddl file not generated during schema-only test")
	}
	if _, err := os.Stat(fmt.Sprintf("%sschema.txt", filePrefix)); os.IsNotExist(err) {
		t.Fatalf("readable schema file not generated during schema-only test")
	}
	if _, err := os.Stat(sessionFile); os.IsNotExist(err) {
		t.Fatalf("session file not generated during schema-only test")
	}
}

func TestIntegration_MySQLDUMP_DataSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-data-subcommand"
	dumpFilePath := "../../test_data/mysqldump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")
	sessionFile := fmt.Sprintf("%ssession.json", filePrefix)
	runSchemaSubcommand(t, dbName, filePrefix, sessionFile, dumpFilePath)

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runDataSubcommand(t, dbName, dbURI, filePrefix, sessionFile, dumpFilePath)
	defer dropDatabase(t, dbURI)
	checkResults(t, dbURI)
}

func TestIntegration_MySQLDUMP_SchemaAndDataSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-schema-and-data"
	dumpFilePath := "../../test_data/mysqldump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runSchemaAndDataSubcommand(t, dbName, dbURI, filePrefix, dumpFilePath)
	defer dropDatabase(t, dbURI)
	checkResults(t, dbURI)
}

func checkResults(t *testing.T, dbURI string) {
	// Make a query to check results.
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkBigInt(ctx, t, client)
}

func checkBigInt(ctx context.Context, t *testing.T, client *spanner.Client) {
	var quantity int64
	iter := client.Single().Read(ctx, "cart", spanner.Key{"901e-a6cfc2b502dc", "abc-123"}, []string{"quantity"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			print("\niterator done\n")
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&quantity); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := quantity, int64(1); got != want {
		t.Fatalf("quantities are not correct: got %v, want %v", got, want)
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
