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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/testing/common"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"

	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func init() {
	logger.Log = zap.NewNop()
}

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
	projectID = os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}

	if projectID == "" {
		log.Println("Integration tests skipped: SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if instanceID == "" {
		log.Println("Integration tests skipped: SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID is missing")
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

func TestIntegration_MYSQL_SchemaAndDataSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "mysql-dc-schema-and-data"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName)

	host, user, srcDb, password := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLDATABASE"), os.Getenv("MYSQLPWD")
	envVars := common.ClearEnvVariables([]string{"MYSQLHOST", "MYSQLUSER", "MYSQLDATABASE", "MYSQLPWD"})
	args := fmt.Sprintf("schema-and-data -source=%s -prefix=%s -source-profile='host=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s'", constants.MYSQL, filePrefix, host, user, srcDb, password, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	common.RestoreEnvVariables(envVars)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI, true)
}

func runSchemaSubcommand(t *testing.T, dbName, filePrefix, sessionFile, dumpFilePath string) {
	args := fmt.Sprintf("schema -prefix %s -source=mysql -target-profile='instance=%s,dbName=%s' < %s", filePrefix, instanceID, dbName, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runDataSubcommand(t *testing.T, dbName, dbURI, filePrefix, sessionFile, dumpFilePath string) {
	args := fmt.Sprintf("data -source=mysql -prefix %s -session %s -target-profile='instance=%s,dbName=%s' < %s", filePrefix, sessionFile, instanceID, dbName, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runSchemaAndDataSubcommand(t *testing.T, dbName, dbURI, filePrefix, dumpFilePath string) {
	args := fmt.Sprintf("schema-and-data -source=mysql -prefix %s -target-profile='instance=%s,dbName=%s' < %s", filePrefix, instanceID, dbName, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_MySQLDUMP_SchemaSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-schema-subcommand"

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)

	dumpFilePath := "../../test_data/mysqldump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName)
	sessionFile := fmt.Sprintf("%s.session.json", filePrefix)
	runSchemaSubcommand(t, dbName, filePrefix, sessionFile, dumpFilePath)
	if _, err := os.Stat(fmt.Sprintf("%s.report.txt", filePrefix)); os.IsNotExist(err) {
		t.Fatalf("report file not generated during schema-only test")
	}
	if _, err := os.Stat(fmt.Sprintf("%s.schema.ddl.txt", filePrefix)); os.IsNotExist(err) {
		t.Fatalf("legal ddl file not generated during schema-only test")
	}
	if _, err := os.Stat(fmt.Sprintf("%s.schema.txt", filePrefix)); os.IsNotExist(err) {
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
	filePrefix := filepath.Join(tmpdir, dbName)
	sessionFile := fmt.Sprintf("%s.session.json", filePrefix)
	runSchemaSubcommand(t, dbName, filePrefix, sessionFile, dumpFilePath)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)
	runDataSubcommand(t, dbName, dbURI, filePrefix, sessionFile, dumpFilePath)
	checkResults(t, dbURI, false)
}

func TestIntegration_MySQLDUMP_SchemaAndDataSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-schema-and-data"
	dumpFilePath := "../../test_data/mysqldump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName)

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runSchemaAndDataSubcommand(t, dbName, dbURI, filePrefix, dumpFilePath)
	defer dropDatabase(t, dbURI)
	checkResults(t, dbURI, false)
}

func TestIntegration_MYSQL_ForeignKeyActionMigration(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "mysql-foreignkey-actions"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName)

	// host, user, srcDb, password := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), "test_foreign_key_action_data", os.Getenv("MYSQLPWD")
	envVars := common.ClearEnvVariables([]string{"MYSQLHOST", "MYSQLUSER", "MYSQLPWD"})
	// args := fmt.Sprintf("schema-and-data -source=%s -prefix=%s -source-profile='host=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s'", constants.MYSQL, filePrefix, host, user, srcDb, password, instanceID, dbName)
	args := fmt.Sprintf("schema-and-data -source=%s -prefix=%s -source-profile='host=localhost,user=root,dbName=test_foreign_key_action_data,password=root' -target-profile='instance=test-instance,dbName=mysql-foreignkey-actions'", constants.MYSQL, filePrefix)
	err := common.RunCommand(args, "emulator-test-project")
	common.RestoreEnvVariables(envVars)
	if err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(t, dbURI)

	checkForeignKeyActions(ctx, t, dbURI)
}

func TestIntegration_MySQLDUMP_ForeignKeyActionMigration(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-schema-and-data"
	dumpFilePath := "../../test_data/mysql_foreignkeyaction_dump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName)

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runSchemaAndDataSubcommand(t, dbName, dbURI, filePrefix, dumpFilePath)
	defer dropDatabase(t, dbURI)
	checkForeignKeyActions(ctx, t, dbURI)
}

func checkResults(t *testing.T, dbURI string, skipJson bool) {
	// Make a query to check results.
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkBigInt(ctx, t, client)
	if !skipJson {
		checkJson(ctx, t, client, dbURI)
	}
}

func checkBigInt(ctx context.Context, t *testing.T, client *spanner.Client) {
	var quantity int64
	iter := client.Single().Read(ctx, "cart", spanner.Key{"901e-a6cfc2b502dc", "abc-123"}, []string{"quantity"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
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

func checkJson(ctx context.Context, t *testing.T, client *spanner.Client, dbURI string) {
	resp, err := databaseAdmin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: dbURI})
	if err != nil {
		t.Fatalf("Could not read DDL from database %s: %v", dbURI, err)
	}
	for _, stmt := range resp.Statements {
		if strings.Contains(stmt, "CREATE TABLE customers") {
			assert.True(t, strings.Contains(stmt, "customer_profile JSON"))
		}
	}
	got_profile := spanner.NullJSON{}
	iter := client.Single().Read(ctx, "customers", spanner.Key{"tel-595"}, []string{"customer_profile"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&got_profile); err != nil {
			t.Fatal(err)
		}
	}
	want_profile := spanner.NullJSON{Valid: true}
	json.Unmarshal([]byte("{\"first_name\": \"Ernie\", \"status\": \"Looking for treats\", \"location\" : \"Brooklyn\"}"), &want_profile.Value)
	assert.Equal(t, got_profile, want_profile)
}

func checkForeignKeyActions(ctx context.Context, t *testing.T, dbURI string) {
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	//comment this out
	stmt1 := spanner.Statement{SQL: `SELECT * FROM products WHERE product_id = "zxi-631"`}
	iter1 := client.Single().Query(ctx, stmt1)
	defer iter1.Stop()
	_, err = iter1.Next()
	assert.Equal(t, nil, err, "Expected rows in 'products'")
	//--

	mutation := spanner.Delete("products", spanner.Key{"zxi-631"})

	_, err = client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		t.Fatalf("Failed to delete row: %v", err)
	}

	stmt := spanner.Statement{SQL: `SELECT * FROM cart WHERE product_id = "zxi-631"`}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err = iter.Next()

	assert.Equal(t, iterator.Done, err, "Expected no rows in 'cart' with deleted product_id") //testing ON DELETE CASCADE
}

// // Generated DDL always contains Foreign Key Definitions as 'ALTER TABLE' statements
// // and will always define the ON DELETE action. Spanner does not support
// // ON UPDATE and this should not be a part of the DDL.
// func checkForeignKeyActions(ctx context.Context, t *testing.T, client *spanner.Client, dbURI string) {

// 	resp, err := databaseAdmin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: dbURI})
// 	if err != nil {
// 		t.Fatalf("Could not read DDL from database %s: %v", dbURI, err)
// 	}
// 	for _, stmt := range resp.Statements {
// 		if strings.Contains(stmt, "ALTER TABLE ") && strings.Contains(stmt, "FOREIGN KEY") {
// 			assert.True(t, strings.Contains(stmt, "ON DELETE"), "Missing ON DELETE action")
// 			assert.False(t, strings.Contains(stmt, "ON UPDATE"), "Unexpected ON UPDATE action")

// 			if strings.Contains(stmt, "cart") {
// 				assert.True(t, strings.Contains(stmt, "ON DELETE CASCADE"))
// 			}
// 		}
// 	}
// }

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
