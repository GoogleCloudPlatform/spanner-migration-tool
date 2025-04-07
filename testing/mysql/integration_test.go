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
	args := fmt.Sprintf("schema-and-data -source=%s -prefix=%s -source-profile='host=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s,project=%s'", constants.MYSQL, filePrefix, host, user, srcDb, password, instanceID, dbName, projectID)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI, true)
}

func runSchemaSubcommand(t *testing.T, dbName, filePrefix, sessionFile, dumpFilePath string) {
	args := fmt.Sprintf("schema -prefix %s -source=mysql -target-profile='instance=%s,dbName=%s,project=%s' < %s", filePrefix, instanceID, dbName, projectID, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runDataSubcommand(t *testing.T, dbName, dbURI, filePrefix, sessionFile, dumpFilePath string) {
	args := fmt.Sprintf("data -source=mysql -prefix %s -session %s -target-profile='instance=%s,dbName=%s,project=%s' < %s", filePrefix, sessionFile, instanceID, dbName, projectID, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runImportSubcommand(t *testing.T, dbName, dumpFilePath string) {
	args := fmt.Sprintf("import -format=mysqldump -instance-id=%s -db-name=%s -project=%s -source-uri=%s", instanceID, dbName, projectID, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func runSchemaAndDataSubcommand(t *testing.T, dbName, dbURI, filePrefix, dumpFilePath string) {
	args := fmt.Sprintf("schema-and-data -source=mysql -prefix %s -target-profile='instance=%s,dbName=%s,project=%s' < %s", filePrefix, instanceID, dbName, projectID, dumpFilePath)
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

func TestIntegration_MySQLDUMP_ImportDataCommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-import-data"
	dumpFilePath := "../../test_data/mysqldump.test.out"

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runImportSubcommand(t, dbName, dumpFilePath)
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

	host, user, srcDb, password := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLDB_FKACTION"), os.Getenv("MYSQLPWD")
	args := fmt.Sprintf("schema-and-data -source=%s -prefix=%s -source-profile='host=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s,project=%s'", constants.MYSQL, filePrefix, host, user, srcDb, password, instanceID, dbName, projectID)
	err := common.RunCommand(args, projectID)
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

func TestIntegration_MySQLDUMP_IMPORT_ForeignKeyActionMigration(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-import-foreign-key-action"
	dumpFilePath := "../../test_data/mysql_foreignkeyaction_dump.test.out"

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runImportSubcommand(t, dbName, dumpFilePath)

	defer dropDatabase(t, dbURI)
	checkResults(t, dbURI, true)
	checkForeignKeyActions(ctx, t, dbURI)
}

func TestIntegration_MySQLDUMP_CheckConstraintMigration(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-check-constraint"
	dumpFilePath := "../../test_data/mysql_checkconstraint_dump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runSchemaAndDataSubcommand(t, dbName, dbURI, filePrefix, dumpFilePath)

	defer dropDatabase(t, dbURI)
	checkCheckConstraints(ctx, t, dbURI)
}

func TestIntegration_MySQLDUMP_IMPORT_CheckConstraintMigration(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "test-import-check-constraint"
	dumpFilePath := "../../test_data/mysql_checkconstraint_dump.test.out"

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	runImportSubcommand(t, dbName, dumpFilePath)

	defer dropDatabase(t, dbURI)
	checkCheckConstraints(ctx, t, dbURI)
}

func TestIntegration_MYSQL_CheckConstraintsActionMigration(t *testing.T) {
	onlyRunForEmulatorTest(t)

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "mysql-checkconstraints-actions"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName)

	host, user, srcDb, password := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLDB_CHECK_CONSTRAINT"), os.Getenv("MYSQLPWD")
	args := fmt.Sprintf("schema-and-data -source=%s -prefix=%s -source-profile='host=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s,project=%s'", constants.MYSQL, filePrefix, host, user, srcDb, password, instanceID, dbName, projectID)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(t, dbURI)

	checkCheckConstraints(ctx, t, dbURI)
}

func checkCheckConstraints(ctx context.Context, t *testing.T, dbURI string) {
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Insert or update data
	insertOrUpdateData := func(data map[string]interface{}) error {
		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.InsertOrUpdateMap("TestTable", data),
		})
		return err
	}

	// Check if a constraint violation occurs
	checkConstraintViolation := func(data map[string]interface{}, expectedErr string) {
		err := insertOrUpdateData(data)
		if err == nil || !strings.Contains(err.Error(), expectedErr) {
			t.Fatalf("Expected constraint violation for '%s' but got none or wrong error: %v", expectedErr, err)
		}
	}

	// Test Case 1: Valid Insert for chk_range/chk_DateRange
	err = insertOrUpdateData(map[string]interface{}{
		"ID":           1,
		"Value":        12,
		"Flag":         false,
		"Date":         time.Now(),
		"Name":         "ValidName",
		"EnumValue":    "OptionA",
		"BooleanValue": 1,
	})
	if err != nil {
		t.Fatalf("Failed to insert valid data for chk_range/chk_DateRange: %v", err)
	}

	// Test Case 2: Valid Insert for chk_bitwise
	err = insertOrUpdateData(map[string]interface{}{
		"ID":    2,
		"Name":  "ValidName",
		"Flag":  false,
		"Value": 12, // valid value
	})

	if err != nil {
		t.Fatalf("Failed to insert valid data for chk_bitwise: %v", err)
	}

	// Test Case 3: Invalid Insert for chk_bitwise (Negative Value)
	checkConstraintViolation(map[string]interface{}{
		"ID":    3,
		"Value": -1, // Value < 0
		"Flag":  false,
	}, "chk_bitwise")

	// Test Case 4: Invalid Insert for chk_DateRange (Negative Value)
	checkConstraintViolation(map[string]interface{}{
		"ID":    4,
		"Value": 12,
		"Date":  time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC),
		"Flag":  false,
	}, "chk_DateRange")

	// Test Case 5: Valid Insert for chk_NullValue (Value is not NULL)
	err = insertOrUpdateData(map[string]interface{}{
		"ID":    5,
		"Value": 12, // Value is not NULL
		"Name":  "ValidName",
		"Flag":  false,
	})
	if err != nil {
		t.Fatalf("Failed to insert valid data for chk_NullValue: %v", err)
	}

	// Test Case 6: Invalid Insert for chk_NullValue (NULL Value)
	checkConstraintViolation(map[string]interface{}{
		"ID":    6,
		"Value": nil, // NULL Value is not allowed
		"Flag":  false,
	}, "chk_NullValue")

	// Test Case 7: Valid Insert for chk_StringLength (Name length > 5)
	err = insertOrUpdateData(map[string]interface{}{
		"ID":    7,
		"Name":  "ValidName", // Name length > 5
		"Flag":  false,
		"Value": 12,
	})
	if err != nil {
		t.Fatalf("Failed to insert valid data for chk_StringLength: %v", err)
	}

	// Test Case 8: Invalid Insert for chk_StringLength (Name length <= 5)
	checkConstraintViolation(map[string]interface{}{
		"ID":    8,
		"Name":  "Test", // Name length <= 5
		"Flag":  false,
		"Value": 12,
	}, "chk_StringLength")

	// Test Case 9: Valid Insert for chk_Enum (Valid Enum)
	err = insertOrUpdateData(map[string]interface{}{
		"ID":        9,
		"EnumValue": "OptionB", // Valid enum value
		"Name":      "ValidName",
		"Flag":      false,
		"Value":     12,
	})
	if err != nil {
		t.Fatalf("Failed to insert valid data for chk_Enum: %v", err)
	}

	// Test Case 10: Invalid Insert for chk_Enum (Invalid Enum)
	checkConstraintViolation(map[string]interface{}{
		"ID":        10,
		"EnumValue": "InvalidOption", // Invalid enum value
		"Flag":      false,
		"Value":     12,
	}, "chk_Enum")

	// Test Case 11: Valid Insert for chk_Boolean (Valid boolean 0 or 1)
	err = insertOrUpdateData(map[string]interface{}{
		"ID":           11,
		"Value":        12,
		"Flag":         false,
		"Name":         "ValidName",
		"BooleanValue": 1, // Valid boolean value
	})
	if err != nil {
		t.Fatalf("Failed to insert valid data for chk_Boolean: %v", err)
	}

	// Test Case 12: Invalid Insert for chk_Boolean (Invalid boolean value)
	checkConstraintViolation(map[string]interface{}{
		"ID":           12,
		"Value":        12,
		"Flag":         false,
		"BooleanValue": 2, // Invalid boolean representation
	}, "chk_Boolean")

	// Test Case 13: Valid Insert for chk_range (Valid value between 10 and 1000)
	err = insertOrUpdateData(map[string]interface{}{
		"ID":           13,
		"Value":        12, // Valid value
		"Flag":         false,
		"Name":         "ValidName",
		"BooleanValue": 1,
	})
	if err != nil {
		t.Fatalf("Failed to insert valid data for chk_range: %v", err)
	}

	// Test Case 14: Invalid Insert for chk_range (Invalid value)
	checkConstraintViolation(map[string]interface{}{
		"ID":           14,
		"Value":        5, // Invalid Value
		"Flag":         false,
		"BooleanValue": 1,
	}, "chk_range")

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

	// Verifying that the row to be deleted exists in child - otherwise test will incorrectly pass
	stmt := spanner.Statement{SQL: `SELECT * FROM cart WHERE product_id = "zxi-631"`}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, _ := iter.Next()
	assert.NotNil(t, row, "Expected rows with product_id \"zxi-631\" in table 'cart'")

	// Deleting row from parent table in Spanner DB
	mutation := spanner.Delete("products", spanner.Key{"zxi-631"})
	_, err = client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		t.Fatalf("Failed to delete row: %v", err)
	}

	// Testing ON DELETE CASCADE i.e. row from child (cart) should have been automatically deleted
	stmt = spanner.Statement{SQL: `SELECT * FROM cart WHERE product_id = "zxi-631"`}
	iter = client.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err = iter.Next()
	assert.Equal(t, iterator.Done, err, "Expected rows in table 'cart' with productid 'zxi-631' to be deleted")
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
