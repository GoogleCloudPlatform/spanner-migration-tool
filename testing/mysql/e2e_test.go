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
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/stretchr/testify/assert"

	"cloud.google.com/go/spanner"
)

type TableLimitTestCase struct {
	name string

	dialect string
	ddls []string

	expectError bool
	expectErrorMessageContains string

	expectedNumberOfTablesCreated int64
}

func TestE2E_CheckTableLimits(t *testing.T) {
	onlyRunForEndToEndTest(t)

	testCases := []TableLimitTestCase {
		{
			name: "Spanner dialect with more than 5000 tables",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: generateCreateTableDdls(5001),

			expectError: true,
			expectErrorMessageContains: "too many tables",
		},
		{
			name: "Postgres dialect with more than 5000 tables",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: generateCreateTableDdls(5001),

			expectError: true,
			expectErrorMessageContains: "too many tables",
		},
		{
			name: "Spanner dialect with exactly 5000 tables",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: generateCreateTableDdls(5000),

			expectedNumberOfTablesCreated: 5000,
		},
		{
			name: "Postgres dialect with exactly 5000 tables",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: generateCreateTableDdls(5000),

			expectedNumberOfTablesCreated: 5000,
		},
	}

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	for _, tc := range testCases {
		runTableLimitTestCase(t, tmpdir, tc)
	}
}

func runTableLimitTestCase(t *testing.T, tmpdir string, tc TableLimitTestCase) {
	dbName := "mysql-table-limits"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)

	filePrefix := filepath.Join(tmpdir, dbName)
	dumpFilePath := filepath.Join(tmpdir, dbName + "_dump.sql")

	writeDumpFile(t, dumpFilePath, tc.ddls)

	args := fmt.Sprintf("schema -prefix %s -source=mysql -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s' < %s", filePrefix, instanceID, dbName, projectID, tc.dialect, dumpFilePath)
	stdout, err := RunCommandReturningStdOut(args, projectID)

	if tc.expectError {
		assert.Error(t, err, tc.name)

		output := stdout + err.Error()
		assert.Contains(t, output, tc.expectErrorMessageContains, tc.name)
		checkDatabaseNotCreatedOrEmpty(t, dbURI, tc.dialect, tc.name)
	} else {
		assert.NoError(t, err, tc.name)
		checkDatabaseSchema(t, dbURI, tc)
	}
}

func checkDatabaseNotCreatedOrEmpty(t *testing.T, dbURI, dialect, testName string) {
	sp, err := spanneraccessor.NewSpannerAccessorClientImpl(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ctx)
	dbExists, err := sp.CheckExistingDb(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	// The Postgres dialect creates the DB and adds tables in two separate calls, so the DB will exist but it
	// should be empty
	if dialect == constants.DIALECT_POSTGRESQL {
		assert.True(t, dbExists, testName)

		client, err := spanner.NewClient(ctx, dbURI)
		if err != nil {
			log.Fatal(err)
		}
		defer client.Close()

		checkNumberOfTables(t, client, 0, testName)
	} else {
		assert.False(t, dbExists, testName)
	}
}

func checkDatabaseSchema(t *testing.T, dbURI string, tc TableLimitTestCase) {
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkNumberOfTables(t, client, tc.expectedNumberOfTablesCreated, tc.name)
}

func checkNumberOfTables(t *testing.T, client *spanner.Client, expectedNumberOfTablesCreated int64, testName string) {
	query := spanner.Statement{SQL : `SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS') AND TABLE_TYPE = 'BASE TABLE'`}
	iter := client.Single().Query(ctx, query)
	defer iter.Stop()
	var numberOfTablesCreated int64
	row, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	row.Columns(&numberOfTablesCreated)

	assert.Equal(t, expectedNumberOfTablesCreated, numberOfTablesCreated, testName)
}

func generateCreateTableDdls(numTables int) []string {
	tableDdls := make([]string, 0)
	for i := 1; i <= numTables; i++ {
		tableName := fmt.Sprintf("Table%d", i)
		tableDdls = append(tableDdls, generateCreateTableDdl(tableName))
	}
	return tableDdls
}

func generateCreateTableDdl(tableName string) string {
	return fmt.Sprintf("CREATE TABLE %s (c1 int PRIMARY KEY);", tableName)
}

func writeDumpFile(t *testing.T, dumpFilePath string, ddls []string) {
	writeDumpErr := os.WriteFile(dumpFilePath, []byte(strings.Join(ddls, "\n")), os.FileMode(0644))
	if writeDumpErr != nil {
		t.Fatal(writeDumpErr)
	}
}

func onlyRunForEndToEndTest(t *testing.T) {
	if os.Getenv("SPANNER_MIGRATION_TOOL_RUN_E2E_TESTS") == "" || os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		t.Skip("Skipping end-to-end tests. To run end-to-end tests, set SPANNER_MIGRATION_TOOL_RUN_E2E_TESTS env var to true, unset SPANNER_EMULATOR_HOST env var and ensure SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID and SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID env vars are set.")
	}
}

func RunCommandReturningStdOut(args string, projectID string) (string, error) {
	cmd := exec.Command("bash", "-c", fmt.Sprintf("go run github.com/GoogleCloudPlatform/spanner-migration-tool %v", args))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GCLOUD_PROJECT=%s", projectID),
	)
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		return out.String(), err
	}
	return out.String(), nil
}
