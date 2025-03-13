/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package import_data_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/testing/common"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID     string
	instanceID    string
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
	projectID := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID")
	instanceID := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for calling testing.Short().

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
		// clean up the table -  skip for now for validation
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}

func TestLocalCSVFile(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()
	// explicitly setting for test.
	projectID := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID")
	instanceID := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID")
	log.Printf("projectID %s, instanceID %s", projectID, instanceID)

	// configure the database client
	dbName := "versionone"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	log.Printf("dbURI %s", dbURI)

	createSpannerDatabase(t, projectID, instanceID, dbName)

	// write new csv data to spanner
	// just trigger the csv command
	manifestFileName := "../../test_data/import_data_integ_test_csv.json"
	args := fmt.Sprintf("data -source=csv -source-profile=manifest=%s -target-profile='instance=%s,dbName=%s,project=%s'", manifestFileName, instanceID, dbName, projectID)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}

	// validate the data

}

func createSpannerDatabase(t *testing.T, project, instance, dbName string) {
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)
	req := &databasepb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	}

	req.CreateStatement = "CREATE DATABASE `" + dbName + "`"
	req.ExtraStatements = []string{"CREATE TABLE table2 (" +
		"c3 INT64," +
		"c4 STRING(100)" +
		") PRIMARY KEY(c3)",
	}
	op, err := databaseAdmin.CreateDatabase(ctx, req)
	if err != nil {
		t.Fatalf("can't build CreateDatabaseRequest for %s: %v", dbURI, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("createDatabase call failed for %s: %v", dbURI, err)
	}
}
