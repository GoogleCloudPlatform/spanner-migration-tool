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
	"fmt"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/testing/common"
)

var (
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
	ctx = context.Background()

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

func TestLocalCSVFile(t *testing.T) {
	// configure the database client
	projectID := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID")
	instanceID := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID")

	// clean up the table
	dbName := "versionone"
	tableName := "table2"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, _ = tx.Update(ctx, spanner.NewStatement("DELETE FROM "+tableName+" WHERE 1=1"))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// write new csv data to spanner
	// just trigger the csv command
	MANIFEST_FILE_NAME := "../../test_data/csv_test2.json"
	args := fmt.Sprintf("data -source=csv -source-profile=manifest=%s -target-profile='instance=%s,dbName=%s,project=%s'", MANIFEST_FILE_NAME, instanceID, dbName, projectID)
	err = common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}

	// validate the data

}
