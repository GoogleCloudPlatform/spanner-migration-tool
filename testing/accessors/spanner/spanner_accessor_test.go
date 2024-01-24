// Copyright 2024 Google LLC
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

// TODO: Refactor this file and other integration tests by moving all common code
// to remove redundancy.

package spanneraccessor_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var (
	projectID  string
	instanceID string

	ctx           context.Context
	databaseAdmin *database.DatabaseAdminClient
)

// This test should move as a mock unit test inside accessors itself.
func TestMain(m *testing.M) {
	cleanup := initTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func init() {
	logger.Log = zap.NewNop()
}

func initTests() (cleanup func()) {
	projectID = os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Unit test for UpdateDDLForeignKeys skipped in -short mode.")
		return noop
	}

	if projectID == "" {
		log.Println("Unit test for UpdateDDLForeignKeys skipped: SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if instanceID == "" {
		log.Println("Unit test for UpdateDDLForeignKeys skipped: SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID is missing")
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

func dropDatabase(t *testing.T, dbPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// Drop the testing database.
	if err := databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbPath}); err != nil {
		t.Fatalf("failed to drop testing database %v: %v", dbPath, err)
	}
}

func TestCheckExistingDb(t *testing.T) {
	onlyRunForEmulatorTest(t)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, "check-db-exists")
	err := conversion.CreateDatabase(ctx, databaseAdmin, dbURI, internal.MakeConv(), os.Stdout, "", constants.BULK_MIGRATION)
	if err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(t, dbURI)
	testCases := []struct {
		dbName   string
		dbExists bool
	}{
		{"check-db-exists", true},
		{"check-db-does-not-exist", false},
	}
	spA := spanneraccessor.SpannerAccessorImpl{}
	for _, tc := range testCases {
		dbExists, err := spA.CheckExistingDb(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, tc.dbName))
		assert.Nil(t, err)
		assert.Equal(t, tc.dbExists, dbExists)
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
