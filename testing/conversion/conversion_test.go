// Copyright 2021 Google LLC
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

package conversion_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID  string
	instanceID string

	ctx           context.Context
	databaseAdmin *database.DatabaseAdminClient
)

func init() {
	logger.Log = zap.NewNop()
}

func TestMain(m *testing.M) {
	cleanup := initTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
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

func BuildConv(t *testing.T, numCols, numFks int, makeEmpty bool) *internal.Conv {
	conv := internal.MakeConv()
	if makeEmpty {
		return conv
	}
	colIds := []string{}
	colDefs := map[string]ddl.ColumnDef{}
	for i := 1; i <= numCols; i++ {
		currColName := fmt.Sprintf("col%d", i)
		currColId := fmt.Sprintf("c%d", i)
		colIds = append(colIds, currColId)
		colDefs[currColId] = ddl.ColumnDef{Name: currColName, T: ddl.Type{Name: ddl.String, Len: int64(10)}}
	}

	var foreignKeys []ddl.Foreignkey
	for i := 1; i <= numFks; i++ {
		foreignKey := ddl.Foreignkey{
			Name:           fmt.Sprintf("fk_%d", i),
			ColIds:         []string{fmt.Sprintf("c%d", i)},
			ReferTableId:   "t2",
			ReferColumnIds: []string{fmt.Sprintf("c%d", i)}}
		foreignKeys = append(foreignKeys, foreignKey)
	}

	conv.SpSchema["t1"] = ddl.CreateTable{
		Name:        "table_a",
		ColIds:      colIds,
		ColDefs:     colDefs,
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1"}},
		ForeignKeys: foreignKeys,
		Id:          "t1",
	}
	conv.SpSchema["t2"] = ddl.CreateTable{
		Name:        "table_b",
		ColIds:      colIds,
		ColDefs:     colDefs,
		PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1"}},
		Id:          "t2",
	}
	return conv
}

func checkResults(t *testing.T, dbpath string, numFks int) {
	resp, err := databaseAdmin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: dbpath})
	if err != nil {
		t.Fatalf("Could not read DDL from database %s: %v", dbpath, err)
	}

	// Each statement in the response is the DDL for a whole table including column names, foreign key statements and primary keys.
	// The data type is bytes. Sample resp.Statements:
	// ["2021/07/16 13:45:14 CREATE TABLE table_a (\n  col1 STRING(10),\n  col2 STRING(10),\n) PRIMARY KEY(col1);"]
	var stmta, stmtb string
	if strings.Contains(resp.Statements[0], "CREATE TABLE table_a") {
		stmta, stmtb = resp.Statements[0], resp.Statements[1]
	} else {
		stmta, stmtb = resp.Statements[1], resp.Statements[0]
	}
	assert.False(t, strings.Contains(stmtb, "FOREIGN KEY"))

	wantFkStmts := []string{}
	for i := 1; i <= numFks; i++ {
		fkStmt := fmt.Sprintf("CONSTRAINT fk_%d FOREIGN KEY(col%d) REFERENCES table_b(col%d),", i, i, i)
		wantFkStmts = append(wantFkStmts, fkStmt)
	}
	var gotFkStmts []string
	// Filter out just the foreign key statements.
	for _, Stmt := range strings.Split(stmta, "\n") {
		if strings.Contains(Stmt, "FOREIGN KEY") {
			gotFkStmts = append(gotFkStmts, strings.TrimSpace(Stmt))
		}
	}

	sort.Strings(gotFkStmts)
	sort.Strings(wantFkStmts)

	assert.Equal(t, wantFkStmts, gotFkStmts)
}

func TestUpdateDDLForeignKeys(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()
	testCases := []struct {
		dbName     string
		numCols    int // Number of columns in the table.
		numFks     int // Number of foreign keys we want to add (ensure it is not greater than numCols).
		numWorkers int // Number of concurrent workers (we set it as 1 for now since spanner emulator does not support concurrent schema updates yet).
	}{
		{"test-workers-five-fks", 10, 5, 1},
		{"test-workers-ten-fks", 10, 10, 1},
	}

	for _, tc := range testCases {
		adminClientImpl, err := spanneradmin.NewAdminClientImpl(ctx)
		if err != nil {
			t.Fatal(err)
		}
		spA := spanneraccessor.SpannerAccessorImpl{AdminClient: adminClientImpl}
		dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, tc.dbName)
		conv := BuildConv(t, tc.numCols, tc.numFks, false)
		err = spA.CreateDatabase(ctx, dbURI, conv, "", constants.BULK_MIGRATION)
		if err != nil {
			t.Fatal(err)
		}
		spanneraccessor.MaxWorkers = tc.numWorkers
		spA.UpdateDDLForeignKeys(ctx, dbURI, conv, "", constants.BULK_MIGRATION)

		checkResults(t, dbURI, tc.numFks)
		// Drop the database later.
		defer dropDatabase(t, dbURI)
	}
}

func TestVerifyDb(t *testing.T) {
	onlyRunForEmulatorTest(t)

	testCases := []struct {
		dbName                  string
		dbExists                bool
		tablesExistingOnSpanner []string
		expectError             bool
	}{
		{"verifydb-exists-schema-clash", true, []string{"table_a"}, true},
		{"verifydb-exists-schema-noclash", true, []string{"unrelated_table"}, false},
		{"verifydb-exists-noschema", true, []string{}, false},
		{"verifydb-does-not-exist", false, []string{}, false},
	}

	for _, tc := range testCases {
		dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, tc.dbName)
		adminClientImpl, err := spanneradmin.NewAdminClientImpl(ctx)
		if err != nil {
			t.Fatal(err)
		}
		spA := spanneraccessor.SpannerAccessorImpl{AdminClient: adminClientImpl}
		tablesExistingOnSpanner := tc.tablesExistingOnSpanner
		conv := internal.MakeConv()
		if len(tablesExistingOnSpanner) > 0 {
			conv.SpSchema["t1"] = ddl.CreateTable{
				Name:        tablesExistingOnSpanner[0],
				ColIds:      []string{"c1"},
				ColDefs:     map[string]ddl.ColumnDef{"c1": {Name: "col1", T: ddl.Type{Name: ddl.String, Len: 10}}},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
				Id:          "t1",
			}
		}
		if tc.dbExists {
			err = spA.CreateDatabase(ctx, dbURI, conv, "", constants.BULK_MIGRATION)
			if err != nil {
				t.Fatal(err)
			}
			defer dropDatabase(t, dbURI)
			dbExists, err := spA.VerifyDb(ctx, dbURI, BuildConv(t, 2, 0, false), tablesExistingOnSpanner)
			assert.True(t, dbExists)
			if tc.expectError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		} else {
			dbExists, err := spA.VerifyDb(ctx, dbURI, conv, tablesExistingOnSpanner)
			assert.Nil(t, err)
			assert.False(t, dbExists)
		}
	}
}

func TestValidateDDL(t *testing.T) {
	onlyRunForEmulatorTest(t)

	testCases := []struct {
		dbName                  string
		conv                    *internal.Conv
		tablesExistingOnSpanner []string
		expectError             bool
	}{
		{
			dbName:                  "validate-ddl-no-spanner-tables",
			conv:                    BuildConv(t, 2, 0, false),
			tablesExistingOnSpanner: []string{},
			expectError:             false,
		},
		{
			dbName:                  "validate-ddl-clash-free-spanner-tables",
			conv:                    BuildConv(t, 2, 0, false),
			tablesExistingOnSpanner: []string{"unrelated_table"},
			expectError:             false,
		},
		{
			dbName:                  "validate-ddl-collision",
			conv:                    BuildConv(t, 2, 0, false),
			tablesExistingOnSpanner: []string{"table_a"},
			expectError:             true,
		},
	}

	for _, tc := range testCases {
		adminClientImpl, err := spanneradmin.NewAdminClientImpl(ctx)
		if err != nil {
			t.Fatal(err)
		}
		spA := spanneraccessor.SpannerAccessorImpl{AdminClient: adminClientImpl}

		err = spA.ValidateDDL(ctx, tc.conv, tc.tablesExistingOnSpanner)
		if tc.expectError {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}
	}
}

func TestGetTableNamesFromSpanner(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	type testCase struct {
		name           string
		dialect        string
		numTables      int
		expectedTables []string
	}

	testCases := []testCase{
		{
			name:           "gsql-no-tables",
			dialect:        constants.DIALECT_GOOGLESQL,
			numTables:      0,
			expectedTables: []string{},
		},
		{
			name:           "gsql-two-tables",
			dialect:        constants.DIALECT_GOOGLESQL,
			numTables:      2,
			expectedTables: []string{"table_a", "table_b"},
		},
		{
			name:           "pgsql-no-tables",
			dialect:        constants.DIALECT_POSTGRESQL,
			numTables:      0,
			expectedTables: []string{},
		},
		{
			name:           "pgsql-two-tables",
			dialect:        constants.DIALECT_POSTGRESQL,
			numTables:      2,
			expectedTables: []string{"table_a", "table_b"},
		},
	}

	for _, tc := range testCases {
		dbName := fmt.Sprintf("test-%s", tc.name)
		dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
		conv := BuildConv(t, 1, 0, true)
		if len(tc.expectedTables) > 0 {
			conv = BuildConv(t, 1, 0, false)

		}
		conv.SpDialect = tc.dialect

		adminClientImpl, err := spanneradmin.NewAdminClientImpl(ctx)
		if err != nil {
			t.Fatal(err)
		}
		spA := spanneraccessor.SpannerAccessorImpl{AdminClient: adminClientImpl}

		err = spA.CreateDatabase(ctx, dbURI, conv, "", constants.BULK_MIGRATION)
		if err != nil {
			t.Fatal(err)
		}
		defer dropDatabase(t, dbURI)

		spClient, err := sp.NewClient(ctx, dbURI)
		if err != nil {
			t.Fatalf("failed to create spanner client: %v", err)
		}

		tableNames, err := spA.GetTableNamesFromSpanner(ctx, tc.dialect, dbURI, spClient)
		if err != nil {
			t.Fatalf("GetTableNamesFromSpanner failed: %v", err)
		}
		assert.ElementsMatch(t, tc.expectedTables, tableNames)
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
