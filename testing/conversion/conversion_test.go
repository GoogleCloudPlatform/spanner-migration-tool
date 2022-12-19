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

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
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
	projectID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Unit test for UpdateDDLForeignKeys skipped in -short mode.")
		return noop
	}

	if projectID == "" {
		log.Println("Unit test for UpdateDDLForeignKeys skipped: HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if instanceID == "" {
		log.Println("Unit test for UpdateDDLForeignKeys skipped: HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID is missing")
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
		dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, tc.dbName)
		conv := BuildConv(t, tc.numCols, tc.numFks, false)
		err := conversion.CreateDatabase(ctx, databaseAdmin, dbURI, conv, os.Stdout)
		if err != nil {
			t.Fatal(err)
		}
		conversion.MaxWorkers = tc.numWorkers
		if err = conversion.UpdateDDLForeignKeys(ctx, databaseAdmin, dbURI, conv, os.Stdout); err != nil {
			t.Fatalf("\nCan't perform update operation on db %s with foreign keys: %v\n", tc.dbName, err)
		}

		checkResults(t, dbURI, tc.numFks)
		// Drop the database later.
		defer dropDatabase(t, dbURI)
	}
}

func TestVerifyDb(t *testing.T) {
	onlyRunForEmulatorTest(t)

	testCases := []struct {
		dbName      string
		dbExists    bool
		emptySchema bool
	}{
		{"verifydb-exists-schema", true, false},
		{"verifydb-exists-noschema", true, true},
		{"verifydb-does-not-exist", false, true},
	}

	for _, tc := range testCases {
		dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, tc.dbName)
		if tc.dbExists {
			err := conversion.CreateDatabase(ctx, databaseAdmin, dbURI, BuildConv(t, 2, 0, tc.emptySchema), os.Stdout)
			if err != nil {
				t.Fatal(err)
			}
			defer dropDatabase(t, dbURI)
			dbExists, err := conversion.VerifyDb(ctx, databaseAdmin, dbURI)
			assert.True(t, dbExists)
			if tc.emptySchema {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
			}
		} else {
			dbExists, err := conversion.VerifyDb(ctx, databaseAdmin, dbURI)
			assert.Nil(t, err)
			assert.False(t, dbExists)
		}

	}
}

func TestCheckExistingDb(t *testing.T) {
	onlyRunForEmulatorTest(t)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, "check-db-exists")
	err := conversion.CreateDatabase(ctx, databaseAdmin, dbURI, internal.MakeConv(), os.Stdout)
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

	for _, tc := range testCases {
		dbExists, err := conversion.CheckExistingDb(ctx, databaseAdmin, fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, tc.dbName))
		assert.Nil(t, err)
		assert.Equal(t, tc.dbExists, dbExists)
	}
}

func TestValidateDDL(t *testing.T) {
	onlyRunForEmulatorTest(t)

	testCases := []struct {
		dbName      string
		emptySchema bool
	}{
		{"validate-ddl-empty-schema", true},
		{"validate-ddl-non-empty-schema", false},
	}

	for _, tc := range testCases {
		dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, tc.dbName)
		err := conversion.CreateDatabase(ctx, databaseAdmin, dbURI, BuildConv(t, 2, 0, tc.emptySchema), os.Stdout)
		if err != nil {
			t.Fatal(err)
		}
		defer dropDatabase(t, dbURI)
		err = conversion.ValidateDDL(ctx, databaseAdmin, dbURI)
		if tc.emptySchema {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
