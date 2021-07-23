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
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"

	database "cloud.google.com/go/spanner/admin/database/apiv1"

	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID  string
	instanceID string

	databaseAdmin *database.DatabaseAdminClient
)

func TestMain(m *testing.M) {
	cleanup := initTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func initTests() (cleanup func()) {
	projectID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID")

	ctx := context.Background()
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

func BuildConv(t *testing.T, numCols int) *internal.Conv {
	conv := internal.MakeConv()
	colNames := []string{}
	colDefs := map[string]ddl.ColumnDef{}
	for i := 1; i <= numCols; i++ {
		currColName := fmt.Sprintf("col%d", i)
		colNames = append(colNames, currColName)
		colDefs[currColName] = ddl.ColumnDef{Name: currColName, T: ddl.Type{Name: ddl.String, Len: int64(10)}}
	}
	conv.SpSchema["table_a"] = ddl.CreateTable{
		Name:     "table_a",
		ColNames: colNames,
		ColDefs:  colDefs,
		Pks:      []ddl.IndexKey{ddl.IndexKey{Col: "col1"}},
	}
	conv.SpSchema["table_b"] = ddl.CreateTable{
		Name:     "table_b",
		ColNames: colNames,
		ColDefs:  colDefs,
		Pks:      []ddl.IndexKey{ddl.IndexKey{Col: "col1"}},
	}
	return conv
}

func addForeignKeysToConv(t *testing.T, conv *internal.Conv, numFks int) {
	var foreignKeys []ddl.Foreignkey
	for i := 1; i <= numFks; i++ {
		foreignKey := ddl.Foreignkey{
			Name:         fmt.Sprintf("fk_%d", i),
			Columns:      []string{fmt.Sprintf("col%d", i)},
			ReferTable:   "table_b",
			ReferColumns: []string{fmt.Sprintf("col%d", i)}}
		foreignKeys = append(foreignKeys, foreignKey)
	}
	spTable := conv.SpSchema["table_a"]
	spTable.Fks = foreignKeys
	conv.SpSchema["table_a"] = spTable
}

func checkResults(t *testing.T, dbpath string, numFks int) {
	ctx := context.Background()
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

func UpdateDDLForeignKeysUtil(t *testing.T, dbName string, numCols, numWorkers, numFks int) {
	// Build a conv without foreign key statements to create just the tables during CreateDatabase.
	conv := BuildConv(t, numCols)

	dbpath, err := conversion.CreateDatabase(projectID, instanceID, dbName, conv, os.Stdout)
	if err != nil {
		t.Fatal(err)
	}

	addForeignKeysToConv(t, conv, numFks)
	conversion.MaxWorkers = numWorkers
	if err = conversion.UpdateDDLForeignKeys(projectID, instanceID, dbName, conv, os.Stdout); err != nil {
		t.Fatalf("\nCan't perform update operation on db %s with foreign keys: %v\n", dbName, err)
	}

	checkResults(t, dbpath, numFks)
	// Drop the database later.
	defer dropDatabase(t, dbpath)
}

func TestUpdateDDLForeignKeys(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()
	UpdateDDLForeignKeysUtil(t, "test-workers-five-fks", 10, 1, 5)
	UpdateDDLForeignKeysUtil(t, "test-workers-ten-fks", 10, 1, 10)
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
