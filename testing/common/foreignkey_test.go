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

func BuildConv(t *testing.T) *internal.Conv {
	conv := internal.MakeConv()
	conv.SpSchema["table_a"] = ddl.CreateTable{
		Name:     "table_a",
		ColNames: []string{"col1", "col2", "col3", "col4", "col5", "col6"},
		ColDefs: map[string]ddl.ColumnDef{
			"col1": ddl.ColumnDef{Name: "col1", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col2": ddl.ColumnDef{Name: "col2", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col3": ddl.ColumnDef{Name: "col3", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col4": ddl.ColumnDef{Name: "col4", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col5": ddl.ColumnDef{Name: "col5", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col6": ddl.ColumnDef{Name: "col6", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "col1"}},
	}
	conv.SpSchema["table_b"] = ddl.CreateTable{
		Name:     "table_b",
		ColNames: []string{"col1", "col2", "col3", "col4", "col5", "col6"},
		ColDefs: map[string]ddl.ColumnDef{
			"col1": ddl.ColumnDef{Name: "col1", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col2": ddl.ColumnDef{Name: "col2", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col3": ddl.ColumnDef{Name: "col3", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col4": ddl.ColumnDef{Name: "col4", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col5": ddl.ColumnDef{Name: "col5", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
			"col6": ddl.ColumnDef{Name: "col6", T: ddl.Type{Name: ddl.String, Len: int64(10)}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "col1"}},
	}
	return conv
}

func addForeignKeysToConv(conv *internal.Conv, t *testing.T) {
	var foreignKeys []ddl.Foreignkey
	for i := 1; i <= 6; i++ {
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

func TestUpdateDDLForeignKeys(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	dbName := fmt.Sprintf("foreign-key-test-two-tables")
	conv := BuildConv(t)

	dbpath, err := conversion.CreateDatabase(projectID, instanceID, dbName, conv, os.Stdout)
	if err != nil {
		t.Fatal(err)
	}
	addForeignKeysToConv(conv, t)
	if err = conversion.UpdateDDLForeignKeys(projectID, instanceID, dbName, 3, conv, os.Stdout); err != nil {
		t.Fatalf("\nCan't perform update operation on db %s with foreign keys: %v\n", dbpath, err)
	}
	ctx := context.Background()
	resp, err := databaseAdmin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: dbpath})
	if err != nil {
		t.Fatalf("Could not read DDL from database %s: %v", dbpath, err)
	}
	var stmta, stmtb string
	if strings.Contains(resp.Statements[0], "CREATE TABLE table_a") {
		stmta, stmtb = resp.Statements[0], resp.Statements[1]
	} else {
		stmta, stmtb = resp.Statements[1], resp.Statements[0]
	}
	assert.False(t, strings.Contains(stmtb, "FOREIGN KEY"))

	wantFkStmts := []string{
		"CONSTRAINT fk_1 FOREIGN KEY(col1) REFERENCES table_b(col1),",
		"CONSTRAINT fk_2 FOREIGN KEY(col2) REFERENCES table_b(col2),",
		"CONSTRAINT fk_3 FOREIGN KEY(col3) REFERENCES table_b(col3),",
		"CONSTRAINT fk_4 FOREIGN KEY(col4) REFERENCES table_b(col4),",
		"CONSTRAINT fk_5 FOREIGN KEY(col5) REFERENCES table_b(col5),",
		"CONSTRAINT fk_6 FOREIGN KEY(col6) REFERENCES table_b(col6)",
	}
	var gotFkStmts []string
	for _, Stmt := range strings.Split(stmta, "\n") {
		if strings.Contains(Stmt, "FOREIGN KEY") {
			gotFkStmts = append(gotFkStmts, strings.TrimSpace(Stmt))
		}
	}

	sort.Strings(gotFkStmts)
	sort.Strings(wantFkStmts)
	log.Println("Want:")
	log.Println(wantFkStmts)
	log.Println("Got:")
	log.Println(gotFkStmts)

	assert.Equal(t, wantFkStmts, gotFkStmts)

	// Drop the database later.
	defer dropDatabase(t, dbpath)

}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
