// Copyright 2022 Google LLC
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

package csv_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/testing/common"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"

	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID  string
	instanceID string

	ctx           context.Context
	databaseAdmin *database.DatabaseAdminClient
)

const (
	ALL_TYPES_CSV      string = "all_data_types.csv"
	MANIFEST_FILE_NAME string = "csv_manifest.json"
)

type SpannerRecord struct {
	AttrBool      bool
	AttrBytes     []byte
	AttrDate      civil.Date
	AttrFloat     float64
	AttrInt       int64
	AttrNumeric   float64
	AttrString    string
	AttrTimestamp time.Time
	AttrJson      spanner.NullJSON
	AttrStringArr []spanner.NullString
	AttrInt64Arr  []spanner.NullInt64
}

func TestMain(m *testing.M) {
	cleanup := initIntegrationTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func initIntegrationTests() (cleanup func()) {
	projectID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}

	if projectID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if instanceID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID is missing")
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

func createSpannerSchema(t *testing.T, project, instance, dbName string) {
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)
	req := &databasepb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	}
	req.CreateStatement = "CREATE DATABASE `" + dbName + "`"
	req.ExtraStatements = []string{"CREATE TABLE all_data_types (" +
		"a BOOL," +
		"b BYTES(50)," +
		"c DATE," +
		"d FLOAT64," +
		"e INT64," +
		"f NUMERIC," +
		"g STRING(50)," +
		"h TIMESTAMP," +
		"i JSON," +
		"j ARRAY<STRING(100)>," +
		"k ARRAY<INT64>," +
		") PRIMARY KEY(e)",
	}
	op, err := databaseAdmin.CreateDatabase(ctx, req)
	if err != nil {
		t.Fatalf("can't build CreateDatabaseRequest for %s", dbURI)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("createDatabase call failed for %s", dbURI)
	}
}

func TestIntegration_CSV_Command(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "csv-test"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)

	writeManifestFile(t)
	writeCSVs(t)
	defer cleanupCSVs()
	defer cleanupManifest()

	// Drop the database later.
	defer dropDatabase(t, dbURI)

	createSpannerSchema(t, projectID, instanceID, dbName)
	args := fmt.Sprintf("data -source=csv -source-profile=manifest=%s -target-profile='instance=%s,dbName=%s'", MANIFEST_FILE_NAME, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}

	checkResults(t, dbURI, false)
}

func writeCSVs(t *testing.T) {
	csvInput := []struct {
		fileName string
		data     []string
	}{
		{
			ALL_TYPES_CSV,
			[]string{
				"a,b,c,d,e,f,g,h,i,j,k\n",
				"true,test,2019-10-29,15.13,100,39.94,Helloworld,2019-10-29 05:30:00,\"{\"\"key1\"\": \"\"value1\"\", \"\"key2\"\": \"\"value2\"\"}\",\"{ab,cd}\",\"[1,2]\"",
			},
		},
	}
	for _, in := range csvInput {
		f, err := os.Create(in.fileName)
		if err != nil {
			t.Fatalf("Could not create %s: %v", in.fileName, err)
		}
		if _, err := f.WriteString(strings.Join(in.data, "")); err != nil {
			t.Fatalf("Could not write to %s: %v", in.fileName, err)
		}
	}
}

func cleanupCSVs() {
	for _, fn := range []string{ALL_TYPES_CSV} {
		os.Remove(fn)
	}
}

func writeManifestFile(t *testing.T) {
	f, err := os.Create(MANIFEST_FILE_NAME)
	if err != nil {
		t.Fatalf("Could not create %s: %v", MANIFEST_FILE_NAME, err)
	}
	defer f.Close()
	_, err = f.WriteString(`
	[
		{
			"table_name": "all_data_types",
			"file_patterns": ["all_data_types.csv"],
			"columns": [
			{ "column_name": "a", "type_name": "BOOL" },
			{ "column_name": "b", "type_name": "BYTES" },
			{ "column_name": "c", "type_name": "DATE" },
			{ "column_name": "d", "type_name": "FLOAT64" },
			{ "column_name": "e", "type_name": "INT64" },
			{ "column_name": "f", "type_name": "NUMERIC" },
			{ "column_name": "g", "type_name": "STRING" },
			{ "column_name": "h", "type_name": "TIMESTAMP" },
			{ "column_name": "i", "type_name": "JSON" }
			]
		}
	]`)
	if err != nil {
		t.Fatalf("Could not write to %s: %v", MANIFEST_FILE_NAME, err)
	}
}

func cleanupManifest() {
	os.Remove(MANIFEST_FILE_NAME)
}

func checkResults(t *testing.T, dbURI string, skipJson bool) {
	// Make a query to check results.
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkRow(ctx, t, client)
}

func checkRow(ctx context.Context, t *testing.T, client *spanner.Client) {
	wantRecord := SpannerRecord{
		AttrBool:      true,
		AttrBytes:     []uint8{0x74, 0x65, 0x73, 0x74},
		AttrDate:      getDate("2019-10-29"),
		AttrFloat:     15.13,
		AttrInt:       int64(100),
		AttrNumeric:   float64(39.94),
		AttrString:    "Helloworld",
		AttrTimestamp: getTime(t, "2019-10-29T05:30:00Z"),
		AttrJson:      spanner.NullJSON{Valid: true},
		AttrStringArr: []spanner.NullString{{StringVal: "ab", Valid: true}, {StringVal: "cd", Valid: true}},
		AttrInt64Arr:  []spanner.NullInt64{{Int64: int64(1), Valid: true}, {Int64: int64(2), Valid: true}},
	}
	json.Unmarshal([]byte("{\"key1\": \"value1\", \"key2\": \"value2\"}"), &wantRecord.AttrJson.Value)

	gotRecord := SpannerRecord{}
	stmt := spanner.Statement{SQL: `SELECT a, b, c, d, e, f, g, h, i, j, k FROM all_data_types`}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Println("Error reading row: ", err)
			t.Fatal(err)
			break
		}
		// We don't create big.Rat fields in the SpannerRecord structs
		// because cmp.Equal cannot compare big.Rat fields automatically.
		var AttrNumeric big.Rat
		if err := row.Columns(&gotRecord.AttrBool, &gotRecord.AttrBytes, &gotRecord.AttrDate, &gotRecord.AttrFloat, &gotRecord.AttrInt, &AttrNumeric, &gotRecord.AttrString, &gotRecord.AttrTimestamp, &gotRecord.AttrJson, &gotRecord.AttrStringArr, &gotRecord.AttrInt64Arr); err != nil {
			log.Println("Error reading into variables: ", err)
			t.Fatal(err)
			break
		}
		gotRecord.AttrNumeric, _ = AttrNumeric.Float64()
	}
	if !cmp.Equal(wantRecord, gotRecord) {
		t.Fatalf("found unequal records, want: %+v, got: %+v", wantRecord, gotRecord)
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}

func getTime(t *testing.T, s string) time.Time {
	x, err := time.Parse(time.RFC3339, s)
	assert.Nil(t, err, fmt.Sprintf("getTime can't parse %s:", s))
	return x
}

func getDate(s string) civil.Date {
	d, _ := civil.ParseDate(s)
	return d
}
