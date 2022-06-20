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

package oracle_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/testing/common"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	"google.golang.org/api/iterator"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID  string
	instanceID string

	ctx           context.Context
	databaseAdmin *database.DatabaseAdminClient
)

type SpannerRecord struct {
	Date_t      spanner.NullDate
	Float_t     float64
	Int_t       int64
	Numeric_t   string
	String_t    string
	Timestamp_t string
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
	flag.Parse()
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
	if databaseAdmin == nil {
		t.Skip("Integration tests skipped")
	}
	tmpdir, err := ioutil.TempDir(".", "int-test-")
	if err != nil {
		log.Fatal(err)
	}
	return tmpdir
}

func TestIntegration_ORACLE_SchemaSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)
	filePrefix := filepath.Join(tmpdir, "Oracle_IntTest.")

	args := fmt.Sprintf("schema -prefix %s -source=%s -source-profile='host=localhost,user=STI,dbName=xe,password=test1,port=1521'", filePrefix, constants.ORACLE)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}
func TestIntegration_ORACLE_SchemaAndDataSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dbName := "schema-and-data"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, "Oracle_IntTest.")

	args := fmt.Sprintf("schema-and-data -prefix %s -source=%s  -source-profile='host=localhost,user=STI,dbName=xe,password=test1,port=1521' -target-profile='instance=%s,dbName=%s'", filePrefix, constants.ORACLE, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	defer dropDatabase(t, dbURI)
	checkResults(t, dbURI)
}
func checkResults(t *testing.T, dbURI string) {
	// Make a query to check results.
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkCommonDataType(ctx, t, client)
}

func checkCommonDataType(ctx context.Context, t *testing.T, client *spanner.Client) {
	wantRecord := SpannerRecord{
		Date_t:      spanner.NullDate{Valid: true, Date: civil.Date{Day: 18, Year: 2022, Month: 01}},
		Float_t:     float64(1234.56789),
		Int_t:       int64(42),
		Numeric_t:   "42.000000000",
		String_t:    "some varchar data",
		Timestamp_t: "2022-01-19T11:27:18.262Z",
	}
	var date spanner.NullDate
	var floatVal float64
	var intVal int64
	var numericVal big.Rat
	var stringVal string
	var timeVal spanner.NullTime
	iter := client.Single().Read(ctx, "ALLTYPES", spanner.Key{3}, []string{"DATE_T", "FLOAT_T", "INTEGER_T", "NUMERIC_T", "VARCHAR_T", "TIMESTAMP_T"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&date, &floatVal, &intVal, &numericVal, &stringVal, &timeVal); err != nil {
			t.Fatal(err)
		}

		fmt.Fprintf(os.Stdout, "%v,%v,%v,%v,%v,%v", date, floatVal, intVal, numericVal, stringVal, timeVal)
		gotRecord := SpannerRecord{
			Date_t:      date,
			Float_t:     floatVal,
			Int_t:       intVal,
			Numeric_t:   numericVal.FloatString(9),
			String_t:    stringVal,
			Timestamp_t: timeVal.String(),
		}
		assert.True(t, cmp.Equal(wantRecord, gotRecord))
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
