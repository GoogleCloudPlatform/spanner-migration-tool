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

package sqlserver

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/testing/common"

	"google.golang.org/api/iterator"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID  string
	instanceID string

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
	projectID = os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for testing.Short().
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

func TestIntegration_SQLserver_SchemaSubcommand(t *testing.T) {
	onlyRunForOmniTest(t)

	t.Parallel()
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)
	dbName := fmt.Sprintf("sqlserver-%d", rand.Intn(1000000))
	filePrefix := filepath.Join(tmpdir, "SqlServer_IntTest.")

	host, port, user, password, srcDb := os.Getenv("SQLSERVERHOST"), os.Getenv("SQLSERVERPORT"), os.Getenv("SQLSERVERUSER"), os.Getenv("MSSQL_SA_PASSWORD"), os.Getenv("SQLSERVERDATABASE")
	args := fmt.Sprintf("schema -prefix %s -source=sqlserver -source-profile='host=%s,port=%s,user=%s,password=%s,dbName=%s' -target-profile='instance=%s,dbName=%s,project=%s'", filePrefix, host, port, user, password, srcDb, instanceID, dbName, projectID)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}
func TestIntegration_SQLserver_SchemaAndDataSubcommand(t *testing.T) {
	onlyRunForOmniTest(t)

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)
	dbName := fmt.Sprintf("sqlserver-%d", rand.Intn(1000000))
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, "SqlServer_IntTest.")

	host, port, user, password, srcDb := os.Getenv("SQLSERVERHOST"), os.Getenv("SQLSERVERPORT"), os.Getenv("SQLSERVERUSER"), os.Getenv("MSSQL_SA_PASSWORD"), os.Getenv("SQLSERVERDATABASE")
	args := fmt.Sprintf("schema-and-data -prefix %s -source=%s -source-profile='host=%s,port=%s,user=%s,password=%s,dbName=%s' -target-profile='instance=%s,dbName=%s,project=%s'", filePrefix, constants.SQLSERVER, host, port, user, password, srcDb, instanceID, dbName, projectID)
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
	checkOtherTables(ctx, t, client)
}

func checkCommonDataType(ctx context.Context, t *testing.T, client *spanner.Client) {
	var (
		boolVal             bool
		date                spanner.NullDate
		floatVal            float64
		bigIntVal           int64
		numericVal          big.Rat
		textVal             string
		timeVal             string
		moneyVal            big.Rat
		binaryVal           []byte
		charVal             string
		dateTime            time.Time
		dateTime2           time.Time
		dateTimeOffset      time.Time
		decimalVal          big.Rat
		geographyVal        string
		geometryVal         string
		hierarchyIdVal      string
		imageVal            []byte
		intVal              int64
		nCharVal            string
		nTextVal            string
		nVarCharVal         string
		nVarCharMaxVal      string
		realVal             float32
		smallDateTime       time.Time
		smallIntVal         int64
		smallMoneyVal       big.Rat
		sqlVariantVal       string
		timeStampVal        []byte
		tinyIntVal          int64
		uniqueIdentifierVal uuid.UUID
		varBinaryVal        []byte
		varBinaryMaxVal     []byte
		varCharVal          string
		varCharMaxVal       string
		xmlVal              string
		sysnameVal          string
	)
	iter := client.Single().Read(ctx, "AllTypes", spanner.Key{1}, []string{
		"Bit", "Date", "Float", "BigInt", "Numeric", "Text", "Time", "Money",
		"Binary", "Char", "DateTime", "DateTime2", "DateTimeOffset", "Decimal",
		"Geography", "Geometry", "HierarchyId", "Image", "Int", "NChar", "NText",
		"NVarChar", "NVarCharMax", "Real", "SmallDateTime", "SmallInt", "SmallMoney",
		"SQLVariant", "TimeStamp", "TinyInt", "UniqueIdentifier", "VarBinary",
		"VarBinaryMax", "VarChar", "VarCharMax", "Xml", "SysName",
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&boolVal, &date, &floatVal, &bigIntVal, &numericVal, &textVal, &timeVal, &moneyVal,
			&binaryVal, &charVal, &dateTime, &dateTime2, &dateTimeOffset, &decimalVal,
			&geographyVal, &geometryVal, &hierarchyIdVal, &imageVal, &intVal, &nCharVal, &nTextVal,
			&nVarCharVal, &nVarCharMaxVal, &realVal, &smallDateTime, &smallIntVal, &smallMoneyVal,
			&sqlVariantVal, &timeStampVal, &tinyIntVal, &uniqueIdentifierVal, &varBinaryVal,
			&varBinaryMaxVal, &varCharVal, &varCharMaxVal, &xmlVal, &sysnameVal); err != nil {
			t.Fatal(err)
		}
	}

	if got, want := boolVal, false; got != want {
		t.Fatalf("Bit are not correct: got %v, want %v", got, want)
	}
	if got, want := date.String(), "2021-12-15"; got != want {
		t.Fatalf("Date are not correct: got %v, want %v", got, want)
	}
	if got, want := floatVal, 1.2; got != want {
		t.Fatalf("Float are not correct: got %v, want %v", got, want)
	}
	if got, want := bigIntVal, int64(-9223372036854775808); got != want {
		t.Fatalf("BigInt are not correct: got %v, want %v", got, want)
	}
	if got, want := numericVal.FloatString(9), "1.123456789"; got != want {
		t.Fatalf("Numeric are not correct: got %v, want %v", got, want)
	}
	if got, want := textVal, "Lorem ipsum dolor sit amet"; got != want {
		t.Fatalf("Text are not correct: got %v, want %v", got, want)
	}
	if got, want := timeVal, "07:39:52.950"; !strings.HasPrefix(got, want) {
		t.Fatalf("Time are not correct: got %v, want %v", got, want)
	}
	if got, want := moneyVal.FloatString(4), "922337203685477.5807"; got != want {
		t.Fatalf("Money are not correct: got %v, want %v", got, want)
	}

	binWant, _ := hex.DecodeString("42696E6172792064617461000000000000000000000000000000000000000000000000000000000000000000000000000000")
	if !bytes.Equal(binaryVal, binWant) {
		t.Fatalf("Binary are not correct: got %v, want %v", binaryVal, binWant)
	}
	if got, want := charVal, "ABC       "; got != want {
		t.Fatalf("Char are not correct: got %v, want %v", got, want)
	}
	if got, want := dateTime.UTC().Format(time.RFC3339Nano), "2021-12-15T07:39:52.943Z"; got != want {
		t.Fatalf("DateTime are not correct: got %v, want %v", got, want)
	}
	if got, want := dateTime2.UTC().Format(time.RFC3339Nano), "2021-12-15T07:39:52.9433333Z"; got != want {
		t.Fatalf("DateTime2 are not correct: got %v, want %v", got, want)
	}
	// DateTimeOffset inserts "2021-12-15T07:39:52.9433333+01:20" which corresponds to UTC "2021-12-15T06:19:52.9433333Z"
	if got, want := dateTimeOffset.UTC().Format(time.RFC3339Nano), "2021-12-15T06:19:52.9433333Z"; got != want {
		t.Fatalf("DateTimeOffset are not correct: got %v, want %v", got, want)
	}
	if got, want := decimalVal.FloatString(9), "123456789.123456789"; got != want {
		t.Fatalf("Decimal are not correct: got %v, want %v", got, want)
	}
	if got, want := geographyVal, "POINT (-122.359 47.65129)"; got != want {
		t.Fatalf("Geography are not correct: got %v, want %v", got, want)
	}
	if got, want := geometryVal, "LINESTRING (-122.36 47.656, -122.343 47.656)"; got != want {
		t.Fatalf("Geometry are not correct: got %v, want %v", got, want)
	}
	if got, want := hierarchyIdVal, "/2/"; got != want {
		t.Fatalf("HierarchyId are not correct: got %v, want %v", got, want)
	}
	imgWant, _ := hex.DecodeString("5465787420617320696D616765")
	if !bytes.Equal(imageVal, imgWant) {
		t.Fatalf("Image are not correct: got %v, want %v", imageVal, imgWant)
	}
	if got, want := intVal, int64(42); got != want {
		t.Fatalf("Int are not correct: got %v, want %v", got, want)
	}
	if got, want := nCharVal, "A         "; got != want {
		t.Fatalf("NChar are not correct: got %v, want %v", got, want)
	}
	if got, want := nTextVal, "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."; got != want {
		t.Fatalf("NText are not correct: got %v, want %v", got, want)
	}
	if got, want := nVarCharVal, "ABC"; got != want {
		t.Fatalf("NVarChar are not correct: got %v, want %v", got, want)
	}
	if got, want := nVarCharMaxVal, "ABCD"; got != want {
		t.Fatalf("NVarCharMax are not correct: got %v, want %v", got, want)
	}
	if got, want := realVal, float32(5.5); got != want {
		t.Fatalf("Real are not correct: got %v, want %v", got, want)
	}
	if got, want := smallDateTime.UTC().Format(time.RFC3339Nano), "2021-12-15T07:40:00Z"; got != want {
		t.Fatalf("SmallDateTime are not correct: got %v, want %v", got, want)
	}
	if got, want := smallIntVal, int64(32767); got != want {
		t.Fatalf("SmallInt are not correct: got %v, want %v", got, want)
	}
	if got, want := smallMoneyVal.FloatString(4), "214748.3647"; got != want {
		t.Fatalf("SmallMoney are not correct: got %v, want %v", got, want)
	}
	if got, want := sqlVariantVal, "1.200000000000000001234"; !strings.Contains(got, "1.2") { // Can be tricky due to precision
		t.Fatalf("SQLVariant are not correct: got %v, want %v", got, want)
	}
	if len(timeStampVal) == 0 {
		t.Fatalf("TimeStamp are not correct: got empty")
	}
	if got, want := tinyIntVal, int64(255); got != want {
		t.Fatalf("TinyInt are not correct: got %v, want %v", got, want)
	}
	if got, want := uniqueIdentifierVal.String(), "5d434705-4f3e-461e-ae36-0f96834a70ba"; got != want {
		t.Fatalf("UniqueIdentifier are not correct: got %v, want %v", got, want)
	}
	varBinWant, _ := hex.DecodeString("56617242696E617279283530292064617461")
	if !bytes.Equal(varBinaryVal, varBinWant) {
		t.Fatalf("VarBinary are not correct: got %v, want %v", varBinaryVal, varBinWant)
	}
	varBinMaxWant, _ := hex.DecodeString("56617242696E617279286D6178292064617461")
	if !bytes.Equal(varBinaryMaxVal, varBinMaxWant) {
		t.Fatalf("VarBinaryMax are not correct: got %v, want %v", varBinaryMaxVal, varBinMaxWant)
	}
	if got, want := varCharVal, "ABCDE ABCDE ABCDE ABCDE ABCDE ABCDE ABCDE ABCDE AB"; got != want {
		t.Fatalf("VarChar are not correct: got %v, want %v", got, want)
	}
	if got, want := varCharMaxVal, "ABCDE ABCDE ABCDE ABCDE ABCDE ABCDE ABCDE ABCDE ABCDE ABCDE"; got != want {
		t.Fatalf("VarCharMax are not correct: got %v, want %v", got, want)
	}
	if got, want := xmlVal, "<note><to>Tove</to><from>Jani</from><heading>Reminder</heading><body>Don't forget me this weekend!</body></note>"; got != want {
		t.Fatalf("Xml are not correct: got %v, want %v", got, want)
	}
	if got, want := sysnameVal, "sysname_test_1"; got != want {
		t.Fatalf("SysName are not correct: got %v, want %v", got, want)
	}
}

func onlyRunForOmniTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against Spanner Omni.")
	}
}

func checkOtherTables(ctx context.Context, t *testing.T, client *spanner.Client) {
	// Check go_get table
	iter := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT count(*) FROM go_get"})
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		t.Logf("Failed to query go_get (might have a different name): %v", err)
	} else {
		var count int64
		if err := row.Columns(&count); err != nil {
			t.Fatal(err)
		}
		if count != 7 {
			t.Fatalf("go_get count mismatch, got %d, want 7", count)
		}
	}

	// Check RowVersionType
	iter2 := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT count(*) FROM RowVersionType"})
	defer iter2.Stop()
	row2, err := iter2.Next()
	if err != nil {
		t.Logf("Failed to query RowVersionType: %v", err)
	} else {
		var count int64
		if err := row2.Columns(&count); err != nil {
			t.Fatal(err)
		}
		if count != 3 {
			t.Fatalf("RowVersionType count mismatch, got %d, want 3", count)
		}
	}
}
