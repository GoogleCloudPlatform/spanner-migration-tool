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

package dynamodb_snapshot_test

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

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	dydb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/testing/common"
)

var (
	projectID  string
	instanceID string

	ctx           context.Context
	databaseAdmin *database.DatabaseAdminClient
	dydbClient    *dydb.DynamoDB
)

// Create struct to hold info about new item in dynamodb.
type DydbRecord struct {
	AttrString    string
	AttrInt       int
	AttrFloat     float64
	AttrBool      bool
	AttrBytes     []byte
	AttrNumberSet []float64 `dynamodbav:",numberset"`
	AttrByteSet   [][]byte
	AttrStringSet []string `dynamodbav:",stringset"`
	AttrList      []interface{}
	AttrMap       map[string]int
}

type SpannerRecord struct {
	AttrString    string
	AttrInt       int64
	AttrFloat     float64
	AttrBool      bool
	AttrBytes     []byte
	AttrNumberSet []float64
	AttrByteSet   [][]byte
	AttrStringSet []string
	AttrList      string
	AttrMap       string
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

	cfg := aws.Config{
		Endpoint: aws.String("http://localhost:8000"),
	}
	sess := session.Must(session.NewSession())
	dydbClient = dydb.New(sess, &cfg)

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

	deleteTableInput := &dydb.DeleteTableInput{
		TableName: aws.String("table_test"),
	}
	dydbClient.DeleteTable(deleteTableInput)
}

func prepareIntegrationTest(t *testing.T) string {
	if databaseAdmin == nil {
		t.Skip("Integration tests skipped")
	}
	tmpdir, err := ioutil.TempDir(".", "int-test-")
	if err != nil {
		log.Fatal(err)
	}
	populateDynamoDB(t)
	return tmpdir
}

func populateDynamoDB(t *testing.T) {
	tableName := "table_test"
	createTableInput := &dydb.CreateTableInput{
		AttributeDefinitions: []*dydb.AttributeDefinition{
			{
				AttributeName: aws.String("AttrString"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dydb.KeySchemaElement{
			{
				AttributeName: aws.String("AttrString"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dydb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(tableName),
	}
	_, err := dydbClient.CreateTable(createTableInput)
	if err != nil {
		t.Fatalf("Got error calling CreateTable: %s", err)
	}
	dydbRecord := DydbRecord{
		AttrString:    "abcd",
		AttrInt:       10,
		AttrFloat:     14.5,
		AttrBool:      true,
		AttrBytes:     []byte{48, 49},
		AttrNumberSet: []float64{1.5, 2.5, 3.5},
		AttrByteSet:   [][]byte{[]byte{48, 49}, []byte{50, 51}},
		AttrStringSet: []string{"abc", "xyz"},
		AttrList:      []interface{}{"str-1", 12.34, true},
		AttrMap:       map[string]int{"key": 100},
	}
	av, err := dynamodbattribute.MarshalMap(dydbRecord)
	if err != nil {
		t.Fatalf("Got error marshalling new movie item: %s", err)
	}

	putItemInput := &dydb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	_, err = dydbClient.PutItem(putItemInput)
	if err != nil {
		t.Fatalf("Got error calling PutItem: %s", err)
	}
	log.Println("Successfully created table and put item for dynamodb")
}

func TestIntegration_DYNAMODB_Command(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := utils.GetDatabaseName(constants.DYNAMODB, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName+".")

	args := fmt.Sprintf(`schema-and-data -source=%s -prefix=%s -target-profile="instance=%s,dbName=%s"`, constants.DYNAMODB, filePrefix, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
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

	checkRow(ctx, t, client)
}

func checkRow(ctx context.Context, t *testing.T, client *spanner.Client) {
	wantRecord := SpannerRecord{
		AttrString:    "abcd",
		AttrInt:       int64(10),
		AttrFloat:     float64(14.5),
		AttrBool:      true,
		AttrBytes:     []byte{48, 49},
		AttrNumberSet: []float64{1.5, 2.5, 3.5},
		AttrByteSet:   [][]byte{[]byte{48, 49}, []byte{50, 51}},
		AttrStringSet: []string{"abc", "xyz"},
		AttrList:      "[\"str-1\",\"12.34\",true]",
		AttrMap:       "{\"key\":\"100\"}",
	}
	gotRecord := SpannerRecord{}
	stmt := spanner.Statement{SQL: `SELECT AttrString, AttrInt, AttrFloat, AttrBool, AttrBytes, AttrNumberSet, AttrByteSet, AttrStringSet, AttrList, AttrMap FROM table_test`}
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
		var AttrInt, AttrFloat big.Rat
		var AttrNumberSet []big.Rat
		if err := row.Columns(&gotRecord.AttrString, &AttrInt, &AttrFloat, &gotRecord.AttrBool, &gotRecord.AttrBytes, &AttrNumberSet, &gotRecord.AttrByteSet, &gotRecord.AttrStringSet, &gotRecord.AttrList, &gotRecord.AttrMap); err != nil {
			log.Println("Error reading into variables: ", err)
			t.Fatal(err)
			break
		}
		gotRecord.AttrFloat, _ = AttrFloat.Float64()
		floatVal, _ := AttrInt.Float64()
		gotRecord.AttrInt = int64(floatVal)
		floatSet := []float64{}
		for _, numericVal := range AttrNumberSet {
			floatVal, _ = numericVal.Float64()
			floatSet = append(floatSet, floatVal)
		}
		gotRecord.AttrNumberSet = floatSet
	}
	assert.True(t, cmp.Equal(wantRecord, gotRecord))
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
