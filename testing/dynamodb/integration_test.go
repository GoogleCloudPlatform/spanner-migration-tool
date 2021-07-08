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

package dynamodb_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/cmd"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	dydb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectID  string
	instanceID string

	databaseAdmin *database.DatabaseAdminClient
)

// Create struct to hold info about new item in dynamodb
type Item struct {
    Year   int
    Title  string
    Plot   string
    Rating float64
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

	ctx := context.Background()
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

func dropDatabase(t *testing.T, dbPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// Drop the testing database.
	if err := databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbPath}); err != nil {
		t.Fatalf("failed to drop testing database %v: %v", dbPath, err)
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
	populateDynamoDB(t)
	return tmpdir
}

func populateDynamoDB(t *testing.T){
	cfg := aws.Config{
		Endpoint: aws.String("http://localhost:8000"),
	}
	sess := session.Must(session.NewSession())
	dydbClient := dydb.New(sess, &cfg)

    tableName := "Movies"
	createTableInput := &dydb.CreateTableInput{
		AttributeDefinitions: []*dydb.AttributeDefinition{
			{
				AttributeName: aws.String("Year"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("Title"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dydb.KeySchemaElement{
			{
				AttributeName: aws.String("Year"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("Title"),
				KeyType:       aws.String("RANGE"),
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

	item := Item{
        Year:   2015,
        Title:  "The Big New Movie",
        Plot:   "Nothing happens at all.",
        Rating: 0.0,
    }
	av, err := dynamodbattribute.MarshalMap(item)
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
}

func TestIntegration_DYNAMODB_SimpleUse(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(conversion.DYNAMODB, now)
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName+".")

	err := cmd.CommandLine(conversion.DYNAMODB, projectID, instanceID, dbName, false, false, 0, "", &conversion.IOStreams{Out: os.Stdout}, filePrefix, now)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbPath)

	checkResults(t, dbPath)
}

func checkResults(t *testing.T, dbPath string) {
	// Make a query to check results.
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkBigInt(ctx, t, client)
}

func checkBigInt(ctx context.Context, t *testing.T, client *spanner.Client) {
	var quantity int64
	iter := client.Single().Read(ctx, "cart", spanner.Key{"901e-a6cfc2b502dc", "abc-123"}, []string{"quantity"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&quantity); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := quantity, int64(1); got != want {
		t.Fatalf("quantities are not correct: got %v, want %v", got, want)
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
