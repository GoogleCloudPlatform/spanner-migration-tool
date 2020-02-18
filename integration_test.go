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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"google.golang.org/api/iterator"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	testProjectID  string
	testInstanceID string

	now = time.Now()

	instanceAdmin *instance.InstanceAdminClient
	databaseAdmin *database.DatabaseAdminClient
)

func TestMain(m *testing.M) {
	_ = initIntegrationTests()
	res := m.Run()
	// cleanup()
	os.Exit(res)
}

func initIntegrationTests() (cleanup func()) {
	testProjectID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID")
	testInstanceID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID")

	ctx := context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}

	if testProjectID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if testInstanceID == "" {
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

func prepareIntegrationTest(t *testing.T) {
	if databaseAdmin == nil {
		t.Skip("Integration tests skipped")
	}
}

func TestIntegration_SimpleUse(t *testing.T) {
	prepareIntegrationTest(t)

	dbName, _ := getDatabaseName(now)
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProjectID, testInstanceID, dbName)

	// Run the command.
	f, err := os.Open("test_data/pg_dump.cart.test.out")
	if err != nil {
		t.Fatalf("failed to open the test data file: %v", err)
	}
	err = process(testProjectID, testInstanceID, dbName, &ioStreams{f, os.Stdout}, "")
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbPath)

	// Make a query to check results.
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var quantities []int64

	iter := client.Single().Read(ctx, "cart", spanner.AllKeys(), []string{"quantity"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var quantity int64
		if err := row.Columns(&quantity); err != nil {
			t.Fatal(err)
		}
		quantities = append(quantities, quantity)
	}

	// Sort a slice of int64 because sort.Ints() does not work for int64.
	sort.Slice(quantities, func(i, j int) bool { return quantities[i] < quantities[j] })

	if got, want := quantities, []int64{1, 2, 106, 125}; !reflect.DeepEqual(got, want) {
		t.Fatalf("quantities are not correct: got %v, want %v", got, want)
	}
}
