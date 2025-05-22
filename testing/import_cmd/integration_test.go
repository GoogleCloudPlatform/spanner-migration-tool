package import_cmd

import (
	"cloud.google.com/go/spanner"
	"context"
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"slices"
	"strings"
	"testing"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/testing/common"
)

var expectedMysqlDumpDDL = "CREATE INDEX fk_author_id_11 ON Books(author_id)|CREATE INDEX fk_book_authors_author_id_5 ON BookAuthors(author_id)|CREATE INDEX fk_customer_id_21 ON Orders(customer_id)|CREATE INDEX fk_genre_id_13 ON Books(genre_id)|CREATE INDEX fk_publisher_id_12 ON Books(publisher_id)|CREATE INDEX fk_reviews_customer_id_29 ON Reviews(customer_id)|CREATE TABLE Authors (  author_id INT64 NOT NULL,  first_name STRING(50) NOT NULL,  last_name STRING(50) NOT NULL,  biography STRING(MAX),) PRIMARY KEY(author_id)|CREATE TABLE BookAuthors (  book_author_id INT64 NOT NULL,  book_id INT64 NOT NULL,  author_id INT64 NOT NULL,) PRIMARY KEY(book_author_id)|CREATE TABLE Books (  book_id INT64 NOT NULL,  title STRING(255) NOT NULL,  isbn STRING(20) NOT NULL,  publication_date DATE,  price NUMERIC NOT NULL,  stock_quantity INT64,  author_id INT64,  publisher_id INT64,  genre_id INT64,  is_featured BOOL,  is_available BOOL,  edition_number BOOL,  page_count INT64,  cover_image_binary BYTES(MAX),  cover_image_varbinary BYTES(MAX),  abstract_blob BYTES(MAX),  sample_chapter_mediumblob BYTES(MAX),  notes_tinyblob BYTES(MAX),  full_text_longblob BYTES(MAX),  flags BYTES(MAX),  series_code STRING(10),  volume_code STRING(5),  last_updated TIMESTAMP,  discount_rate NUMERIC,  special_price NUMERIC,  average_rating FLOAT64,  binding_type STRING(MAX),  weight_grams FLOAT32,  internal_id INT64,  catalog_id INT64,  shelf_number INT64,  inventory_code INT64,  metadata JSON,  keywords STRING(MAX),  description_text STRING(MAX),  long_description_mediumtext STRING(MAX),  short_summary_tinytext STRING(MAX),  full_review_longtext STRING(MAX),  last_modified TIMESTAMP,  product_code STRING(50),  ean_code STRING(13),) PRIMARY KEY(book_id)|CREATE TABLE Customers (  customer_id INT64 NOT NULL,  first_name STRING(50) NOT NULL,  last_name STRING(50) NOT NULL,  email STRING(100) NOT NULL,  phone_number STRING(20),  address STRING(MAX),  registration_date TIMESTAMP,) PRIMARY KEY(customer_id)|CREATE TABLE Genres (  genre_id INT64 NOT NULL,  name STRING(50) NOT NULL,) PRIMARY KEY(genre_id)|CREATE TABLE OrderItems (  order_item_id INT64 NOT NULL,  order_id INT64 NOT NULL,  book_id INT64 NOT NULL,  quantity INT64 NOT NULL,  price_at_purchase NUMERIC NOT NULL,) PRIMARY KEY(order_item_id)|CREATE TABLE Orders (  order_id INT64 NOT NULL,  customer_id INT64 NOT NULL,  order_date TIMESTAMP,  shipping_address STRING(MAX) NOT NULL,  order_status STRING(MAX),  total_amount NUMERIC NOT NULL,) PRIMARY KEY(order_id)|CREATE TABLE Publishers (  publisher_id INT64 NOT NULL,  name STRING(100) NOT NULL,  address STRING(MAX),) PRIMARY KEY(publisher_id)|CREATE TABLE Reviews (  review_id INT64 NOT NULL,  book_id INT64 NOT NULL,  customer_id INT64 NOT NULL,  rating INT64,  comment STRING(MAX),  review_date TIMESTAMP,  CONSTRAINT reviews_chk_1 CHECK((`rating`>=1) AND (`rating`<=5)),) PRIMARY KEY(review_id)|CREATE UNIQUE INDEX book_id ON BookAuthors(book_id, author_id)|CREATE UNIQUE INDEX book_id_28 ON Reviews(book_id, customer_id)|CREATE UNIQUE INDEX email ON Customers(email)|CREATE UNIQUE INDEX isbn ON Books(isbn)|CREATE UNIQUE INDEX name ON Genres(name)|CREATE UNIQUE INDEX name_23 ON Publishers(name)"
var expectedMysqlDumpCustomerRow = "string_value:\"1\"string_value:\"Christopher\"string_value:\"Miller\"string_value:\"christopher.miller0@example.com\"string_value:\"123-456-4205\"string_value:\"57 Main St, Anytown, CA 18404\"string_value:\"2025-04-08T08:40:54Z\""

var (
	projectID     string
	instanceID    string
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
	flag.Parse() // Needed for calling testing.Short().

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

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}

func TestMysqlExampleImportDumpFile(t *testing.T) {
	onlyRunForEmulatorTest(t)
	tests := []struct {
		name    string
		dumpUri string
		dbName  string
		wantErr bool
	}{
		{
			name:    "sakila dump file",
			dumpUri: "gs://smt-integration-test/import/mysql/sakila-dump.sql",
			dbName:  "sakila",
			wantErr: false,
		},
		{
			name:    "world dump file",
			dumpUri: "../../test_data/world.sql",
			dbName:  "world_mysql_example",
			wantErr: false,
		},
		{
			name:    "world dump file",
			dumpUri: "../../test_data/menagerie.sql",
			dbName:  "menagerie",
			wantErr: false,
		},
		{
			name:    "airport mysql example",
			dumpUri: "gs://smt-integration-test/import/mysql/airport-db.sql",
			dbName:  "airport_mysql_example",
			wantErr: false,
		},
		{
			name:    "employees mysql example",
			dumpUri: "gs://smt-integration-test/import/mysql/employees.sql",
			dbName:  "employees_mysql_example",
			wantErr: false,
		},
		// TODO: ADD Mysql Example Airport DB Tests.
		// TODO: Move test data to GCS
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, tt.dbName)
			log.Printf("Spanner database used for testing: %s", dbURI)

			createSpannerDatabase(t, projectID, instanceID, tt.dbName)
			defer databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbURI})

			dumpFilePath := tt.dumpUri

			args := fmt.Sprintf("import -source-format=mysqldump -project=%s -instance-id=%s -database-name=%s -source-uri=%s",
				projectID, instanceID, tt.dbName, dumpFilePath)
			err := common.RunCommand(args, projectID)
			assert.NoError(t, err)

			// TODO validation to be added.
		})
	}
}

func TestLocalImportMysqlDumpFile(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	log.Printf("projectID %s, instanceID %s", projectID, instanceID)

	// configure the database client
	dbName := "import_test_local_mysql_dump"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	log.Printf("Spanner database URI used for testing: %s", dbURI)

	createSpannerDatabase(t, projectID, instanceID, dbName)
	defer databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbURI})

	dumpFilePath := "../../test_data/mysql_dump_import_data.sql"

	args := fmt.Sprintf("import -source-format=mysqldump -project=%s -instance-id=%s -database-name=%s -source-uri=%s", projectID, instanceID, dbName, dumpFilePath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, fetchSpannerDDL(t, dbURI), expectedMysqlDumpDDL)

	assert.Equal(t, fetchRow(t, dbURI, "Customers", "customer_id", 1), expectedMysqlDumpCustomerRow)
}

func fetchSpannerDDL(t *testing.T, dbURI string) string {
	ddlResponse, err := databaseAdmin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: dbURI})
	if err != nil {
		t.Fatal(err)
	}

	ddlStmts := ddlResponse.GetStatements()
	slices.Sort(ddlStmts)

	return strings.Replace(strings.Join(ddlStmts, "|"), "\n", "", -1)
}

func fetchRow(t *testing.T, dbURI, table, primaryKey string, id int64) string {

	spannerClient, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		t.Fatal(err)
	}
	defer spannerClient.Close()

	stmt := spanner.Statement{
		SQL: fmt.Sprintf("select * from %s where %s = @id", table, primaryKey),
		Params: map[string]interface{}{
			"id": id,
		},
	}

	rows := spannerClient.ReadOnlyTransaction().Query(ctx, stmt)

	row, err := rows.Next()
	if err != nil {
		t.Fatal(err)
	}

	defer rows.Stop()

	var rowStr strings.Builder

	for i := 0; i < row.Size(); i++ {
		rowStr.WriteString(row.ColumnValue(i).String())
	}

	return rowStr.String()
}

func createSpannerDatabase(t *testing.T, project, instance, dbName string) {
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)
	req := &databasepb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	}

	req.CreateStatement = fmt.Sprintf("CREATE DATABASE `%s`", dbName)
	op, err := databaseAdmin.CreateDatabase(ctx, req)
	if err != nil {
		t.Fatalf("can't build CreateDatabaseRequest for %s: %v", dbURI, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("createDatabase call failed for %s: %v", dbURI, err)
	}
}
