package shared

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func GetMetadataDbName() string {
	return "harbourbridge_metadata"
}

func GetSpannerUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, GetMetadataDbName())
}

func PingMetadataDb(projectId string, instanceId string) bool {
	uri := GetSpannerUri(projectId, instanceId)
	if uri == "" {
		return false
	}

	ctx := context.Background()
	spClient, err := spanner.NewClient(ctx, uri)
	defer spClient.Close()
	if err != nil {
		return false
	}

	txn := spClient.ReadOnlyTransaction()
	defer txn.Close()

	query := spanner.Statement{
		SQL: "SELECT 1",
	}
	iter := txn.Query(ctx, query)
	_, err = iter.Next()

	if err == nil {
		return true
	}

	fmt.Println(err)
	errMsg := fmt.Sprint(err)
	//ToDo : Check error type instead of message
	if !strings.Contains(errMsg, "Database not found") {
		return false
	}

	err = createDatabase(ctx, uri)
	if err != nil {
		return false
	}
	return true
}

func createDatabase(ctx context.Context, uri string) error {

	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(uri)
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()
	fmt.Printf("Creating database to store session metadata...")

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
		ExtraStatements: []string{
			`CREATE TABLE SchemaConversionSession (
				VersionId STRING(36) NOT NULL,
				PreviousVersionId ARRAY<STRING(36)>,
				SessionName STRING(50) NOT NULL,
				EditorName STRING(100) NOT NULL,
				DatabaseType STRING(50) NOT NULL,
				DatabaseName STRING(50) NOT NULL,
				Notes ARRAY<STRING(MAX)> NOT NULL,
				Tags ARRAY<STRING(20)>,
				SchemaChanges STRING(MAX),
				SchemaConversionObject JSON NOT NULL,
				CreatedOn TIMESTAMP NOT NULL,
			  ) PRIMARY KEY(VersionId)`,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}

	fmt.Printf("Created database [%s]\n", matches[2])
	return nil
}
