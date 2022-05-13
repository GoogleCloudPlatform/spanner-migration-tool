package shared

import (
	"context"
	"fmt"
	"regexp"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const metadataDbName string = "harbourbridge_metadata"

func GetMetadataDbName() string {
	return metadataDbName
}

func GetSpannerUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, GetMetadataDbName())
}

func CheckOrCreateMetadataDb(projectId string, instanceId string) bool {
	uri := GetSpannerUri(projectId, instanceId)
	if uri == "" {
		fmt.Println("Invalid spanner uri")
		return false
	}

	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	defer adminClient.Close()

	dbExists, err := conversion.CheckExistingDb(ctx, adminClient, uri)
	if err != nil {
		fmt.Println(err)
		return false
	}
	if dbExists {
		return true
	}
	err = createDatabase(ctx, uri)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}

func createDatabase(ctx context.Context, uri string) error {

	// Spanner uri will be in this format 'projects/project-id/instances/spanner-instance-id/databases/db-name'
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(uri)
	spInstance := matches[1]
	dbName := matches[2]

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()
	fmt.Println("Creating database to store session metadata...")

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          spInstance,
		CreateStatement: "CREATE DATABASE `" + dbName + "`",
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
				CreateTimestamp TIMESTAMP NOT NULL,
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
