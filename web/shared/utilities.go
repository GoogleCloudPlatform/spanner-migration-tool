package shared

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
)

func GetSessionFilePath(dbName string) string {
	dirPath := "harbour_bridge_output"
	return fmt.Sprintf("%s/%s/%s.session.json", dirPath, dbName, dbName)
}

func GetMetadataDbUri() string {
	config, err := GetConfigForSpanner()
	if err != nil || config.GCPProjectID == "" || config.SpannerInstanceID == "" {
		return ""
	}
	return fmt.Sprintf("projects/%s/instances/%s/databases/harbourbridge_metadata", config.GCPProjectID, config.SpannerInstanceID)
}

func PingMetadataDb() bool {
	spUri := GetMetadataDbUri()
	if spUri == "" {
		return false
	}

	ctx := context.Background()
	spClient, err := spanner.NewClient(ctx, spUri)
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
	return err == nil
}
