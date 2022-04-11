package shared

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
)

// func GetMetadataDbUri() string {
// 	config, err := config.GetConfigForSpanner()
// 	if err != nil || config.GCPProjectID == "" || config.SpannerInstanceID == "" {
// 		return ""
// 	}
// 	return GetSpannerUri(config.GCPProjectID, config.SpannerInstanceID)
// }

func GetSpannerUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/harbourbridge_metadata", projectId, instanceId)
}

func PingMetadataDb(uri string) bool {
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
	return err == nil
}
