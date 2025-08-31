package utils

import (
	"context"
	"testing"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func TestNewSpannerClient(t *testing.T) {
	ctx := context.Background()
	db := "projects/p/instances/i/databases/d"

	tests := []struct {
		name                 string
		spannerApiEndpoint   string
		gcloudAuthPlugin     string
		gcloudAuthToken      string
		expectedOptionsCount int
	}{
		{
			name:                 "No env vars set",
			expectedOptionsCount: 0,
		},
		{
			name:                 "Only SPANNER_API_ENDPOINT set",
			spannerApiEndpoint:   "localhost:9010",
			expectedOptionsCount: 1,
		},
		{
			name:                 "Only GCLOUD_AUTH_PLUGIN set to true",
			gcloudAuthPlugin:     "true",
			gcloudAuthToken:      "test-token",
			expectedOptionsCount: 1,
		},
		{
			name:                 "Both SPANNER_API_ENDPOINT and GCLOUD_AUTH_PLUGIN set",
			spannerApiEndpoint:   "localhost:9010",
			gcloudAuthPlugin:     "true",
			gcloudAuthToken:      "test-token",
			expectedOptionsCount: 2,
		},
		{
			name:                 "GCLOUD_AUTH_PLUGIN set to false",
			gcloudAuthPlugin:     "false",
			expectedOptionsCount: 0,
		},
		{
			name:                 "SPANNER_API_ENDPOINT set and GCLOUD_AUTH_PLUGIN is false",
			spannerApiEndpoint:   "localhost:9010",
			gcloudAuthPlugin:     "false",
			expectedOptionsCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldFunc := newClient
			defer func() { newClient = oldFunc }()
			if tt.spannerApiEndpoint != "" {
				t.Setenv("SPANNER_API_ENDPOINT", tt.spannerApiEndpoint)
			}
			if tt.gcloudAuthPlugin != "" {
				t.Setenv("GCLOUD_AUTH_PLUGIN", tt.gcloudAuthPlugin)
			}
			if tt.gcloudAuthToken != "" {
				t.Setenv("GCLOUD_AUTH_ACCESS_TOKEN", tt.gcloudAuthToken)
			}

			newClient = func(ctx context.Context, database string, opts ...option.ClientOption) (*sp.Client, error) {
				assert.Equal(t, db, database)
				assert.Len(t, opts, tt.expectedOptionsCount)
				return nil, nil
			}

			_, err := NewSpannerClient(ctx, db)
			assert.NoError(t, err)
		})
	}
}

func TestNewDatabaseAdminClient(t *testing.T) {
	ctx := context.Background()

	origNewDatabaseAdminClientFunc := newDatabaseAdminClient
	defer func() { newDatabaseAdminClient = origNewDatabaseAdminClientFunc }()

	tests := []struct {
		name                 string
		spannerApiEndpoint   string
		gcloudAuthPlugin     string
		gcloudAuthToken      string
		expectedOptionsCount int
	}{
		{
			name:                 "No env vars set",
			expectedOptionsCount: 0,
		},
		{
			name:                 "Only SPANNER_API_ENDPOINT set",
			spannerApiEndpoint:   "localhost:9010",
			expectedOptionsCount: 1,
		},
		{
			name:                 "Only GCLOUD_AUTH_PLUGIN set to true",
			gcloudAuthPlugin:     "true",
			gcloudAuthToken:      "test-token",
			expectedOptionsCount: 1,
		},
		{
			name:                 "Both SPANNER_API_ENDPOINT and GCLOUD_AUTH_PLUGIN set",
			spannerApiEndpoint:   "localhost:9010",
			gcloudAuthPlugin:     "true",
			gcloudAuthToken:      "test-token",
			expectedOptionsCount: 2,
		},
		{
			name:                 "GCLOUD_AUTH_PLUGIN set to false",
			gcloudAuthPlugin:     "false",
			expectedOptionsCount: 0,
		},
		{
			name:                 "SPANNER_API_ENDPOINT set and GCLOUD_AUTH_PLUGIN is false",
			spannerApiEndpoint:   "localhost:9010",
			gcloudAuthPlugin:     "false",
			expectedOptionsCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.spannerApiEndpoint != "" {
				t.Setenv("SPANNER_API_ENDPOINT", tt.spannerApiEndpoint)
			}
			if tt.gcloudAuthPlugin != "" {
				t.Setenv("GCLOUD_AUTH_PLUGIN", tt.gcloudAuthPlugin)
			}
			if tt.gcloudAuthToken != "" {
				t.Setenv("GCLOUD_AUTH_ACCESS_TOKEN", tt.gcloudAuthToken)
			}

			newDatabaseAdminClient = func(ctx context.Context, opts ...option.ClientOption) (*database.DatabaseAdminClient, error) {
				assert.Len(t, opts, tt.expectedOptionsCount)
				return nil, nil
			}

			_, err := NewDatabaseAdminClient(ctx)
			assert.NoError(t, err)
		})
	}
}
