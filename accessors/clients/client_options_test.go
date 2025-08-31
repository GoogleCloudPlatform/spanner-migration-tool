package clients

import (
	"testing"
)

func TestFetchSpannerClientOptions(t *testing.T) {
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

			opts := FetchSpannerClientOptions()
			if len(opts) != tt.expectedOptionsCount {
				t.Errorf("FetchSpannerClientOptions() returned %d options, want %d", len(opts), tt.expectedOptionsCount)
			}
		})
	}
}

func TestFetchStorageClientOptions(t *testing.T) {
	tests := []struct {
		name                 string
		gcloudAuthPlugin     string
		gcloudAuthToken      string
		expectedOptionsCount int
	}{
		{
			name:                 "No auth env vars set",
			expectedOptionsCount: 0,
		},
		{
			name:                 "GCLOUD_AUTH_PLUGIN set to true",
			gcloudAuthPlugin:     "true",
			gcloudAuthToken:      "test-token",
			expectedOptionsCount: 1,
		},
		{
			name:                 "GCLOUD_AUTH_PLUGIN set to false",
			gcloudAuthPlugin:     "false",
			expectedOptionsCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.gcloudAuthPlugin != "" {
				t.Setenv("GCLOUD_AUTH_PLUGIN", tt.gcloudAuthPlugin)
			}
			if tt.gcloudAuthToken != "" {
				t.Setenv("GCLOUD_AUTH_ACCESS_TOKEN", tt.gcloudAuthToken)
			}

			opts := FetchStorageClientOptions()
			if len(opts) != tt.expectedOptionsCount {
				t.Errorf("FetchStorageClientOptions() returned %d options, want %d", len(opts), tt.expectedOptionsCount)
			}
		})
	}
}

func TestFetchAuthClientOptions(t *testing.T) {
	tests := []struct {
		name             string
		gcloudAuthPlugin string
		gcloudAuthToken  string
		expectNil        bool
	}{
		{
			name:      "GCLOUD_AUTH_PLUGIN not set",
			expectNil: true,
		},
		{
			name:             "GCLOUD_AUTH_PLUGIN is true",
			gcloudAuthPlugin: "true",
			gcloudAuthToken:  "test-token",
			expectNil:        false,
		},
		{
			name:             "GCLOUD_AUTH_PLUGIN is false",
			gcloudAuthPlugin: "false",
			expectNil:        true,
		},
		{
			name:             "GCLOUD_AUTH_PLUGIN is true but no token",
			gcloudAuthPlugin: "true",
			expectNil:        false, // The function still returns an option, even if the token is empty.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.gcloudAuthPlugin != "" {
				t.Setenv("GCLOUD_AUTH_PLUGIN", tt.gcloudAuthPlugin)
			}
			if tt.gcloudAuthToken != "" {
				t.Setenv("GCLOUD_AUTH_ACCESS_TOKEN", tt.gcloudAuthToken)
			}

			opt := fetchAuthClientOptions()

			if tt.expectNil && opt != nil {
				t.Errorf("fetchAuthClientOptions() returned a non-nil option, want nil")
			}
			if !tt.expectNil && opt == nil {
				t.Errorf("fetchAuthClientOptions() returned a nil option, want non-nil")
			}
		})
	}
}
