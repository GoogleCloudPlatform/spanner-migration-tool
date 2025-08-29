// Copyright 2025 Google LLC
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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSessionFileName(t *testing.T) {
	testCases := []struct {
		name            string
		sessionFileName string
		filePrefix      string
		expected        string
	}{
		{
			name:            "Empty session file name",
			sessionFileName: "",
			filePrefix:      "my-prefix",
			expected:        "my-prefix.session.json",
		},
		{
			name:            "Session file name with .json suffix",
			sessionFileName: "my-session.json",
			filePrefix:      "my-prefix",
			expected:        "my-session.json",
		},
		{
			name:            "Session file name with a different extension",
			sessionFileName: "my-session.txt",
			filePrefix:      "my-prefix",
			expected:        "my-session.json",
		},
		{
			name:            "Session file name with no extension",
			sessionFileName: "my-session",
			filePrefix:      "my-prefix",
			expected:        "my-session.json",
		},
		{
			name:            "Session file name with multiple dots",
			sessionFileName: "my.special.session.json",
			filePrefix:      "my-prefix",
			expected:        "my.special.session.json",
		},
		{
			name:            "Session file name with multiple dots and different extension",
			sessionFileName: "my.special.session.dat",
			filePrefix:      "my-prefix",
			expected:        "my.special.session.json",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := GetSessionFileName(tc.sessionFileName, tc.filePrefix)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

// func TestMetricsPopulation(t *testing.T) {
// 	// Test cases for the metricsPopulation function.
// 	// This function modifies the context by adding migration metadata.
// 	// We test two primary scenarios:
// 	// 1. When metrics population is enabled (default).
// 	// 2. When metrics population is explicitly skipped.
// 	testCases := []struct {
// 		name                string
// 		driver              string
// 		skipMetrics         bool
// 		expectMetadata      bool
// 		expectedMigrationId string
// 	}{
// 		{
// 			name:                "Metrics population enabled",
// 			driver:              "mysql",
// 			skipMetrics:         false,
// 			expectMetadata:      true,
// 			expectedMigrationId: "spanner-migration-tool:mysql:schema-conv:go",
// 		},
// 		{
// 			name:           "Metrics population skipped",
// 			driver:         "postgres",
// 			skipMetrics:    true,
// 			expectMetadata: false,
// 		},
// 	}

// 	for _, tc := range testCases {
// 		t.Run(tc.name, func(t *testing.T) {
// 			conv := &internal.Conv{
// 				Audit: internal.Audit{
// 					SkipMetricsPopulation: tc.skipMetrics,
// 				},
// 			}
// 			ctx := context.Background()

// 			// Call the function to populate metrics.
// 			metricsPopulation(ctx, tc.driver, conv)

// 			// Retrieve outgoing metadata from the context.
// 			md, ok := metadata.FromOutgoingContext(ctx)

// 			if !tc.expectMetadata {
// 				assert.False(t, ok, "Expected no outgoing metadata to be set")
// 				return
// 			}

// 			// If metadata is expected, perform detailed checks.
// 			assert.True(t, ok, "Expected outgoing metadata to be set")
// 			values := md.Get(constants.MigrationMetadataKey)
// 			assert.Len(t, values, 1, "Expected exactly one metadata value")

// 			// Decode the metadata and verify its content.
// 			decodedBytes, err := base64.StdEncoding.DecodeString(values[0])
// 			assert.NoError(t, err, "Failed to decode metadata value")

// 			var migrationData internal.MigrationData
// 			err = proto.Unmarshal(decodedBytes, &migrationData)
// 			assert.NoError(t, err, "Failed to unmarshal migration data protobuf")

// 			assert.Equal(t, tc.expectedMigrationId, migrationData.MigrationId)
// 		})
// 	}
// }
