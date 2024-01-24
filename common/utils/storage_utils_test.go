// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package utils

import (
	"net/url"
	"os"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestMain(m *testing.M) {
	res := m.Run()
	os.Exit(res)
}

func TestParseGCSFilePath(t *testing.T) {
	testCases := []struct {
		name        string
		filePath    string
		expectError bool
		want        *url.URL
	}{
		{
			name:        "Basic",
			filePath:    "gs://test-bucket/path/to/folder/",
			expectError: false,
			want: &url.URL{
				Scheme: "gs",
				Host:   "test-bucket",
				Path:   "/path/to/folder/",
			},
		},
		{
			name:        "Append Slash",
			filePath:    "gs://test-bucket/path/to/folder",
			expectError: false,
			want: &url.URL{
				Scheme: "gs",
				Host:   "test-bucket",
				Path:   "/path/to/folder/",
			},
		},
		{
			name:        "Empty File path",
			filePath:    "",
			expectError: true,
			want:        nil,
		},
		{
			name:        "Wrong Scheme",
			filePath:    "ab://testpath",
			expectError: true,
			want:        nil,
		},
		{
			name:        "Malformed Path",
			filePath:    "://path",
			expectError: true,
			want:        nil,
		},
	}

	for _, tc := range testCases {
		got, err := ParseGCSFilePath(tc.filePath)
		assert.Equal(t, tc.expectError, err != nil)
		assert.Equal(t, tc.want, got)
	}
}
