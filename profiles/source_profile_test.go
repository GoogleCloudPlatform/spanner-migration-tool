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

package profiles

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSourceProfileFile(t *testing.T) {
	testCases := []struct {
		name         string
		params       map[string]string
		pipedToStdin bool
		want         SourceProfileFile
	}{
		{
			name:         "no params, file piped",
			params:       map[string]string{},
			pipedToStdin: true,
			want:         SourceProfileFile{Format: "dump"},
		},
		{
			name:         "format param, file piped",
			params:       map[string]string{"Format": "dump"},
			pipedToStdin: true,
			want:         SourceProfileFile{Format: "dump"},
		},
		{
			name:         "format and path param, file piped -- piped file takes precedence",
			params:       map[string]string{"format": "dump", "file": "file1.mysqldump"},
			pipedToStdin: true,
			want:         SourceProfileFile{Format: "dump"},
		},
		{
			name:         "format and path param, no file piped",
			params:       map[string]string{"format": "dump", "file": "file1.mysqldump"},
			pipedToStdin: false,
			want:         SourceProfileFile{Format: "dump", Path: "file1.mysqldump"},
		},
		{
			name:         "only path param, no file piped -- default dump format",
			params:       map[string]string{"file": "file1.mysqldump"},
			pipedToStdin: false,
			want:         SourceProfileFile{Format: "dump", Path: "file1.mysqldump"},
		},
	}

	for _, tc := range testCases {
		// Override filePipedToStdin with the test value.
		filePipedToStdin = func() bool { return tc.pipedToStdin }

		profile := NewSourceProfileFile(tc.params)
		assert.Equal(t, profile, tc.want, tc.name)
	}
}

func TestNewSourceProfileConfigFile(t *testing.T) {
	
	testCases := []struct {
		name          string
		source        string
		path          string
		errorExpected bool
	}{
		{
			name:          "bulk config for mysql",
			source:        "mysql",
			path:          filepath.Join("..", "test_data", "mysql_shard_bulk.cfg"),
			errorExpected: false,
		},
		{
			name:          "streaming config for mysql",
			source:        "mysql",
			path:          filepath.Join("..", "test_data", "mysql_shard_streaming.cfg"),
			errorExpected: false,
		},
		{
			name:          "config for non-mysql",
			source:        "postgres",
			path:          "",
			errorExpected: true,
		},
	}
	for _, tc := range testCases {
		_, err := NewSourceProfileConfig(tc.source, tc.path)
		assert.Equal(t, tc.errorExpected, err != nil)
	}
}

func TestNewSourceProfileConnectionSQL(t *testing.T) {
	// Avoid getting/settinng env variables in the unit tests.
	testCases := []struct {
		name          string
		params        map[string]string
		errorExpected bool
	}{
		{
			name:          "mandatory params provided",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "password": "e"},
			errorExpected: false,
		},
		{
			name:          "partial mandatory params provided",
			params:        map[string]string{"user": "b", "dbName": "c"},
			errorExpected: true,
		},
		{
			name:          "no mandatory params but optional provided",
			params:        map[string]string{"port": "b"},
			errorExpected: true,
		},
		{
			name:          "partial mandatory params and optional provided",
			params:        map[string]string{"host": "a", "port": "b"},
			errorExpected: true,
		},
		{
			name:          "all params provided",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "port": "d", "password": "e"},
			errorExpected: false,
		},
		{
			name:          "empty mandatory param",
			params:        map[string]string{"host": "", "user": "b", "dbName": "c"},
			errorExpected: true,
		},
		{
			name:          "empty port",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "password": "e"},
			errorExpected: false,
		},
	}

	for _, tc := range testCases {
		_, pgErr := NewSourceProfileConnectionPostgreSQL(tc.params)
		_, mysqlErr := NewSourceProfileConnectionMySQL(tc.params)
		assert.Equal(t, tc.errorExpected, pgErr != nil)
		assert.Equal(t, tc.errorExpected, mysqlErr != nil)
	}
}

func TestNewSourceProfileConnectionDynamoDB(t *testing.T) {
	// Avoid getting/settinng env variables in the unit tests.
	testCases := []struct {
		name          string
		params        map[string]string
		errorExpected bool
	}{
		{
			name:          "no params",
			params:        map[string]string{},
			errorExpected: false,
		},
		{
			name:          "valid schema sample size",
			params:        map[string]string{"schema-sample-size": "15"},
			errorExpected: false,
		},
		{
			name:          "invalid schema sample size",
			params:        map[string]string{"schema-sample-size": "a"},
			errorExpected: true,
		},
	}

	for _, tc := range testCases {
		_, err := NewSourceProfileConnectionDynamoDB(tc.params)
		assert.Equal(t, tc.errorExpected, err != nil)
	}
}
