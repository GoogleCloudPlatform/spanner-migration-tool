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
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaSetFlags(t *testing.T) {
	testCases := []struct {
		testName       string
		flagArgs       []string
		expectedValues SchemaCmd
	}{
		{
			testName: "Default Values",
			flagArgs: []string{},
			expectedValues: SchemaCmd{
				source:          "",
				sourceProfile:   "",
				target:          "Spanner",
				targetProfile:   "",
				filePrefix:      "",
				project:         "",
				logLevel:        "DEBUG",
				dryRun:          false,
				validate:        false,
				sessionJSON:     "",
				sessionFileName: "",
			},
		},
		{
			testName: "Source and Target",
			flagArgs: []string{"--source=PostgreSQL", "--target=Spanner"},
			expectedValues: SchemaCmd{
				source:          "PostgreSQL",
				sourceProfile:   "",
				target:          "Spanner",
				targetProfile:   "",
				filePrefix:      "",
				project:         "",
				logLevel:        "DEBUG",
				dryRun:          false,
				validate:        false,
				sessionJSON:     "",
				sessionFileName: "",
			},
		},
		{
			testName: "Source and Target Profiles",
			flagArgs: []string{"--source-profile=source.json", "--target-profile=target.json"},
			expectedValues: SchemaCmd{
				source:          "",
				sourceProfile:   "source.json",
				target:          "Spanner",
				targetProfile:   "target.json",
				filePrefix:      "",
				project:         "",
				logLevel:        "DEBUG",
				dryRun:          false,
				validate:        false,
				sessionJSON:     "",
				sessionFileName: "",
			},
		},
		{
			testName: "File Prefix, Project and Log Level",
			flagArgs: []string{"--prefix=test", "--project=gcp-project-id", "--log-level=INFO"},
			expectedValues: SchemaCmd{
				source:          "",
				sourceProfile:   "",
				target:          "Spanner",
				targetProfile:   "",
				filePrefix:      "test",
				project:         "gcp-project-id",
				logLevel:        "INFO",
				dryRun:          false,
				validate:        false,
				sessionJSON:     "",
				sessionFileName: "",
			},
		},
		{
			testName: "Dry Run and Validate",
			flagArgs: []string{"--dry-run", "--validate"},
			expectedValues: SchemaCmd{
				source:          "",
				sourceProfile:   "",
				target:          "Spanner",
				targetProfile:   "",
				filePrefix:      "",
				project:         "",
				logLevel:        "DEBUG",
				dryRun:          true,
				validate:        true,
				sessionJSON:     "",
				sessionFileName: "",
			},
		},
		{
			testName: "Session JSON and Session File Name",
			flagArgs: []string{"--session=test-session.json", "--session-file-name=my-session.json"},
			expectedValues: SchemaCmd{
				source:          "",
				sourceProfile:   "",
				target:          "Spanner",
				targetProfile:   "",
				filePrefix:      "",
				project:         "",
				logLevel:        "DEBUG",
				dryRun:          false,
				validate:        false,
				sessionJSON:     "test-session.json",
				sessionFileName: "my-session.json",
			},
		},
		{
			testName: "All Flags Combined",
			flagArgs: []string{
				"--source=MySQL",
				"--source-profile=mysql.json",
				"--target=Spanner",
				"--target-profile=spanner.json",
				"--prefix=output",
				"--project=my-gcp-project",
				"--log-level=WARN",
				"--dry-run",
				"--validate",
				"--session=restored-session.json",
				"--session-file-name=my-session.json",
			},
			expectedValues: SchemaCmd{
				source:          "MySQL",
				sourceProfile:   "mysql.json",
				target:          "Spanner",
				targetProfile:   "spanner.json",
				filePrefix:      "output",
				project:         "my-gcp-project",
				logLevel:        "WARN",
				dryRun:          true,
				validate:        true,
				sessionJSON:     "restored-session.json",
				sessionFileName: "my-session.json",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			fs := flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
			schemaCmd := SchemaCmd{}
			schemaCmd.SetFlags(fs)
			err := fs.Parse(tc.flagArgs)
			if err != nil {
				t.Fatalf("Failed to parse flags: %v", err)
			}
			assert.Equal(t, tc.expectedValues, schemaCmd, tc.testName)
		})
	}
}
