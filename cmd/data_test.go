// Copyright 2024 Google LLC
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

        "github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
        "github.com/stretchr/testify/assert"
)

func TestDataSetFlags(t *testing.T) {
        testCases := []struct {
                testName       string
                flagArgs      []string
                expectedValues DataCmd
        }{
                {
                        testName: "Default Values",
                        flagArgs: []string{},
                        expectedValues: DataCmd{
                                source:           "",
                                sourceProfile:    "",
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           false,
                                logLevel:         "DEBUG",
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "Source and Target",
                        flagArgs: []string{"--source=PostgreSQL", "--target=Spanner"},
                        expectedValues: DataCmd{
                                source:           "PostgreSQL",
                                sourceProfile:    "",
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           false,
                                logLevel:         "DEBUG",
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "Source and Target Profiles",
                        flagArgs: []string{"--source-profile=source.json", "--target-profile=target.json"},
                        expectedValues: DataCmd{
                                source:           "",
                                sourceProfile:    "source.json",
                                target:           "Spanner",
                                targetProfile:    "target.json",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           false,
                                logLevel:         "DEBUG",
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "File Prefix and Write Limit",
                        flagArgs: []string{"--file-prefix=test", "--write-limit=100"},
                        expectedValues: DataCmd{
                                source:           "",
                                sourceProfile:    "",
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "test",
                                WriteLimit:       100,
                                dryRun:           false,
                                logLevel:         "DEBUG",
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "Dry Run and Log Level",
                        flagArgs: []string{"--dry-run", "--log-level=INFO"},
                        expectedValues: DataCmd{
                                source:           "",
                                sourceProfile:    "",
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           true,
                                logLevel:         "INFO",
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "Skip Foreign Keys and Validate",
                        flagArgs: []string{"--skip-foreign-keys", "--validate"},
                        expectedValues: DataCmd{
                                source:           "",
                                sourceProfile:    "",
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           false,
                                logLevel:         "DEBUG",
                                SkipForeignKeys:  true,
                                validate:         true,
                                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
                        },
                },
                {
                        testName: "Custom Dataflow Template",
                        flagArgs: []string{"--dataflow-template=gs://my-bucket/my-template"},
                        expectedValues: DataCmd{
                                source:           "",
                                sourceProfile:    "",
                                target:           "Spanner",
                                targetProfile:    "",
                                filePrefix:       "",
                                WriteLimit:       DefaultWritersLimit,
                                dryRun:           false,
                                logLevel:         "DEBUG",
                                SkipForeignKeys:  false,
                                validate:         false,
                                dataflowTemplate: "gs://my-bucket/my-template",
                        },
                },
                {
                        testName: "All Flags Combined",
                        flagArgs: []string{
                                "--source=MySQL",
                                "--source-profile=mysql.json",
                                "--target=Spanner",
                                "--target-profile=spanner.json",
                                "--file-prefix=output",
                                "--write-limit=50",
                                "--dry-run",
                                "--log-level=WARN",
                                "--skip-foreign-keys",
                                "--validate",
                                "--dataflow-template=gs://custom/template",
                        },
                        expectedValues: DataCmd{
                                source:           "MySQL",
                                sourceProfile:    "mysql.json",
                                target:           "Spanner",
                                targetProfile:    "spanner.json",
                                filePrefix:       "output",
                                WriteLimit:       50,
                                dryRun:           true,
                                logLevel:         "WARN",
                                SkipForeignKeys:  true,
                                validate:         true,
                                dataflowTemplate: "gs://custom/template",
                        },
                },
        }

        for _, tc := range testCases {
                t.Run(tc.testName, func(t *testing.T) {
                        fs := flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
                        dataCmd := DataCmd{}
                        dataCmd.SetFlags(fs)
                        err := fs.Parse(tc.flagArgs)
                        assert.Equal(t, tc.expectedValues, dataCmd, tc.testName)
                })
        }
}
