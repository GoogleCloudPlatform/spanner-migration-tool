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

func TestSchemaAndDataCmd_SetFlags_DefaultValues(t *testing.T) {
        expectedValues:= SchemaAndDataCmd{
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
        }

        schemaAndDataCmd:= SchemaAndDataCmd{}
        fs:= flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
        schemaAndDataCmd.SetFlags(fs)
        assert.Equal(t, expectedValues, schemaAndDataCmd, "Default Values")
}

func TestSchemaAndDataCmd_SetFlags_NonDefaultValues(t *testing.T) {
        fs.String("source", "MySQL", "")
        fs.String("source-profile", "file=test.sql", "")
        fs.String("target", "Spanner", "")
        fs.String("target-profile", "instance=test-instance", "")
        fs.String("prefix", "test-prefix", "")
        fs.Int64("write-limit", 1000, "")
        fs.Bool("dry-run", true, "")
        fs.String("log-level", "INFO", "")
        fs.Bool("skip-foreign-keys", true, "")
        fs.Bool("validate", true, "")

        expectedValues:= SchemaAndDataCmd{
                source:           "MySQL",
                sourceProfile:    "file=test.sql",
                target:           "Spanner",
                targetProfile:    "instance=test-instance",
                filePrefix:       "test-prefix",
                WriteLimit:       1000,
                dryRun:           true,
                logLevel:         "INFO",
                SkipForeignKeys:  true,
                validate:         true,
                dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
        }

        schemaAndDataCmd:= SchemaAndDataCmd{}
        fs:= flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
        schemaAndDataCmd.SetFlags(fs)
        assert.Equal(t, expectedValues, schemaAndDataCmd, "Non-Default Values")
}

func TestSchemaAndDataCmd_SetFlags_InvalidLogLevel(t *testing.T) {
        fs.String("log-level", "INVALID", "")

        expectedValues:= SchemaAndDataCmd{
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
        }

        schemaAndDataCmd:= SchemaAndDataCmd{}
        fs:= flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
        schemaAndDataCmd.SetFlags(fs)
        assert.Equal(t, expectedValues, schemaAndDataCmd, "Invalid Log Level")
}

func TestSchemaAndDataCmd_SetFlags_EmptySourceProfile(t *testing.T) {
        fs.String("source", "MySQL", "")
        fs.String("source-profile", "", "")

        expectedValues:= SchemaAndDataCmd{
                source:           "MySQL",
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
        }

        schemaAndDataCmd:= SchemaAndDataCmd{}
        fs:= flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
        schemaAndDataCmd.SetFlags(fs)
        assert.Equal(t, expectedValues, schemaAndDataCmd, "Empty Source Profile")
}
