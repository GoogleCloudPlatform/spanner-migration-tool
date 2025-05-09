/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package cmd

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/import_file"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/google/subcommands"
	"github.com/stretchr/testify/assert"
)

const expectedDDL = "CREATE TABLE cart ( \tuser_id STRING(20) NOT NULL , \tproduct_id STRING(20) NOT NULL , \tquantity INT64, \tlast_modified TIMESTAMP NOT NULL , ) PRIMARY KEY (user_id, product_id);CREATE INDEX idx ON cart (quantity)"

func TestBasicCsvImport(t *testing.T) {
	importDataCmd := ImportDataCmd{}

	fs := flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
	importDataCmd.SetFlags(fs)

	importDataCmd.project = "test-project"
	importDataCmd.instanceId = "test-instance"
	importDataCmd.databaseName = "versionone"
	importDataCmd.tableName = "table2"
	importDataCmd.sourceUri = "../test_data/basic_csv.csv"
	importDataCmd.sourceFormat = "csv"
	importDataCmd.schemaUri = "../test_data/basic_csv_schema.csv"
	importDataCmd.csvLineDelimiter = "\n"
	importDataCmd.csvFieldDelimiter = ","
	importDataCmd.Execute(context.Background(), fs)
}

func TestImportDataCmd_SetFlags(t *testing.T) {
	cmd := &ImportDataCmd{}
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	cmd.SetFlags(fs)

	assert.NotNil(t, fs.Lookup("instance-id"))
	assert.NotNil(t, fs.Lookup("database-name"))
	assert.NotNil(t, fs.Lookup("table-name"))
	assert.NotNil(t, fs.Lookup("source-uri"))
	assert.NotNil(t, fs.Lookup("source-format"))
	assert.NotNil(t, fs.Lookup("schema-uri"))
	assert.NotNil(t, fs.Lookup("csv-line-delimiter"))
	assert.NotNil(t, fs.Lookup("csv-field-delimiter"))
	assert.NotNil(t, fs.Lookup("project"))
}

func TestValidateInputLocal_MissingInstanceID(t *testing.T) {
	input := &ImportDataCmd{}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify instanceId")
}

func TestValidateInputLocal_MissingDatabaseName(t *testing.T) {
	input := &ImportDataCmd{instanceId: "test-instance"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify databaseName")
}

func TestValidateInputLocal_MissingSourceURI(t *testing.T) {
	input := &ImportDataCmd{instanceId: "test-instance", databaseName: "test-db"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify sourceUri")
}

func TestValidateInputLocal_MissingSourceFormat(t *testing.T) {
	input := &ImportDataCmd{instanceId: "test-instance", databaseName: "test-db", sourceUri: "file:///tmp/data.csv"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify sourceFormat")
}

func TestValidateInputLocal_CSVMissingSchemaURI(t *testing.T) {
	input := &ImportDataCmd{instanceId: "test-instance", databaseName: "test-db", sourceUri: "file:///tmp/data.csv", sourceFormat: constants.CSV}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify schemaUri")
}

func TestValidateInputLocal_SuccessCSV(t *testing.T) {
	input := &ImportDataCmd{
		instanceId:   "test-instance",
		databaseName: "test-db",
		sourceUri:    "file:///tmp/data.csv",
		sourceFormat: constants.CSV,
		schemaUri:    "file:///tmp/schema.csv",
	}
	err := validateInputLocal(input)
	assert.NoError(t, err)
}

func TestValidateInputLocal_SuccessNonCSV(t *testing.T) {
	input := &ImportDataCmd{
		instanceId:   "test-instance",
		databaseName: "test-db",
		sourceUri:    "gs://bucket/data.avro",
		sourceFormat: "avro",
	}
	err := validateInputLocal(input)
	assert.NoError(t, err)
}

func TestHandleTableNameDefaults_TableNamePresent(t *testing.T) {
	tableName := "explicit_table"
	sourceUri := "gs://bucket/data.csv"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "explicit_table", result)
}

func TestHandleTableNameDefaults_TableNameEmptyFileScheme(t *testing.T) {
	tableName := ""
	sourceUri := "file:///path/to/my_data.csv"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "my_data.csv", result)
}

func TestHandleTableNameDefaults_TableNameEmptyGCScheme(t *testing.T) {
	tableName := ""
	sourceUri := "gs://my-bucket/data_file.txt"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "data_file.txt", result)
}

func TestHandleTableNameDefaults_TableNameEmptyLocalPathNoScheme(t *testing.T) {
	tableName := ""
	sourceUri := "/tmp/another_file.json"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "another_file.json", result)
}

func TestHandleTableNameDefaults_TableNameEmptyRelativePath(t *testing.T) {
	tableName := ""
	sourceUri := "relative/path/some_data.avro"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "some_data.avro", result)
}

func TestHandleTableNameDefaults_TableNameEmptyURIWithTrailingSlash(t *testing.T) {
	tableName := ""
	sourceUri := "gs://my-bucket/folder/"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "folder", result)
}

func TestImportDataCmd_HandleDumpExecute(t *testing.T) {

	tests := []struct {
		name                string
		cmd                 *ImportDataCmd
		expectedStatus      subcommands.ExitStatus
		expectedError       error // Add expectedError
		spannerAccessorMock func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error)
	}{
		{
			name: "successful MySQL dump import",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instanceId:   "test-instance",
				databaseName: "test-db",
				sourceUri:    "../test_data/basic_mysql_dump.test.out",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitSuccess,
			expectedError:  nil,
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					UpdateDatabaseMock: func(ctx context.Context, dbURI string, conv *internal.Conv, driver string) error {
						return nil
					},
					RefreshMock: func(ctx context.Context, dbURI string) {
					},
					GetSpannerClientMock: func() spannerclient.SpannerClient {
						return &spannerclient.SpannerClientMock{
							ApplyMock: func(ctx context.Context, ms []*spanner.Mutation, opts ...spanner.ApplyOption) (commitTimestamp time.Time, err error) {
								return time.Now(), nil
							},
						}
					},
				}, nil
			},
		},
		{
			name: "Mysql Dump failed initialisation SpannerAccessor",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instanceId:   "test-instance",
				databaseName: "test-db",
				sourceUri:    "../test_data/basic_mysql_dump.test.out",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  nil,
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return nil, fmt.Errorf("failed to create or update database")
			},
		},
		{
			name: "MySQL dump invalid instance",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instanceId:   "",
				databaseName: "test-db",
				sourceUri:    "nonexistent_file.sql",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  nil,
		},
		{
			name: "failed MySQL dump import",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instanceId:   "test-instance",
				databaseName: "test-db",
				sourceUri:    "nonexistent_file.sql",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  nil,
		},
		{
			name: "unsupported format",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instanceId:   "test-instance",
				databaseName: "test-db",
				sourceUri:    "testdata/test.txt",
				sourceFormat: "unsupported",
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  nil, // The function handles the unsupported format internally and returns a failure status
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalNewSpannerAccessor := import_file.NewSpannerAccessor
			import_file.NewSpannerAccessor = tc.spannerAccessorMock
			defer func() {
				import_file.NewSpannerAccessor = originalNewSpannerAccessor
			}()

			// Create a new flag set and register the command's flags
			f := flag.NewFlagSet("import", flag.ContinueOnError)
			// Execute the command
			status := tc.cmd.Execute(context.Background(), f)

			// Check the exit status
			if status != tc.expectedStatus {
				t.Errorf("Unexpected exit status: got %v, want %v", status, tc.expectedStatus)
			}
		})
	}
}

func TestImportDataCmd_handleDump(t *testing.T) {
	tests := []struct {
		name                string
		sourceUri           string
		dialect             string
		spannerAccessorMock func(t *testing.T) spanneraccessor.SpannerAccessor
		wantErr             bool
	}{
		{
			name:      "Successful MySQL Dump Import",
			sourceUri: "../test_data/basic_mysql_dump.test.out",
			dialect:   constants.DIALECT_GOOGLESQL,
			spannerAccessorMock: func(t *testing.T) spanneraccessor.SpannerAccessor {
				mock := &spanneraccessor.SpannerAccessorMock{
					UpdateDatabaseMock: func(ctx context.Context, dbURI string, conv *internal.Conv, driver string) error {
						assert.Equal(t, "projects/test-project/instances/test-instance/databases/test-db", dbURI)
						assert.Equal(t, constants.MYSQLDUMP, driver)
						assert.Equal(t, expectedDDL, fetchDDLString(conv))

						return nil
					},
					GetSpannerClientMock: func() spannerclient.SpannerClient {
						return &spannerclient.SpannerClientMock{
							ApplyMock: func(ctx context.Context, ms []*spanner.Mutation, opts ...spanner.ApplyOption) (commitTimestamp time.Time, err error) {
								assert.Equal(t, 1, len(ms))
								mutationString := fmt.Sprintf("%v", *ms[0])
								assert.True(t, strings.Contains(mutationString, "cart"))
								assert.True(t, strings.Contains(mutationString, "901e-a6cfc2b502dc"))
								return time.Now(), nil
							},
						}
					},
					SetSpannerClientMock: func(spannerClient spannerclient.SpannerClient) {
					},
					RefreshMock: func(ctx context.Context, dbURI string) {
					},
				}
				return mock
			},
			wantErr: false,
		},
		{
			name:      "Failed CreateOrUpdateDatabase",
			sourceUri: "./testdata/mysqldump.sql",
			dialect:   constants.DIALECT_GOOGLESQL,
			spannerAccessorMock: func(t *testing.T) spanneraccessor.SpannerAccessor {
				mock := &spanneraccessor.SpannerAccessorMock{
					CreateOrUpdateDatabaseMock: func(ctx context.Context, dbURI, sourceFormat string, conv *internal.Conv, migrationType string) error {
						return fmt.Errorf("failed to create or update database")
					},
					GetSpannerClientMock: func() spannerclient.SpannerClient {
						return &spannerclient.SpannerClientMock{}
					},
				}
				return mock
			},
			wantErr: true,
		},
		{
			name:      "Failed Dump File Read",
			sourceUri: "./testdata/wrongfile.sql",
			dialect:   constants.DIALECT_GOOGLESQL,
			spannerAccessorMock: func(t *testing.T) spanneraccessor.SpannerAccessor {
				mock := &spanneraccessor.SpannerAccessorMock{
					CreateOrUpdateDatabaseMock: func(ctx context.Context, dbURI, sourceFormat string, conv *internal.Conv, migrationType string) error {
						return nil
					},
					GetSpannerClientMock: func() spannerclient.SpannerClient {
						return &spannerclient.SpannerClientMock{}
					},
				}
				return mock
			},
			wantErr: true,
		},
	}
	originalSpannerAccessorFunc := import_file.NewSpannerAccessor
	defer func() {
		import_file.NewSpannerAccessor = originalSpannerAccessorFunc
	}()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cmd := &ImportDataCmd{
				project:      "test-project",
				instanceId:   "test-instance",
				databaseName: "test-db",
				sourceUri:    tt.sourceUri,
				sourceFormat: constants.MYSQLDUMP,
			}
			import_file.NewSpannerAccessor = func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return tt.spannerAccessorMock(t), nil
			}

			err := cmd.handleDatabaseDumpFile(
				ctx,
				fmt.Sprintf("projects/%s/instances/%s/databases/%s", cmd.project, cmd.instanceId, cmd.databaseName),
				constants.MYSQLDUMP,
				tt.dialect)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func fetchDDLString(conv *internal.Conv) string {
	return strings.Replace(strings.Join(
		ddl.GetDDL(
			ddl.Config{Comments: false, ProtectIds: false, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: "mysql"},
			conv.SpSchema,
			conv.SpSequences), ";"), "\n", " ", -1)
}
