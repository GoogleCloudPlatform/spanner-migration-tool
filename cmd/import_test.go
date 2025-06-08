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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/file_reader"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"

	"cloud.google.com/go/spanner"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/import_file"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	sourcesspanner "github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
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
	importDataCmd.instance = "test-instance"
	importDataCmd.database = "versionone"
	importDataCmd.tableName = "table2"
	importDataCmd.sourceUri = "../test_data/basic_csv.csv"
	importDataCmd.sourceFormat = "csv"
	importDataCmd.schemaUri = "../test_data/basic_csv_schema.json"
	importDataCmd.csvLineDelimiter = "\n"
	importDataCmd.csvFieldDelimiter = ","
	importDataCmd.Execute(context.Background(), fs)
}

func TestImportDataCmd_SetFlags(t *testing.T) {
	cmd := &ImportDataCmd{}
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	cmd.SetFlags(fs)

	assert.NotNil(t, fs.Lookup("instance"))
	assert.NotNil(t, fs.Lookup("database"))
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
	assert.Contains(t, err.Error(), "Please specify instance")
}

func TestValidateInputLocal_MissingDatabaseName(t *testing.T) {
	input := &ImportDataCmd{instance: "test-instance"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify database")
}

func TestValidateInputLocal_MissingSourceURI(t *testing.T) {
	input := &ImportDataCmd{instance: "test-instance", database: "test-db"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify sourceUri")
}

func TestValidateInputLocal_MissingSourceFormat(t *testing.T) {
	input := &ImportDataCmd{instance: "test-instance", database: "test-db", sourceUri: "file:///tmp/data.csv"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify sourceFormat")
}

func TestValidateInputLocal_CSVMissingSchemaURI(t *testing.T) {
	input := &ImportDataCmd{instance: "test-instance", database: "test-db", sourceUri: "file:///tmp/data.csv", sourceFormat: constants.CSV}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify schemaUri")
}

func TestValidateInputLocal_SuccessCSV(t *testing.T) {
	input := &ImportDataCmd{
		instance:        "test-instance",
		database:        "test-db",
		sourceUri:       "file:///tmp/data.csv",
		sourceFormat:    constants.CSV,
		schemaUri:       "file:///tmp/schema.csv",
		databaseDialect: constants.DIALECT_GOOGLESQL,
	}
	err := validateInputLocal(input)
	assert.NoError(t, err)
}

func TestValidateInputLocal_SuccessNonCSV(t *testing.T) {
	input := &ImportDataCmd{
		instance:        "test-instance",
		database:        "test-db",
		sourceUri:       "gs://bucket/data.avro",
		sourceFormat:    "avro",
		databaseDialect: constants.DIALECT_POSTGRESQL,
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

func TestHandleTableNameDefaults(t *testing.T) {
	tests := []struct {
		name      string
		sourceUri string
		expected  string
	}{
		{
			name:      "URIWithTrailingSlash",
			sourceUri: "gs://my-bucket/folder/",
			expected:  "folder",
		},
		{
			name:      "RelativePath",
			sourceUri: "relative/path/some_data.avro",
			expected:  "some_data",
		},
		{
			name:      "LocalPathNoScheme",
			sourceUri: "/tmp/another_file.json",
			expected:  "another_file",
		},
		{
			name:      "GCScheme",
			sourceUri: "s://my-bucket/data_file.txt",
			expected:  "data_file",
		},
		{
			name:      "FileScheme",
			sourceUri: "file:///path/to/my_data.csv",
			expected:  "my_data",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := handleTableNameDefaults("", tc.sourceUri)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// --- Basic Valid Cases ---
		{"myTableName", "mytablename"},
		{"another_table", "another_table"},
		{"table123", "table123"},
		{"_leading_underscore", "_leading_underscore"},
		{"has_numbers_123", "has_numbers_123"},
		{"ALLCAPS", "allcaps"},

		// --- Leading Character Trimming (underscoreOrAlphabet) ---
		{"_ABC", "_abc"},
		{"-table", "table"},
		{"#table", "table"},
		{"1table", "table"},
		{"-1table", "table"},
		{"   leading_spaces", "leading_spaces"},
		{"_leading_underscores_and_spaces", "_leading_underscores_and_spaces"},
		{"__leading_double_underscore", "__leading_double_underscore"},

		// --- Invalid Characters Removal (underscoreOrAlphanumeric) ---
		{"table name", "tablename"},
		{"table.name", "tablename"},
		{"table-name", "tablename"},
		{"table!@#$%^&*()", "table"},
		{"table_name_with_spaces and stuff", "table_name_with_spacesandstuff"},
		{"mixed_Case_AND_SYMBOLS!@", "mixed_case_and_symbols"},
		{"__Table__Name__", "__table__name__"},
		{"Table Name With Space And Special Chars!@#$", "tablenamewithspaceandspecialchars"},

		// --- Empty/Edge Cases ---
		{"", ""},
		{"   ", ""},
		{"!!!", ""},
		{"_!@#", "_"},
		{"_123", "_123"},
		{"__", "__"},
		{"A", "a"},
		{"1", ""},
		{"-", ""},
		{"-1", ""},
		{"-a", "a"},

		// --- Unicode Characters ---
		{"tābļē_ňāmē", "tābļē_ňāmē"},
		{"table_名稱", "table_名稱"},
		{"table_привет", "table_привет"},
		{"😊table😁name", "tablename"},
		{"table_日本語_123", "table_日本語_123"},
		{"你好_world", "你好_world"},
		{"_hello_世界_123", "_hello_世界_123"},
		{"table_!@#_name", "table__name"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) { // Use t.Run for better test output for each case
			got := sanitizeTableName(tt.input)
			if got != tt.expected {
				t.Errorf("sanitizeTableName(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestImportDataCmd_HandleCsvExecute(t *testing.T) {

	tests := []struct {
		name                string
		cmd                 *ImportDataCmd
		expectedStatus      subcommands.ExitStatus
		expectedError       error // Add expectedError
		spannerAccessorMock func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error)
		infoClientFunc      func(ctx context.Context, dbURI string, spDialect string) (*sourcesspanner.InfoSchemaImpl, error)
		csvSchemaFunc       func(projectId, instanceId, dbName, tableName, schemaUri string, schemaFileReader file_reader.FileReader) import_file.CsvSchema
		csvDataFunc         func(projectId, instanceId, dbName, tableName, sourceUri, csvFieldDelimiter string, sourceFileReader file_reader.FileReader) import_file.CsvData
	}{
		{
			name: "successful csv import_existing DB",
			cmd: &ImportDataCmd{
				project:         "test-project",
				instance:        "test-instance",
				database:        "test-db",
				sourceUri:       "../test_data/basic_mysql_dump.test.out",
				schemaUri:       "../test_data/basic_csv_schema.json",
				sourceFormat:    constants.CSV,
				databaseDialect: constants.DIALECT_GOOGLESQL,
			},
			expectedStatus: subcommands.ExitSuccess,
			expectedError:  nil,
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return true, nil
					},
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_GOOGLESQL, nil
					},
				}, nil
			},
			infoClientFunc: func(ctx context.Context, dbURI string, spDialect string) (*sourcesspanner.InfoSchemaImpl, error) {
				return &sourcesspanner.InfoSchemaImpl{}, nil
			},
			csvSchemaFunc: func(projectId, instanceId, dbName, tableName, schemaUri string, schemaFileReader file_reader.FileReader) import_file.CsvSchema {
				return &import_file.MockCsvSchema{}
			},
			csvDataFunc: func(projectId, instanceId, dbName, tableName, sourceUri, csvFieldDelimiter string, sourceFileReader file_reader.FileReader) import_file.CsvData {
				return &import_file.MockCsvData{}
			},
		},
		{
			name: "successful csv import_new DB",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    "../test_data/basic_mysql_dump.test.out",
				schemaUri:    "../test_data/basic_csv_schema.json",
				sourceFormat: constants.CSV,
			},
			expectedStatus: subcommands.ExitSuccess,
			expectedError:  nil,
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return false, nil
					},
					CreateEmptyDatabaseMock: func(ctx context.Context, dbURI, dialect string) error {
						return nil
					},
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_GOOGLESQL, nil
					},
				}, nil
			},
			infoClientFunc: func(ctx context.Context, dbURI string, spDialect string) (*sourcesspanner.InfoSchemaImpl, error) {
				return &sourcesspanner.InfoSchemaImpl{}, nil
			},
			csvSchemaFunc: func(projectId, instanceId, dbName, tableName, schemaUri string, schemaFileReader file_reader.FileReader) import_file.CsvSchema {
				return &import_file.MockCsvSchema{}
			},
			csvDataFunc: func(projectId, instanceId, dbName, tableName, sourceUri, csvFieldDelimiter string, sourceFileReader file_reader.FileReader) import_file.CsvData {
				return &import_file.MockCsvData{}
			},
		},
		{
			name: "error in handling csv",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    "../test_data/basic_mysql_dump.test.out",
				schemaUri:    "../test_data/basic_csv_schema.json",
				sourceFormat: constants.CSV,
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  fmt.Errorf("error in creating info client"),
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return true, nil
					},
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_GOOGLESQL, nil
					},
				}, nil
			},
			infoClientFunc: func(ctx context.Context, dbURI string, spDialect string) (*sourcesspanner.InfoSchemaImpl, error) {
				return nil, fmt.Errorf("error in creating info client")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			originalNewSpannerAccessor := import_file.NewSpannerAccessor
			originalNewInfoSchemaFunc := sourcesspanner.NewInfoSchemaImplWithSpannerClient
			originalNewCsvSchema := import_file.NewCsvSchema
			originalNewCsvData := import_file.NewCsvData

			defer func() {
				import_file.NewSpannerAccessor = originalNewSpannerAccessor
				sourcesspanner.NewInfoSchemaImplWithSpannerClient = originalNewInfoSchemaFunc
				import_file.NewCsvSchema = originalNewCsvSchema
				import_file.NewCsvData = originalNewCsvData
			}()

			// Mock InfoSchema
			sourcesspanner.NewInfoSchemaImplWithSpannerClient = tc.infoClientFunc
			// Mock CsvSchema
			import_file.NewCsvSchema = tc.csvSchemaFunc
			import_file.NewCsvData = tc.csvDataFunc
			import_file.NewSpannerAccessor = tc.spannerAccessorMock

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

func TestImportDataCmd_HandleDumpExecute(t *testing.T) {

	tests := []struct {
		name                string
		cmd                 *ImportDataCmd
		expectedStatus      subcommands.ExitStatus
		expectedError       error // Add expectedError
		spannerAccessorMock func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error)
	}{
		{
			name: "successful MySQL dump import_existing DB",
			cmd: &ImportDataCmd{
				project:         "test-project",
				instance:        "test-instance",
				database:        "test-db",
				sourceUri:       "../test_data/basic_mysql_dump.test.out",
				sourceFormat:    constants.MYSQLDUMP,
				databaseDialect: constants.DIALECT_GOOGLESQL,
			},
			expectedStatus: subcommands.ExitSuccess,
			expectedError:  nil,
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return true, nil
					},
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
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_GOOGLESQL, nil
					},
				}, nil
			},
		},
		{
			name: "successful MySQL dump import_new DB",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    "../test_data/basic_mysql_dump.test.out",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitSuccess,
			expectedError:  nil,
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return false, nil
					},
					CreateEmptyDatabaseMock: func(ctx context.Context, dbURI, dialect string) error {
						return nil
					},
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
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_GOOGLESQL, nil
					},
				}, nil
			},
		},
		{
			name: "error in handling mysql dump",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    "../test_data/basic_mysql_dump.test.out",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  fmt.Errorf("error in handling mysql dump"),
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return true, nil
					},
					UpdateDatabaseMock: func(ctx context.Context, dbURI string, conv *internal.Conv, driver string) error {
						return fmt.Errorf("error in handling mysql dump")
					},
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_GOOGLESQL, nil
					},
				}, nil
			},
		},
		{
			name: "Mysql Dump failed initialisation SpannerAccessor",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instance:     "test-instance",
				database:     "test-db",
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
				instance:     "",
				database:     "test-db",
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
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    "nonexistent_file.sql",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  nil,
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return true, nil
					},
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_GOOGLESQL, nil
					},
				}, nil
			},
		},
		{
			name: "unsupported format",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    "testdata/test.txt",
				sourceFormat: "unsupported",
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  nil, // The function handles the unsupported format internally and returns a failure status
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return true, nil
					},
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_GOOGLESQL, nil
					},
				}, nil
			},
		},
		{
			name: "invalid dialect",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    "testdata/test.txt",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  nil, // The function handles the unsupported format internally and returns a failure status
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return true, nil
					},
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return constants.DIALECT_POSTGRESQL, nil
					},
				}, nil
			},
		},
		{
			name: "get dialect error",
			cmd: &ImportDataCmd{
				project:      "test-project",
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    "testdata/test.txt",
				sourceFormat: constants.MYSQLDUMP,
			},
			expectedStatus: subcommands.ExitFailure,
			expectedError:  nil, // The function handles the unsupported format internally and returns a failure status
			spannerAccessorMock: func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
				return &spanneraccessor.SpannerAccessorMock{
					CheckExistingDbMock: func(ctx context.Context, dbURI string) (bool, error) {
						return true, nil
					},
					GetDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
						return "", fmt.Errorf("failed to get dialect")
					},
				}, nil
			},
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
			sourceUri: "../test_data/basic_mysql_dump.test.out",
			dialect:   constants.DIALECT_GOOGLESQL,
			spannerAccessorMock: func(t *testing.T) spanneraccessor.SpannerAccessor {
				mock := &spanneraccessor.SpannerAccessorMock{
					UpdateDatabaseMock: func(ctx context.Context, dbURI string, conv *internal.Conv, driver string) error {
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
				instance:     "test-instance",
				database:     "test-db",
				sourceUri:    tt.sourceUri,
				sourceFormat: constants.MYSQLDUMP,
			}

			fileReader, _ := file_reader.NewFileReader(ctx, tt.sourceUri)
			defer fileReader.Close()

			err := cmd.handleDatabaseDumpFile(
				ctx,
				fmt.Sprintf("projects/%s/instances/%s/databases/%s", cmd.project, cmd.instance, cmd.database),
				constants.MYSQLDUMP,
				tt.dialect,
				tt.spannerAccessorMock(t),
				fileReader)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestHandleCsv(t *testing.T) {
	expectedDbUri := "projects/test-project/instances/test-instance/databases/test-db"
	expectedDialect := constants.DIALECT_POSTGRESQL

	testCases := []struct {
		desc           string
		expectedErr    error
		infoClientFunc func(ctx context.Context, dbURI string, spDialect string) (*sourcesspanner.InfoSchemaImpl, error)
		csvSchemaFunc  func(projectId, instanceId, dbName, tableName, schemaUri string, schemaFileReader file_reader.FileReader) import_file.CsvSchema
		csvDataFunc    func(projectId, instanceId, dbName, tableName, sourceUri, csvFieldDelimiter string, sourceFileReader file_reader.FileReader) import_file.CsvData
	}{
		{
			desc:        "Successful CSV import",
			expectedErr: nil,
			infoClientFunc: func(ctx context.Context, dbURI string, spDialect string) (*sourcesspanner.InfoSchemaImpl, error) {
				assert.Equal(t, expectedDbUri, dbURI)
				assert.Equal(t, expectedDialect, spDialect)
				return &sourcesspanner.InfoSchemaImpl{}, nil
			},
			csvSchemaFunc: func(projectId, instanceId, dbName, tableName, schemaUri string, schemaFileReader file_reader.FileReader) import_file.CsvSchema {
				assert.Equal(t, "test-project", projectId)
				assert.Equal(t, "test-instance", instanceId)
				assert.Equal(t, "test-db", dbName)
				assert.Equal(t, "test-table", tableName)
				assert.Equal(t, "gs://test-bucket/test_schema.json", schemaUri)

				return &import_file.MockCsvSchema{}
			},
			csvDataFunc: func(projectId, instanceId, dbName, tableName, sourceUri, csvFieldDelimiter string, sourceFileReader file_reader.FileReader) import_file.CsvData {
				assert.Equal(t, "test-project", projectId)
				assert.Equal(t, "test-instance", instanceId)
				assert.Equal(t, "test-db", dbName)
				assert.Equal(t, "test-table", tableName)
				assert.Equal(t, ",", csvFieldDelimiter)
				return &import_file.MockCsvData{}
			},
		},
		{
			desc: "Schema creation fails",
			infoClientFunc: func(ctx context.Context, dbURI string, spDialect string) (*sourcesspanner.InfoSchemaImpl, error) {
				return &sourcesspanner.InfoSchemaImpl{}, nil
			},
			csvSchemaFunc: func(projectId, instanceId, dbName, tableName, schemaUri string, schemaFileReader file_reader.FileReader) import_file.CsvSchema {
				return &import_file.MockCsvSchema{
					CreateSchemaFn: func(ctx context.Context, dialect string, sp spanneraccessor.SpannerAccessor) error {
						return fmt.Errorf("schema creation error")
					},
				}
			},
			expectedErr: fmt.Errorf("schema creation error"),
		},
		{
			desc: "Data import fails",
			infoClientFunc: func(ctx context.Context, dbURI string, spDialect string) (*sourcesspanner.InfoSchemaImpl, error) {
				return &sourcesspanner.InfoSchemaImpl{}, nil
			},
			csvSchemaFunc: func(projectId, instanceId, dbName, tableName, schemaUri string, schemaFileReader file_reader.FileReader) import_file.CsvSchema {
				return &import_file.MockCsvSchema{}
			},
			csvDataFunc: func(projectId, instanceId, dbName, tableName, sourceUri, csvFieldDelimiter string, sourceFileReader file_reader.FileReader) import_file.CsvData {
				return &import_file.MockCsvData{
					ImportDataFn: func(ctx context.Context, spannerInfoSchema *sourcesspanner.InfoSchemaImpl, dialect string, conv *internal.Conv, commonInfoSchema common.InfoSchemaInterface, csv csv.CsvInterface) error {
						return fmt.Errorf("data import error")
					},
				}
			},
			expectedErr: fmt.Errorf("data import error"),
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			ctx := context.Background()
			cmd := &ImportDataCmd{
				project:           "test-project",
				instance:          "test-instance",
				database:          "test-db",
				tableName:         "test-table",
				sourceUri:         "gs://test-bucket/test.csv",
				schemaUri:         "gs://test-bucket/test_schema.json",
				csvFieldDelimiter: ",",
			}
			originalNewInfoSchemaFunc := sourcesspanner.NewInfoSchemaImplWithSpannerClient
			originalNewCsvSchema := import_file.NewCsvSchema
			originalNewCsvData := import_file.NewCsvData

			defer func() {
				sourcesspanner.NewInfoSchemaImplWithSpannerClient = originalNewInfoSchemaFunc
				import_file.NewCsvSchema = originalNewCsvSchema
				import_file.NewCsvData = originalNewCsvData
			}()

			// Mock InfoSchema
			sourcesspanner.NewInfoSchemaImplWithSpannerClient = tC.infoClientFunc
			// Mock CsvSchema
			import_file.NewCsvSchema = tC.csvSchemaFunc
			import_file.NewCsvData = tC.csvDataFunc

			err := cmd.handleCsv(ctx, expectedDbUri, constants.DIALECT_POSTGRESQL, &spanneraccessor.SpannerAccessorMock{}, &file_reader.GcsFileReaderImpl{}, &file_reader.LocalFileReaderImpl{})

			if tC.expectedErr != nil {
				assert.EqualError(t, err, tC.expectedErr.Error())
			} else if err == nil {
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

func TestGetDialectWithDefaults(t *testing.T) {
	testCases := []struct {
		name            string
		inputDialect    string
		expectedDialect string
		expectWarning   bool
	}{
		{
			name:            "Empty Dialect",
			inputDialect:    "",
			expectedDialect: constants.DIALECT_GOOGLESQL,
		},
		{
			name:            "GoogleSQL Dialect",
			inputDialect:    constants.DIALECT_GOOGLESQL,
			expectedDialect: constants.DIALECT_GOOGLESQL,
		},
		{
			name:            "PG Dialect",
			inputDialect:    constants.DIALECT_POSTGRESQL,
			expectedDialect: constants.DIALECT_POSTGRESQL,
		},
		{
			name:            "Unknown Dialect",
			inputDialect:    "postgres",
			expectedDialect: constants.DIALECT_GOOGLESQL,
		},
		{
			name:            "Another Unknown Dialect",
			inputDialect:    "sqlserver",
			expectedDialect: constants.DIALECT_GOOGLESQL,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualDialect := getDialectWithDefaults(tc.inputDialect)
			if actualDialect != tc.expectedDialect {
				t.Errorf("For input '%s', expected databaseDialect '%s', but got '%s'", tc.inputDialect, tc.expectedDialect, actualDialect)
			}
		})
	}
}
