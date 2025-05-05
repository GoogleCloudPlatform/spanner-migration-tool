package import_file

import (
	"errors"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"os"
	"testing"
)

func TestNewImportFromDump(t *testing.T) {
	tests := []struct {
		name                string
		projectId           string
		instanceId          string
		databaseName        string
		dumpUri             string
		sourceFormat        string
		spannerAccessorMock func(t *testing.T) spanneraccessor.SpannerAccessor
		wantErr             bool
		expectedError       string
	}{
		{
			name:         "Successful creation",
			projectId:    "test-project",
			instanceId:   "test-instance",
			databaseName: "test-db",
			dumpUri:      "../test_data/basic_mysql_dump.test.out",
			sourceFormat: constants.MYSQLDUMP,
			spannerAccessorMock: func(t *testing.T) spanneraccessor.SpannerAccessor {
				return &spanneraccessor.SpannerAccessorMock{}
			},
			wantErr: false,
		},
		{
			name:         "Unsupported source format",
			projectId:    "test-project",
			instanceId:   "test-instance",
			databaseName: "test-db",
			dumpUri:      "../test_data/basic_mysql_dump.test.out",
			sourceFormat: "unsupported",
			spannerAccessorMock: func(t *testing.T) spanneraccessor.SpannerAccessor {
				return &spanneraccessor.SpannerAccessorMock{}
			},
			wantErr:       true,
			expectedError: "process dump for sourceFormat unsupported not supported",
		},
		{
			name:         "Failed to open dump file",
			projectId:    "test-project",
			instanceId:   "test-instance",
			databaseName: "test-db",
			dumpUri:      "nonexistent_file.sql",
			sourceFormat: constants.MYSQLDUMP,
			spannerAccessorMock: func(t *testing.T) spanneraccessor.SpannerAccessor {
				return &spanneraccessor.SpannerAccessorMock{}
			},
			wantErr:       true,
			expectedError: "can't read dump file: nonexistent_file.sql due to: open nonexistent_file.sql: no such file or directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spannerAccessor := tt.spannerAccessorMock(t)

			_, err := NewImportFromDump(tt.projectId, tt.instanceId, tt.databaseName, tt.dumpUri, tt.sourceFormat, spannerAccessor)

			if tt.wantErr {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	t.Run("successful creation with a temporary file", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_dump_*.sql")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())

		spannerAccessor := (&spanneraccessor.SpannerAccessorMock{})

		_, err = NewImportFromDump(
			"test-project",
			"test-instance",
			"test-db",
			tmpFile.Name(),
			constants.MYSQLDUMP,
			spannerAccessor,
		)
		assert.NoError(t, err)
	})
}

func TestCreateSchema(t *testing.T) {
	testCases := []struct {
		name                 string
		sourceFormat         string
		dumpContent          string
		processDumpError     error
		schemaToSpannerError error
		expectedConv         *internal.Conv
		expectedError        error
		expectedErrorMsg     string
	}{
		{
			name:                 "Successful schema creation",
			sourceFormat:         constants.MYSQLDUMP,
			dumpContent:          "CREATE TABLE test (id INT PRIMARY KEY);",
			processDumpError:     nil,
			schemaToSpannerError: nil,
			expectedConv: &internal.Conv{
				SpDialect:    constants.DIALECT_GOOGLESQL,
				Source:       constants.MYSQLDUMP,
				SpProjectId:  "test-project",
				SpInstanceId: "test-instance",
			},
			expectedError: nil,
		},
		{
			name:                 "Error in processing dump",
			sourceFormat:         constants.MYSQLDUMP,
			dumpContent:          "CREATE TABLE test (id INT PRIMARY KEY);",
			processDumpError:     errors.New("failed to parse the dump file"),
			schemaToSpannerError: nil,
			expectedConv:         nil,
			expectedError:        errors.New("failed to parse the dump file"),
			expectedErrorMsg:     "failed to parse the dump file",
		},
		{
			name:                 "Error in schema to spanner",
			sourceFormat:         constants.MYSQLDUMP,
			dumpContent:          "CREATE TABLE test (id INT PRIMARY KEY);",
			processDumpError:     nil,
			schemaToSpannerError: errors.New("failed to convert schema to spanner DDL"),
			expectedConv:         nil,
			expectedError:        errors.New("failed to convert schema to spanner DDL"),
			expectedErrorMsg:     "failed to convert schema to spanner DDL",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			spannerAccessorMock := &spanneraccessor.SpannerAccessorMock{}

			file, err := os.CreateTemp("", "testfile.sql")
			file.WriteString(tc.dumpContent)
			file.Close()

			dbDumpProcessorMock := &common.MockDbDump{}
			dbDumpProcessorMock.On("ProcessDump", mock.Anything, mock.Anything).Return(tc.processDumpError)

			dbDumpProcessorMock.On("GetToDdl").Return(&common.MockToDdl{})

			schemaToSchema := &common.MockSchemaToSpanner{}
			schemaToSchema.On("SchemaToSpannerDDL", mock.Anything, mock.Anything, mock.Anything).Return(tc.schemaToSpannerError)

			source := &ImportFromDumpImpl{
				ProjectId:       "test-project",
				InstanceId:      "test-instance",
				DatabaseName:    "test-db",
				DumpUri:         file.Name(),
				SourceFormat:    tc.sourceFormat,
				SpannerAccessor: spannerAccessorMock,
				dbDumpProcessor: dbDumpProcessorMock,
				schemaToSpanner: schemaToSchema,
			}

			conv, err := source.CreateSchema(constants.DIALECT_GOOGLESQL)

			if tc.expectedError != nil {
				assert.EqualError(t, err, tc.expectedErrorMsg)
				assert.Nil(t, conv)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, conv)
				assert.Equal(t, tc.expectedConv.SpDialect, conv.SpDialect)
				assert.Equal(t, tc.expectedConv.Source, conv.Source)
				assert.Equal(t, tc.expectedConv.SpProjectId, conv.SpProjectId)
				assert.Equal(t, tc.expectedConv.SpInstanceId, conv.SpInstanceId)
				assert.True(t, conv.SchemaMode())
			}
		})
	}
}

func TestImportData(t *testing.T) {
	testCases := []struct {
		name             string
		sourceFormat     string
		dumpContent      string
		expectedError    error
		expectedErrorMsg string
	}{
		{
			name:             "Successful data import",
			sourceFormat:     constants.MYSQLDUMP,
			dumpContent:      "INSERT INTO test (id) VALUES (1);",
			expectedError:    nil,
			expectedErrorMsg: "",
		},
		{
			name:             "Error in processing dump",
			sourceFormat:     constants.MYSQLDUMP,
			dumpContent:      "INSERT INTO test (id) VALUES (1);",
			expectedErrorMsg: "error in processing dump",
			expectedError:    errors.New("error in processing dump"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			spannerClientMock := spannerclient.SpannerClientMock{}

			spannerAccessorMock := &spanneraccessor.SpannerAccessorMock{
				GetSpannerClientMock: func() spannerclient.SpannerClient {
					return &spannerClientMock
				},
			}
			schemaToSchema := &common.MockSchemaToSpanner{}

			dbDumpProcessorMock := &common.MockDbDump{}
			dbDumpProcessorMock.On("ProcessDump", mock.Anything, mock.Anything).Return(tc.expectedError)

			file, err := os.CreateTemp("", "testfile.sql")
			file.WriteString(tc.dumpContent)
			file.Close()
			source := &ImportFromDumpImpl{
				ProjectId:       "test-project",
				InstanceId:      "test-instance",
				DatabaseName:    "test-db",
				DumpUri:         file.Name(),
				SourceFormat:    tc.sourceFormat,
				SpannerAccessor: spannerAccessorMock,
				dbDumpProcessor: dbDumpProcessorMock,
				schemaToSpanner: schemaToSchema,
			}

			conv := &internal.Conv{
				SpDialect:    constants.DIALECT_GOOGLESQL,
				Source:       tc.sourceFormat,
				SpProjectId:  "test-project",
				SpInstanceId: "test-instance",
			}

			err = source.ImportData(conv)

			assert.True(t, conv.DataMode())

			if tc.expectedError != nil {
				assert.EqualError(t, err, tc.expectedErrorMsg)
			} else {
				assert.NoError(t, err)
			}

		})
	}
}

func TestGetDbDump(t *testing.T) {
	testCases := []struct {
		name          string
		sourceFormat  string
		expectedType  interface{}
		expectedError error
	}{
		{
			name:         "MySQL Dump",
			sourceFormat: constants.MYSQLDUMP,
			expectedType: mysql.DbDumpImpl{},
		},
		{
			name:         "Postgres Dump",
			sourceFormat: constants.PGDUMP,
			expectedType: postgres.DbDumpImpl{},
		},
		{
			name:          "Unsupported SourceFormat",
			sourceFormat:  "unsupported",
			expectedError: errors.New("process dump for sourceFormat unsupported not supported"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbDump, err := getDbDump(tc.sourceFormat)
			if tc.expectedError != nil {
				assert.EqualError(t, err, tc.expectedError.Error())
				assert.Nil(t, dbDump)
			} else {
				assert.NoError(t, err)
				assert.IsType(t, tc.expectedType, dbDump)
			}
		})
	}
}

func TestFinalize(t *testing.T) {
	file, err := os.CreateTemp("", "testfile.sql")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	source := &ImportFromDumpImpl{
		dumpReader: file,
	}

	err = source.Finalize()
	assert.NoError(t, err)

	// Verify that the file is closed
	_, err = file.Read([]byte{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "file already closed")
}
