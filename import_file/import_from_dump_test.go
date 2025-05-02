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

func TestCreateSchema(t *testing.T) {
	testCases := []struct {
		name                 string
		driver               string
		dumpContent          string
		processDumpError     error
		schemaToSpannerError error
		expectedConv         *internal.Conv
		expectedError        error
		expectedErrorMsg     string
	}{
		{
			name:                 "Successful schema creation",
			driver:               constants.MYSQLDUMP,
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
			name:                 "Error in process dump",
			driver:               constants.MYSQLDUMP,
			dumpContent:          "CREATE TABLE test (id INT PRIMARY KEY);",
			processDumpError:     errors.New("failed to parse the dump file"),
			schemaToSpannerError: nil,
			expectedConv:         nil,
			expectedError:        errors.New("failed to parse the dump file"),
			expectedErrorMsg:     "failed to parse the dump file",
		},
		{
			name:                 "Error in process dump",
			driver:               constants.MYSQLDUMP,
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
				Driver:          tc.driver,
				SpannerAccessor: spannerAccessorMock,
				dbDumpProcessor: dbDumpProcessorMock,
				schemaToSpanner: schemaToSchema,
			}

			// Act
			conv, err := source.CreateSchema(constants.DIALECT_GOOGLESQL)

			// Assert
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
		driver           string
		dumpContent      string
		expectedError    error
		expectedErrorMsg string
	}{
		{
			name:             "Successful data import",
			driver:           constants.MYSQLDUMP,
			dumpContent:      "INSERT INTO test (id) VALUES (1);",
			expectedError:    nil,
			expectedErrorMsg: "",
		},
		{
			name:             "Successful data import",
			driver:           constants.MYSQLDUMP,
			dumpContent:      "INSERT INTO test (id) VALUES (1);",
			expectedErrorMsg: "error in processing dump",
			expectedError:    errors.New("error in processing dump"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange

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
				Driver:          tc.driver,
				SpannerAccessor: spannerAccessorMock,
				dbDumpProcessor: dbDumpProcessorMock,
				schemaToSpanner: schemaToSchema,
			}

			conv := &internal.Conv{
				SpDialect:    constants.DIALECT_GOOGLESQL,
				Source:       tc.driver,
				SpProjectId:  "test-project",
				SpInstanceId: "test-instance",
			}

			// Act
			err = source.ImportData(conv)

			assert.True(t, conv.DataMode())
			// Assert
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
		driver        string
		expectedType  interface{}
		expectedError error
	}{
		{
			name:         "MySQL Dump",
			driver:       constants.MYSQLDUMP,
			expectedType: mysql.DbDumpImpl{},
		},
		{
			name:         "Postgres Dump",
			driver:       constants.PGDUMP,
			expectedType: postgres.DbDumpImpl{},
		},
		{
			name:          "Unsupported Driver",
			driver:        "unsupported",
			expectedError: errors.New("process dump for driver unsupported not supported"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dbDump, err := getDbDump(tc.driver)
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
