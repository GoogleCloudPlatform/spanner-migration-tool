package import_file

import (
	"cloud.google.com/go/spanner"
	"context"
	"errors"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/stretchr/testify/mock"
	"strings"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/stretchr/testify/assert"
)

func TestCreateSchema(t *testing.T) {
	testCases := []struct {
		name             string
		driver           string
		dumpContent      string
		processDumpError error
		expectedConv     *internal.Conv
		expectedError    error
		expectedErrorMsg string
	}{
		{
			name:             "Successful schema creation",
			driver:           "mysql",
			dumpContent:      "CREATE TABLE test (id INT PRIMARY KEY);",
			processDumpError: nil,
			expectedConv: &internal.Conv{
				SpDialect:    constants.DIALECT_GOOGLESQL,
				Source:       "mysql",
				SpProjectId:  "test-project",
				SpInstanceId: "test-instance",
			},
			expectedError: nil,
		},
		{
			name:             "Error in process dump",
			driver:           "mysql",
			dumpContent:      "CREATE TABLE test (id INT PRIMARY KEY);",
			processDumpError: errors.New("process dump error"),
			expectedConv:     nil,
			expectedError:    errors.New("failed to parse the dump file"),
			expectedErrorMsg: "failed to parse the dump file",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			mockProcessDump := conversion.MockProcessDumpByDialect{}
			mockProcessDump.On("ProcessDump", tc.driver, mock.Anything, mock.Anything).Return(tc.processDumpError)

			source := &ImportFromDumpImpl{
				ProjectId:  "test-project",
				InstanceId: "test-instance",
				DbName:     "test-db",
				DumpUri:    "test-uri",
				DumpReader: strings.NewReader(tc.dumpContent),
				Driver:     tc.driver,
			}

			// Act
			conv, err := source.CreateSchema(constants.DIALECT_GOOGLESQL, &mockProcessDump)

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
			mockProcessDump.AssertExpectations(t)
		})
	}
}

func TestImportData(t *testing.T) {
	testCases := []struct {
		name             string
		driver           string
		dumpContent      string
		processDumpError error
		expectedError    error
		expectedErrorMsg string
		spannerError     error
	}{
		{
			name:             "Successful data import",
			driver:           "mysql",
			dumpContent:      "INSERT INTO test (id) VALUES (1);",
			processDumpError: nil,
			expectedError:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			mockProcessDump := conversion.MockProcessDumpByDialect{}
			mockProcessDump.On("ProcessDump", tc.driver, mock.Anything, mock.Anything).Return(tc.processDumpError)

			spannerClientMock := spannerclient.SpannerClientMock{
				ApplyMock: func(ctx context.Context, ms []*spanner.Mutation, opts ...spanner.ApplyOption) (commitTimestamp time.Time, err error) {
					return time.Now(), tc.spannerError
				},
			}

			source := &ImportFromDumpImpl{
				ProjectId:  "test-project",
				InstanceId: "test-instance",
				DbName:     "test-db",
				DumpUri:    "test-uri",
				DumpReader: strings.NewReader(tc.dumpContent),
				Driver:     tc.driver,
			}
			conv := &internal.Conv{
				SpDialect:    constants.DIALECT_GOOGLESQL,
				Source:       "mysql",
				SpProjectId:  "test-project",
				SpInstanceId: "test-instance",
			}

			// Act
			err := source.ImportData(conv, &mockProcessDump, spannerClientMock)

			assert.True(t, conv.DataMode())
			// Assert
			if tc.expectedError != nil {
				if tc.spannerError != nil {
					assert.Equal(t, err.Error(), tc.spannerError.Error())
				} else {
					assert.EqualError(t, err, tc.expectedErrorMsg)
				}

			} else {
				assert.NoError(t, err)
			}

			mockProcessDump.AssertExpectations(t)
		})
	}
}
