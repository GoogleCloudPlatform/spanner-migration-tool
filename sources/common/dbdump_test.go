package common

import (
	"bufio"
	"errors"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/stretchr/testify/mock"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/mocks"
	"github.com/stretchr/testify/assert"
)

func TestProcessDbDump_SchemaMode_Success(t *testing.T) {
	mockDbDump := &MockDbDump{}
	mockDbDump.On("ProcessDump", mock.Anything, mock.Anything).Return(nil)
	mockToDdl := &MockToDdl{}
	mockDbDump.On("GetToDdl").Return(mockToDdl)

	mockExprVerifier := &mocks.MockExpressionVerificationAccessor{}
	mockDDLVerifier := &expressions_api.MockDDLVerifier{}

	mockDDLVerifier.VerifySpannerDDLMock = func(conv *internal.Conv, expressionDetails []internal.ExpressionDetail) (internal.VerifyExpressionsOutput, error) {
		return internal.VerifyExpressionsOutput{}, nil
	}
	mockDDLVerifier.GetSourceExpressionDetailsMock = func(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail {
		return []internal.ExpressionDetail{}
	}

	conv := internal.MakeConv()
	conv.SetSchemaMode()
	r := getReader()

	err := ProcessDbDump(conv, r, mockDbDump, mockDDLVerifier, mockExprVerifier)
	assert.NoError(t, err)
	mockDbDump.AssertExpectations(t)
}

func TestProcessDbDump_ProcessDump_Failure(t *testing.T) {
	mockDbDump := &MockDbDump{}
	mockDbDump.On("ProcessDump", mock.Anything, mock.Anything).Return(errors.New("process dump error"))
	mockExprVerifier := &mocks.MockExpressionVerificationAccessor{}
	mockDDLVerifier := &expressions_api.MockDDLVerifier{}
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	r := &internal.Reader{}

	err := ProcessDbDump(conv, r, mockDbDump, mockDDLVerifier, mockExprVerifier)
	assert.Error(t, err)
	assert.Equal(t, "process dump error", err.Error())
	mockDbDump.AssertExpectations(t)
}

func TestProcessDbDump_SchemaToSpannerDDL_Failure(t *testing.T) {
	mockDbDump := &MockDbDump{}
	mockDbDump.On("ProcessDump", mock.Anything, mock.Anything).Return(nil)
	mockToDdl := &MockToDdl{}
	mockDbDump.On("GetToDdl").Return(mockToDdl)

	mockExprVerifier := &mocks.MockExpressionVerificationAccessor{}
	mockExprVerifier.On("VerifyExpressions", mock.Anything, mock.Anything).Return(errors.New("SchemaToSpannerDDL error"))
	mockExprVerifier.On("RefreshSpannerClient", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockDDLVerifier := &expressions_api.MockDDLVerifier{}
	mockDDLVerifier.VerifySpannerDDLMock = func(conv *internal.Conv, expressionDetails []internal.ExpressionDetail) (internal.VerifyExpressionsOutput, error) {
		return internal.VerifyExpressionsOutput{}, nil
	}
	mockDDLVerifier.GetSourceExpressionDetailsMock = func(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail {
		return []internal.ExpressionDetail{}
	}

	conv := internal.MakeConv()
	conv.SpDialect = constants.DIALECT_GOOGLESQL
	conv.Source = constants.MYSQLDUMP
	conv.SpProjectId = "test-project"
	conv.SpInstanceId = "test-instance"
	conv.SetSchemaMode()
	r := getReader()
	err := ProcessDbDump(conv, r, mockDbDump, mockDDLVerifier, mockExprVerifier)
	assert.Nilf(t, err, "SchemaToSpannerDDL error")
	mockDbDump.AssertExpectations(t)
}

func getReader() *internal.Reader {
	return internal.NewReader(bufio.NewReader(strings.NewReader("CREATE TABLE test (id INT PRIMARY KEY);")), nil)
}
