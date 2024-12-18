package mocks

import (
	"context"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/stretchr/testify/mock"
)

// MockExpressionVerificationAccessor is a mock of ExpressionVerificationAccessor
type MockExpressionVerificationAccessor struct {
    mock.Mock
}

// VerifyExpressions is a mocked method for expression verification
func (m *MockExpressionVerificationAccessor) VerifyExpressions(ctx context.Context, input internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput {
    args := m.Called(ctx, input)
    return args.Get(0).(internal.VerifyExpressionsOutput)
}