package api

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

func (t *TableAPIHandler) HandleExpressionColErrorForTest(exp *internal.ExpressionVerificationOutput, conv *internal.Conv, errorType internal.SchemaIssue) string {
	return t.handleExpressionColError(exp, conv, errorType)
}
