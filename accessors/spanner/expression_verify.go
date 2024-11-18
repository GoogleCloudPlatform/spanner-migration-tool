package spanneraccessor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

const THREAD_POOL = 500

type ExpressionVerificationAccessor interface {
	//Batch API which parallelizes expression verification calls
	VerifyExpressions(ctx context.Context, adminClient spanneradmin.AdminClient, verifyExpressionsInput internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput
}

type ExpressionVerificationAccessorImpl struct {
	SpannerAccessor SpannerAccessorImpl
}

func (ev *ExpressionVerificationAccessorImpl) VerifyExpressions(ctx context.Context, adminClient spanneradmin.AdminClient, verifyExpressionsInput internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput {
	err := ev.validateRequest(verifyExpressionsInput)
	if err != nil {
		return internal.VerifyExpressionsOutput{Err: err}
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", verifyExpressionsInput.Project, verifyExpressionsInput.Instance, "smt-staging-db")
	dbExists, err := ev.SpannerAccessor.CheckExistingDb(ctx, adminClient, dbURI)
	if err != nil {
		return internal.VerifyExpressionsOutput{Err: err}
	}
	if dbExists {
		err := ev.SpannerAccessor.DropDatabase(ctx, adminClient, dbURI)
		if err != nil {
			return internal.VerifyExpressionsOutput{Err: err}
		}
	}
	verifyExpressionsInput.Conv, err = ev.simplifyConv(verifyExpressionsInput.Conv)
	if err != nil {
		return internal.VerifyExpressionsOutput{Err: err}
	}
	err = ev.SpannerAccessor.CreateDatabase(ctx, adminClient, dbURI, verifyExpressionsInput.Conv, verifyExpressionsInput.Source, constants.DATAFLOW_MIGRATION)
	if err != nil {
		return internal.VerifyExpressionsOutput{Err: err}
	}
	//Drop the staging database after verifications are completed.
	defer ev.SpannerAccessor.DropDatabase(ctx, adminClient, dbURI)
	verificationInputList := make([]internal.ExpressionVerificationInput, len(verifyExpressionsInput.ExpressionDetailList))
    for i, expressionDetail := range verifyExpressionsInput.ExpressionDetailList {
        verificationInputList[i] = internal.ExpressionVerificationInput{
            DbURI:            dbURI,
            ExpressionDetail: expressionDetail,
        }
    }
	r := common.RunParallelTasksImpl[internal.ExpressionVerificationInput, internal.ExpressionVerificationOutput]{}
	expressionVerificationOutputList, _ := r.RunParallelTasks(verificationInputList, THREAD_POOL, ev.verifyExpressionInternal, true)
	var verifyExpressionsOutput internal.VerifyExpressionsOutput
	for _, expressionVerificationOutput := range expressionVerificationOutputList {
		verifyExpressionsOutput.ExpressionVerificationOutputList = append(verifyExpressionsOutput.ExpressionVerificationOutputList, expressionVerificationOutput.Result)
	}
	return verifyExpressionsOutput
}

func (ev *ExpressionVerificationAccessorImpl) verifyExpressionInternal(expressionVerificationInput internal.ExpressionVerificationInput, mutex *sync.Mutex) common.TaskResult[internal.ExpressionVerificationOutput] {
	var sqlStatement string
	switch expressionVerificationInput.ExpressionDetail.Type {
	case "CHECK":
		sqlStatement = fmt.Sprintf("SELECT 1 from %s where %s;", expressionVerificationInput.ExpressionDetail.ReferenceElement.Name, expressionVerificationInput.ExpressionDetail.Expression)
	case "DEFAULT":
		sqlStatement = fmt.Sprintf("SELECT CAST(%s) as %s", expressionVerificationInput.ExpressionDetail.Expression, expressionVerificationInput.ExpressionDetail.ReferenceElement.Name)
	default:
		return common.TaskResult[internal.ExpressionVerificationOutput]{Result: internal.ExpressionVerificationOutput{Result: false, Err: fmt.Errorf("invalid expression type requested")}, Err: nil}
	}
	result, err := ev.SpannerAccessor.ValidateDML(context.Background(), expressionVerificationInput.DbURI, sqlStatement)
	return common.TaskResult[internal.ExpressionVerificationOutput]{Result: internal.ExpressionVerificationOutput{Result: result, Err: err}, Err: nil}
}

func (ev *ExpressionVerificationAccessorImpl) validateRequest(verifyExpressionsInput internal.VerifyExpressionsInput) error {
	if verifyExpressionsInput.Project == "" || verifyExpressionsInput.Instance == "" || verifyExpressionsInput.Conv == nil || verifyExpressionsInput.Source == "" {
		return fmt.Errorf("one of project, instance, conv or source is empty. These are mandatory fields = %v", verifyExpressionsInput)
	}
	for _,expressionDetail := range verifyExpressionsInput.ExpressionDetailList {
		if expressionDetail.ExpressionId == "" || expressionDetail.Expression == "" || expressionDetail.Type == "" || expressionDetail.ReferenceElement.Name == "" {
			return fmt.Errorf("one of expressionId, expression, type or referenceElement.Name is empty. These are mandatory fields = %v", expressionDetail)
		}
	}
	return nil
}

//We simplify conv to remove any existing expressions that are part of the SpSchema to ensure that the stagingDB creation
//does not fail due to inconsistent, user configured expressions during a schema conversion session. 
//The minimal conv object needed for stagingDB is one which contains all table and column definitions only.
func (ev *ExpressionVerificationAccessorImpl) simplifyConv(inputConv *internal.Conv) (*internal.Conv, error) {
	convCopy := &internal.Conv{}
    convJSON, err := json.Marshal(inputConv)
    if err != nil {
        return nil, fmt.Errorf("error marshaling conv: %v", err)
    }
    err = json.Unmarshal(convJSON, convCopy)
    if err != nil {
        return nil, fmt.Errorf("error unmarshaling conv: %v", err)
    }
    // Reset the SpSequences field in the Conv copy.  A nil check
    // here won't hurt but shouldn't technically be necessary as the
    // map should always be initialized.
    if convCopy.SpSequences != nil {
        convCopy.SpSequences = make(map[string]ddl.Sequence) // Reset to empty map
    }
	return convCopy, nil
}