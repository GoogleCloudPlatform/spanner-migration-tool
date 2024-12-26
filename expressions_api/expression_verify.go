package expressions_api

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/task"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

const THREAD_POOL = 500

type ExpressionVerificationAccessor interface {
	//Batch API which parallelizes expression verification calls
	VerifyExpressions(ctx context.Context, verifyExpressionsInput internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput
	RefreshSpannerClient(ctx context.Context, project string, instance string) error
}

type ExpressionVerificationAccessorImpl struct {
	SpannerAccessor *spanneraccessor.SpannerAccessorImpl
}

func NewExpressionVerificationAccessorImpl(ctx context.Context, project string, instance string) (*ExpressionVerificationAccessorImpl, error) {
	var spannerAccessor *spanneraccessor.SpannerAccessorImpl
	var err error
	fmt.Printf("#######")
	fmt.Printf(fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.TEMP_DB))
	if project != "" && instance != "" {
		spannerAccessor, err = spanneraccessor.NewSpannerAccessorClientImplWithSpannerClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.TEMP_DB))
		if err != nil {
			return nil, err
		}
	} else {
		spannerAccessor, err = spanneraccessor.NewSpannerAccessorClientImpl(ctx)
		if err != nil {
			return nil, err
		}
	}
	return &ExpressionVerificationAccessorImpl{
		SpannerAccessor: spannerAccessor,
	}, nil
}

// APIs to verify and process Spanner DLL features such as Default Values, Check Constraints
type DDLVerifier interface {
	VerifySpannerDDL(conv *internal.Conv, expressionDetails []internal.ExpressionDetail) (internal.VerifyExpressionsOutput, error)
	GetSourceExpressionDetails(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail
	GetSpannerExpressionDetails(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail
	RefreshSpannerClient(ctx context.Context, project string, instance string) error
}
type DDLVerifierImpl struct {
	Expressions ExpressionVerificationAccessor
}

func NewDDLVerifierImpl(ctx context.Context, project string, instance string) (*DDLVerifierImpl, error) {
	expVerifier, err := NewExpressionVerificationAccessorImpl(ctx, project, instance)
	return &DDLVerifierImpl{
		Expressions: expVerifier,
	}, err
}

func (ev *ExpressionVerificationAccessorImpl) VerifyExpressions(ctx context.Context, verifyExpressionsInput internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput {
	err := ev.validateRequest(verifyExpressionsInput)
	if err != nil {
		return internal.VerifyExpressionsOutput{Err: err}
	}
	dbURI := ev.SpannerAccessor.SpannerClient.DatabaseName()
	dbExists, err := ev.SpannerAccessor.CheckExistingDb(ctx, dbURI)
	if err != nil {
		return internal.VerifyExpressionsOutput{Err: err}
	}
	if dbExists {
		err := ev.SpannerAccessor.DropDatabase(ctx, dbURI)
		if err != nil {
			return internal.VerifyExpressionsOutput{Err: err}
		}
	}
	verifyExpressionsInput.Conv, err = ev.removeExpressions(verifyExpressionsInput.Conv)
	if err != nil {
		return internal.VerifyExpressionsOutput{Err: err}
	}
	err = ev.SpannerAccessor.CreateDatabase(ctx, dbURI, verifyExpressionsInput.Conv, verifyExpressionsInput.Source, constants.DATAFLOW_MIGRATION)
	if err != nil {
		return internal.VerifyExpressionsOutput{Err: err}
	}
	//Drop the staging database after verifications are completed.
	defer ev.SpannerAccessor.DropDatabase(ctx, dbURI)
	//This recreates a spanner client for the staging database before doing operations on it.
	ev.SpannerAccessor.Refresh(ctx, dbURI)
	r := task.RunParallelTasksImpl[internal.ExpressionDetail, internal.ExpressionVerificationOutput]{}
	expressionVerificationOutputList, _ := r.RunParallelTasks(verifyExpressionsInput.ExpressionDetailList, THREAD_POOL, ev.verifyExpressionInternal, true)
	var verifyExpressionsOutput internal.VerifyExpressionsOutput
	var errorCount int16 = 0
	for _, expressionVerificationOutput := range expressionVerificationOutputList {
		verifyExpressionsOutput.ExpressionVerificationOutputList = append(verifyExpressionsOutput.ExpressionVerificationOutputList, expressionVerificationOutput.Result)
		if expressionVerificationOutput.Result.Err != nil {
			errorCount++
		}
	}
	if errorCount != 0 {
		verifyExpressionsOutput.Err = fmt.Errorf("%d expressions either failed verification or did not get verified. Please look at the individual errors returned for each expression", errorCount)

	}
	return verifyExpressionsOutput
}

func (ev *ExpressionVerificationAccessorImpl) RefreshSpannerClient(ctx context.Context, project string, instance string) error {
	spannerClient, err := spannerclient.NewSpannerClientImpl(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.TEMP_DB))
	if err != nil {
		return err
	}
	ev.SpannerAccessor.SpannerClient = spannerClient
	return nil
}

func (ev *ExpressionVerificationAccessorImpl) verifyExpressionInternal(expressionDetail internal.ExpressionDetail, mutex *sync.Mutex) task.TaskResult[internal.ExpressionVerificationOutput] {
	var sqlStatement string
	switch expressionDetail.Type {
	case constants.CHECK_EXPRESSION:
		sqlStatement = fmt.Sprintf("SELECT 1 from %s where %s;", expressionDetail.ReferenceElement.Name, expressionDetail.Expression)
	case constants.DEFAUT_EXPRESSION:
		sqlStatement = fmt.Sprintf("SELECT CAST(%s as %s)", expressionDetail.Expression, expressionDetail.ReferenceElement.Name)
	default:
		return task.TaskResult[internal.ExpressionVerificationOutput]{Result: internal.ExpressionVerificationOutput{Result: false, Err: fmt.Errorf("invalid expression type requested")}, Err: nil}
	}
	result, err := ev.SpannerAccessor.ValidateDML(context.Background(), sqlStatement)
	return task.TaskResult[internal.ExpressionVerificationOutput]{Result: internal.ExpressionVerificationOutput{Result: result, Err: err, ExpressionDetail: expressionDetail}, Err: nil}
}

func (ev *ExpressionVerificationAccessorImpl) validateRequest(verifyExpressionsInput internal.VerifyExpressionsInput) error {
	if verifyExpressionsInput.Conv == nil || verifyExpressionsInput.Source == "" {
		return fmt.Errorf("one of conv or source is empty. These are mandatory fields = %v", verifyExpressionsInput)
	}
	for _, expressionDetail := range verifyExpressionsInput.ExpressionDetailList {
		if expressionDetail.ExpressionId == "" || expressionDetail.Expression == "" || expressionDetail.Type == "" || expressionDetail.ReferenceElement.Name == "" {
			return fmt.Errorf("one of expressionId, expression, type or referenceElement.Name is empty. These are mandatory fields = %v", expressionDetail)
		}
	}
	return nil
}

// We simplify conv to remove any existing expressions that are part of the SpSchema to ensure that the stagingDB creation
// does not fail due to inconsistent, user configured expressions during a schema conversion session.
// The minimal conv object needed for stagingDB is one which contains all table and column definitions only.
func (ev *ExpressionVerificationAccessorImpl) removeExpressions(inputConv *internal.Conv) (*internal.Conv, error) {
	convCopy := &internal.Conv{}
	convJSON, err := json.Marshal(inputConv)
	if err != nil {
		return nil, fmt.Errorf("error marshaling conv: %v", err)
	}
	err = json.Unmarshal(convJSON, convCopy)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling conv: %v", err)
	}
	//Set sequences as nil
	//TODO: Implement similar checks for DEFAULT and CHECK constraints as well
	convCopy.SpSequences = nil
	for _, table := range convCopy.SpSchema {
		for colName, colDef := range table.ColDefs {
			colDef.AutoGen = ddl.AutoGenCol{}
			colDef.DefaultValue = ddl.DefaultValue{}
			table.ColDefs[colName] = colDef
		}
	}
	return convCopy, nil
}

func (ddlv *DDLVerifierImpl) VerifySpannerDDL(conv *internal.Conv, expressionDetails []internal.ExpressionDetail) (internal.VerifyExpressionsOutput, error) {
	ctx := context.Background()
	verifyExpressionsInput := internal.VerifyExpressionsInput{
		Conv:                 conv,
		Source:               conv.Source,
		ExpressionDetailList: expressionDetails,
	}
	ddlv.Expressions.RefreshSpannerClient(context.Background(), conv.SpProjectId, conv.SpInstanceId)
	verificationResults := ddlv.Expressions.VerifyExpressions(ctx, verifyExpressionsInput)

	return verificationResults, verificationResults.Err
}

func (ddlv *DDLVerifierImpl) GetSourceExpressionDetails(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail {
	expressionDetails := []internal.ExpressionDetail{}
	// Collect default values for verification
	for _, tableId := range tableIds {
		srcTable := conv.SrcSchema[tableId]
		for _, srcColId := range srcTable.ColIds {
			srcCol := srcTable.ColDefs[srcColId]
			if srcCol.DefaultValue.IsPresent {
				defaultValueExp := internal.ExpressionDetail{
					ReferenceElement: internal.ReferenceElement{
						Name: conv.SpSchema[tableId].ColDefs[srcColId].T.Name,
					},
					ExpressionId: srcCol.DefaultValue.Value.ExpressionId,
					Expression:   srcCol.DefaultValue.Value.Statement,
					Type:         "DEFAULT",
					Metadata:     map[string]string{"TableId": tableId, "ColId": srcColId},
				}
				expressionDetails = append(expressionDetails, defaultValueExp)
			}
		}
	}
	return expressionDetails
}

func (ddlv *DDLVerifierImpl) GetSpannerExpressionDetails(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail {
	expressionDetails := []internal.ExpressionDetail{}
	// Collect default values for verification
	for _, tableId := range tableIds {
		spTable := conv.SpSchema[tableId]
		for _, spColId := range spTable.ColIds {
			spCol := spTable.ColDefs[spColId]
			if spCol.DefaultValue.IsPresent {
				defaultValueExp := internal.ExpressionDetail{
					ReferenceElement: internal.ReferenceElement{
						Name: conv.SpSchema[tableId].ColDefs[spColId].T.Name,
					},
					ExpressionId: spCol.DefaultValue.Value.ExpressionId,
					Expression:   spCol.DefaultValue.Value.Statement,
					Type:         "DEFAULT",
					Metadata:     map[string]string{"TableId": tableId, "ColId": spColId},
				}
				expressionDetails = append(expressionDetails, defaultValueExp)
			}
		}
	}
	return expressionDetails
}

func (ddlv *DDLVerifierImpl) RefreshSpannerClient(ctx context.Context, project string, instance string) error {
	return ddlv.Expressions.RefreshSpannerClient(ctx, project, instance)
}
