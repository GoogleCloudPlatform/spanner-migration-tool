package spanneraccessor

import (
	"context"
	"fmt"
	"sync"

	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
)

type ExpressionVerificationAccessor interface {
	//Creates an empty staging database if it does not exist.
	CreateStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error)
	//Initialize an empty database (or force initializes a non-empty one) with a conv object
	InitializeStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, force bool) (bool, error)
	//Deletes a staging database
	DeleteStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error)
	//Internal API which verifies an expression by making a call to Spanner
	VerifyExpression(verificationInput internal.VerificationInput, mutex *sync.Mutex) common.TaskResult[internal.VerificationResult]
	//Batch API which parallelizes expression verification calls
	BatchVerifyExpressions(ctx context.Context, verificationInputList []internal.VerificationInput) internal.BatchVerificationResult
}

type ExpressionVerificationAccessorImpl struct {
	SpannerAccessor SpannerAccessorImpl
}

func (ev *ExpressionVerificationAccessorImpl) CreateStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error) {
	dbExists, err := ev.SpannerAccessor.CheckExistingDb(ctx, adminClient, dbURI)
	if err != nil {
		return false, err
	}
	if !dbExists {
		err := ev.SpannerAccessor.CreateEmptyDatabase(ctx, adminClient, dbURI)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (ev *ExpressionVerificationAccessorImpl) InitializeStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, force bool) (bool, error) {
	if !force {
		//if force is disabled, fail if the database is not empty
		if ev.SpannerAccessor.ValidateDDL(ctx, adminClient, dbURI) != nil {
			return false, fmt.Errorf("staging database is not empty and force is disabled, cannot update the staging db")
		} else {
			ev.SpannerAccessor.UpdateDatabase(ctx, adminClient, dbURI, conv, conv.SpDialect)
		}
	} else {
		//if force is enabled, update database if it is empty, otherwise drop the non-empty database and create
		// a new one.
		if ev.SpannerAccessor.ValidateDDL(ctx, adminClient, dbURI) != nil {
			fmt.Printf("StagingDb was not empty but force is passed!!")
			ev.SpannerAccessor.CreateOrUpdateDatabase(ctx, adminClient, dbURI, conv.SpDialect, conv, constants.DATAFLOW_MIGRATION)
		}
	}
	return true, nil
}

func (ev *ExpressionVerificationAccessorImpl) DeleteStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error) {
	err := ev.SpannerAccessor.DropDatabase(ctx, adminClient, dbURI)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (ev *ExpressionVerificationAccessorImpl) VerifyExpression(verificationInput internal.VerificationInput, mutex *sync.Mutex) common.TaskResult[internal.VerificationResult] {
	var sqlStatement string
	switch verificationInput.ExpressionDetail.Type {
	case "CHECK":
		sqlStatement = fmt.Sprintf("SELECT 1 from %s where %s;", verificationInput.ExpressionDetail.ReferenceElement.Name, verificationInput.ExpressionDetail.Expression)
	case "DEFAULT":
		sqlStatement = fmt.Sprintf("SELECT CAST(%s) as %s", verificationInput.ExpressionDetail.Expression, verificationInput.ExpressionDetail.ReferenceElement.Name)
	default:
		return common.TaskResult[internal.VerificationResult]{Result: internal.VerificationResult{Result: false, Err: fmt.Errorf("invalid expression type requested")}, Err: nil}
	}
	result, err := ev.SpannerAccessor.ValidateDML(context.Background(), verificationInput.DbURI, sqlStatement)
	return common.TaskResult[internal.VerificationResult]{Result: internal.VerificationResult{Result: result, Err: err}, Err: nil}
}

func (ev *ExpressionVerificationAccessorImpl) BatchVerifyExpressions(ctx context.Context, verificationInputList []internal.VerificationInput) internal.BatchVerificationResult {
	r := common.RunParallelTasksImpl[internal.VerificationInput, internal.VerificationResult]{}
	THREAD_POOL := 500
	verificationResults, _ := r.RunParallelTasks(verificationInputList, THREAD_POOL, ev.VerifyExpression, true)
	var batchVerificationResult internal.BatchVerificationResult
	for _, verificationResult := range verificationResults {
		batchVerificationResult.VerificationResultList = append(batchVerificationResult.VerificationResultList, verificationResult.Result)
	}
	fmt.Printf("Thread Pool size: %d\n", THREAD_POOL)
	return batchVerificationResult
}
