package spanneraccessor

import (
	"context"
	"sync"

	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
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

type ExpressionVerificationAccessorImpl struct{}

func (ev *ExpressionVerificationAccessorImpl) CreateStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error) {
	return true, nil
}

func (ev *ExpressionVerificationAccessorImpl) InitializeStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, force bool) (bool, error) {
	return true, nil
}

func (ev *ExpressionVerificationAccessorImpl) DeleteStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error) {
	return true, nil
}

func (ev *ExpressionVerificationAccessorImpl) VerifyExpression(verificationInput internal.VerificationInput, mutex *sync.Mutex) common.TaskResult[internal.VerificationResult] {
	return common.TaskResult[internal.VerificationResult]{}
}

func (ev *ExpressionVerificationAccessorImpl) BatchVerifyExpressions(ctx context.Context, verificationInputList []internal.VerificationInput) (internal.BatchVerificationResult) {
	return internal.BatchVerificationResult{}
}




