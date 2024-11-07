package spanneraccessor

import (
	"context"

	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)


type ExpressionVerificationAccessor interface {
	//Creates an empty staging database if it does not exist. 
	CreateStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error)
	//Initialize an empty database (or force initializes a non-empty one) with a conv object
	InitializeStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, force bool) (bool, error)
	//Deletes a staging database
	DeleteStagingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error)
	//Verifies an expression by making a call to Spanner
	VerifyExpression(ctx context.Context, spannerClient spannerclient.SpannerClient, dbURI string, conv *internal.Conv, expressionDetail internal.ExpressionDetail) (internal.VerificationResult)
	//Batch API which parallelizes expression verification calls
	BatchVerifyExpressions(ctx context.Context, spannerClient spannerclient.SpannerClient, dbURI string, conv *internal.Conv, expressionDetails []internal.ExpressionDetail) (internal.BatchVerificationResult)
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

func (ev *ExpressionVerificationAccessorImpl) VerifyExpression(ctx context.Context, spannerClient spannerclient.SpannerClient, dbURI string, conv *internal.Conv, expressionDetail internal.ExpressionDetail) (internal.VerificationResult) {
	return internal.VerificationResult{}
}

func (ev *ExpressionVerificationAccessorImpl) BatchVerifyExpressions(ctx context.Context, spannerClient spannerclient.SpannerClient, dbURI string, conv *internal.Conv, expressionDetails []internal.ExpressionDetail) (internal.BatchVerificationResult) {
	return internal.BatchVerificationResult{}
}




