package expressions_api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"os"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestVerifyExpressions(t *testing.T) {
	ctx := context.Background()
	conv := internal.MakeConv()
	ReadSessionFile(conv, "../../test_data/session_expression_verify.json")
	input := internal.VerifyExpressionsInput{
		Project:  "test-project",
		Instance: "test-instance",
		Conv:     conv,
		Source:   "mysql",
		ExpressionDetailList: []internal.ExpressionDetail{
			{
				Expression: "id > 10",
				Type:       "CHECK", ReferenceElement: internal.ReferenceElement{Name: "Books"}, ExpressionId: "1"}},
	}

	t.Run("Happy case 1: stagingdb does not exist and expression is successfully verified", func(t *testing.T) {
		spannerMockClient := spannerclient.SpannerClientMock{
			SingleMock: func() spannerclient.ReadOnlyTransaction {
				return &spannerclient.ReadOnlyTransactionMock{
					QueryMock: func(ctx context.Context, stmt spanner.Statement) spannerclient.RowIterator {
						return &spannerclient.RowIteratorMock{
							NextMock: func() (*spanner.Row, error) {
								return nil, iterator.Done // Simulate successful query
							},
							StopMock: func() {},
						}
					},
				}
			},
		}
		spannerAdminMockClient := &spanneradmin.AdminClientMock{
			GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
				//mocks that an existing stagingDb does not exist
				return nil, fmt.Errorf("database not found")
			},
			CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
				return &spanneradmin.CreateDatabaseOperationMock{
					WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
				}, nil
			},
			DropDatabaseMock: func(ctx context.Context, req *databasepb.DropDatabaseRequest, opts ...gax.CallOption) error {
				return nil
			},
		}
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{SpannerClient: spannerMockClient, AdminClient: spannerAdminMockClient}}
		output := ev.VerifyExpressions(ctx, input)
		assert.Nil(t, output.Err)
		assert.Equal(t, len(output.ExpressionVerificationOutputList), 1)
		assert.True(t, output.ExpressionVerificationOutputList[0].Result)
	})

	t.Run("Happy case 2: Successfully dropped existing stagingDb and verified expressions", func(t *testing.T) {
		spannerMockClient := spannerclient.SpannerClientMock{
			SingleMock: func() spannerclient.ReadOnlyTransaction {
				return &spannerclient.ReadOnlyTransactionMock{
					QueryMock: func(ctx context.Context, stmt spanner.Statement) spannerclient.RowIterator {
						return &spannerclient.RowIteratorMock{
							NextMock: func() (*spanner.Row, error) {
								return nil, iterator.Done // Simulate successful query
							},
							StopMock: func() {},
						}
					},
				}
			},
		}
		spannerAdminMockClient := &spanneradmin.AdminClientMock{
			GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
				//mocks that an existing stagingDb exists
				return nil, nil
			},
			CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
				return &spanneradmin.CreateDatabaseOperationMock{
					WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
				}, nil
			},
			DropDatabaseMock: func(ctx context.Context, req *databasepb.DropDatabaseRequest, opts ...gax.CallOption) error {
				return nil
			},
		}
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{SpannerClient: spannerMockClient, AdminClient: spannerAdminMockClient}}
		output := ev.VerifyExpressions(ctx, input)
		assert.Nil(t, output.Err)
		assert.Equal(t, len(output.ExpressionVerificationOutputList), 1)
		assert.True(t, output.ExpressionVerificationOutputList[0].Result)
	})

	t.Run("Error in creating staging database", func(t *testing.T) {
		spannerAdminMockClient := &spanneradmin.AdminClientMock{
			GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
				//mocks that an existing stagingDb does not exist
				return nil, fmt.Errorf("database not found")
			},
			CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
				return &spanneradmin.CreateDatabaseOperationMock{
					//mocks error in creating database
					WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) {
						return nil, fmt.Errorf("unable to create database")
					},
				}, nil
			},
			DropDatabaseMock: func(ctx context.Context, req *databasepb.DropDatabaseRequest, opts ...gax.CallOption) error {
				//mocks being unable to drop the stagingDb
				return fmt.Errorf("unable to drop the database")
			},
		}
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{AdminClient: spannerAdminMockClient}}
		output := ev.VerifyExpressions(ctx, input)
		assert.NotNil(t, output.Err)
	})

	t.Run("Error in dropping existing database", func(t *testing.T) {
		spannerAdminMockClient := &spanneradmin.AdminClientMock{
			GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
				//mocks that an existing stagingDb exists
				return nil, nil
			},
			CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
				return &spanneradmin.CreateDatabaseOperationMock{
					WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
				}, nil
			},
			DropDatabaseMock: func(ctx context.Context, req *databasepb.DropDatabaseRequest, opts ...gax.CallOption) error {
				//mocks being unable to drop the stagingDb
				return fmt.Errorf("unable to drop the database")
			},
		}
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{AdminClient: spannerAdminMockClient}}
		output := ev.VerifyExpressions(ctx, input)
		assert.NotNil(t, output.Err)

	})

	t.Run("Invalid expression", func(t *testing.T) {
		spannerMockClient := spannerclient.SpannerClientMock{
			SingleMock: func() spannerclient.ReadOnlyTransaction {
				return &spannerclient.ReadOnlyTransactionMock{
					QueryMock: func(ctx context.Context, stmt spanner.Statement) spannerclient.RowIterator {
						return &spannerclient.RowIteratorMock{
							NextMock: func() (*spanner.Row, error) {
								return nil, fmt.Errorf("syntax error in query") // Simulate unsuccessful query
							},
							StopMock: func() {},
						}
					},
				}
			},
		}
		spannerAdminMockClient := &spanneradmin.AdminClientMock{
			GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
				//mocks that an existing stagingDb does not exist
				return nil, fmt.Errorf("database not found")
			},
			CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
				return &spanneradmin.CreateDatabaseOperationMock{
					WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
				}, nil
			},
			DropDatabaseMock: func(ctx context.Context, req *databasepb.DropDatabaseRequest, opts ...gax.CallOption) error {
				return nil
			},
		}
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{SpannerClient: spannerMockClient, AdminClient: spannerAdminMockClient}}
		output := ev.VerifyExpressions(ctx, input)
		assert.NotNil(t, output.Err)
		assert.Equal(t, len(output.ExpressionVerificationOutputList), 1)
		assert.False(t, output.ExpressionVerificationOutputList[0].Result)
	})

	t.Run("Missing Project", func(t *testing.T) {
		badInput := input
		badInput.Project = ""
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{}}
		output := ev.VerifyExpressions(ctx, badInput)
		assert.NotNil(t, output.Err)
		assert.True(t, strings.Contains(output.Err.Error(), "one of project, instance, conv or source is empty. These are mandatory fields"))
	})

	t.Run("Missing Instance", func(t *testing.T) {
		badInput := input
		badInput.Instance = ""
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{}}
		output := ev.VerifyExpressions(ctx, badInput)
		assert.NotNil(t, output.Err)
		assert.True(t, strings.Contains(output.Err.Error(), "one of project, instance, conv or source is empty. These are mandatory fields"))
	})

	t.Run("Nil conv", func(t *testing.T) {
		badInput := input
		badInput.Conv = nil
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{}}
		output := ev.VerifyExpressions(ctx, badInput)
		assert.NotNil(t, output.Err)
		assert.True(t, strings.Contains(output.Err.Error(), "one of project, instance, conv or source is empty. These are mandatory fields"))
	})

	t.Run("Missing Source", func(t *testing.T) {
		badInput := input
		badInput.Source = ""
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{}}
		output := ev.VerifyExpressions(ctx, badInput)
		assert.NotNil(t, output.Err)
		assert.True(t, strings.Contains(output.Err.Error(), "one of project, instance, conv or source is empty. These are mandatory fields"))
	})

	t.Run("Missing expressionId", func(t *testing.T) {
		badInput := input
		badInput.ExpressionDetailList[0].ExpressionId = ""
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{}}
		output := ev.VerifyExpressions(ctx, badInput)
		assert.NotNil(t, output.Err)
		assert.True(t, strings.Contains(output.Err.Error(), "one of expressionId, expression, type or referenceElement.Name is empty. These are mandatory fields"))
	})

	t.Run("Missing expression", func(t *testing.T) {
		badInput := input
		badInput.ExpressionDetailList[0].Expression = ""
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{}}
		output := ev.VerifyExpressions(ctx, badInput)
		assert.NotNil(t, output.Err)
		assert.True(t, strings.Contains(output.Err.Error(), "one of expressionId, expression, type or referenceElement.Name is empty. These are mandatory fields"))
	})

	t.Run("Missing expression type", func(t *testing.T) {
		badInput := input
		badInput.ExpressionDetailList[0].Type = ""
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{}}
		output := ev.VerifyExpressions(ctx, badInput)
		assert.NotNil(t, output.Err)
		assert.True(t, strings.Contains(output.Err.Error(), "one of expressionId, expression, type or referenceElement.Name is empty. These are mandatory fields"))
	})

	t.Run("Missing Reference Table Name", func(t *testing.T) {
		badInput := input
		badInput.ExpressionDetailList[0].ReferenceElement.Name = ""
		ev := &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spanneraccessor.SpannerAccessorImpl{}}
		output := ev.VerifyExpressions(ctx, badInput)
		assert.NotNil(t, output.Err)
		assert.True(t, strings.Contains(output.Err.Error(), "one of expressionId, expression, type or referenceElement.Name is empty. These are mandatory fields"))
	})

}

// ReadSessionFile reads a session JSON file and
// unmarshal it's content into *internal.Conv.
func ReadSessionFile(conv *internal.Conv, sessionJSON string) error {
	s, err := os.ReadFile(sessionJSON)
	if err != nil {
		return err
	}
	err = json.Unmarshal(s, &conv)
	if err != nil {
		return err
	}
	return nil
}
