package import_data

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
)

func TestCsvSchemaImpl_CreateSchema(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name              string
		source            CsvSchemaImpl
		dialect           string
		spannerClientMock spannerclient.SpannerClientMock
		adminClientMock   *spanneradmin.AdminClientMock
		wantErr           bool
	}{
		{
			name: "successful schema creation",
			source: CsvSchemaImpl{
				ProjectId:         "test-project",
				InstanceId:        "test-instance",
				DbName:            "test-db",
				TableName:         "test-table",
				SchemaUri:         "../test_data/basic_csv_schema.csv",
				CsvFieldDelimiter: ",",
			},
			dialect:           constants.DIALECT_GOOGLESQL,
			spannerClientMock: getSpannerClientMock(getDefaultRowIteratoMock()),
			adminClientMock:   getSpannerAdminClientMock(),
			wantErr:           false,
		},
		{
			name: "table exists",
			source: CsvSchemaImpl{
				ProjectId:         "test-project",
				InstanceId:        "test-instance",
				DbName:            "test-db",
				TableName:         "test-table",
				SchemaUri:         "../test_data/basic_csv_schema.csv",
				CsvFieldDelimiter: ",",
			},
			dialect: constants.DIALECT_GOOGLESQL,
			spannerClientMock: getSpannerClientMock(&spannerclient.RowIteratorMock{
				NextMock: func() (*spanner.Row, error) {
					return &spanner.Row{}, nil
				},
				StopMock: func() {},
			}),
			adminClientMock: getSpannerAdminClientMock(),
			wantErr:         false,
		},
		{
			name: "update database ddl error",
			source: CsvSchemaImpl{
				ProjectId:         "test-project",
				InstanceId:        "test-instance",
				DbName:            "test-db",
				TableName:         "test-table",
				SchemaUri:         "test-schema.csv",
				CsvFieldDelimiter: ",",
			},
			dialect:           constants.DIALECT_GOOGLESQL,
			spannerClientMock: getSpannerClientMock(getDefaultRowIteratoMock()),
			adminClientMock:   getSpannerAdminClientMock(),
			wantErr:           true,
		},
		// Add other test cases here...
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spannerAccessor := &spanneraccessor.SpannerAccessorImpl{SpannerClient: tt.spannerClientMock, AdminClient: tt.adminClientMock}
			if err := tt.source.CreateSchema(ctx, tt.dialect, spannerAccessor); (err != nil) != tt.wantErr {
				t.Errorf("CsvSchemaImpl.CreateSchema() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func getSpannerAdminClientMock() *spanneradmin.AdminClientMock {
	return &spanneradmin.AdminClientMock{
		UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
			return &spanneradmin.UpdateDatabaseDdlOperationMock{
				WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
			}, nil
		},
	}

}

func getSpannerClientMock(riMock *spannerclient.RowIteratorMock) spannerclient.SpannerClientMock {
	return spannerclient.SpannerClientMock{
		SingleMock: func() spannerclient.ReadOnlyTransaction {
			return &spannerclient.ReadOnlyTransactionMock{
				QueryMock: func(ctx context.Context, stmt spanner.Statement) spannerclient.RowIterator {
					return riMock
				},
			}
		},
	}
}

func getDefaultRowIteratoMock() *spannerclient.RowIteratorMock {
	return &spannerclient.RowIteratorMock{
		NextMock: func() (*spanner.Row, error) {
			return nil, iterator.Done
		},
		StopMock: func() {},
	}
}
