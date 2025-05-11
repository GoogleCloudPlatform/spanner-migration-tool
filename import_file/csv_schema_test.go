package import_file

import (
	"context"
	"errors"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/file_reader"
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
				ProjectId:  "test-project",
				InstanceId: "test-instance",
				DbName:     "test-db",
				TableName:  "test-table",
				SchemaUri:  "../test_data/basic_csv_schema.json",
			},
			dialect:           constants.DIALECT_GOOGLESQL,
			spannerClientMock: getSpannerClientMock(getDefaultRowIteratoMock()),
			adminClientMock:   getSpannerAdminClientMock(nil),
			wantErr:           false,
		},
		{
			name: "table exists",
			source: CsvSchemaImpl{
				ProjectId:  "test-project",
				InstanceId: "test-instance",
				DbName:     "test-db",
				TableName:  "test-table",
				SchemaUri:  "../test_data/basic_csv_schema.json",
			},
			dialect: constants.DIALECT_GOOGLESQL,
			spannerClientMock: getSpannerClientMock(&spannerclient.RowIteratorMock{
				NextMock: func() (*spanner.Row, error) {
					return &spanner.Row{}, nil
				},
				StopMock: func() {},
			}),
			adminClientMock: getSpannerAdminClientMock(nil),
			wantErr:         false,
		},
		{
			name: "update database ddl error",
			source: CsvSchemaImpl{
				ProjectId:  "test-project",
				InstanceId: "test-instance",
				DbName:     "test-db",
				TableName:  "test-table",
				SchemaUri:  "../test_data/basic_csv_schema.json",
			},
			dialect:           constants.DIALECT_GOOGLESQL,
			spannerClientMock: getSpannerClientMock(getDefaultRowIteratoMock()),
			adminClientMock:   getSpannerAdminClientMock(errors.New("update error")),
			wantErr:           true,
		},
		// Add other test cases here...
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spannerAccessor := &spanneraccessor.SpannerAccessorImpl{SpannerClient: tt.spannerClientMock, AdminClient: tt.adminClientMock}
			tt.source.SchemaFileReader, _ = file_reader.NewFileReader(ctx, tt.source.SchemaUri)
			if err := tt.source.CreateSchema(ctx, tt.dialect, spannerAccessor); (err != nil) != tt.wantErr {
				t.Errorf("CsvSchemaImpl.CreateSchema() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func getSpannerAdminClientMock(err error) *spanneradmin.AdminClientMock {
	return &spanneradmin.AdminClientMock{
		UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
			return &spanneradmin.UpdateDatabaseDdlOperationMock{
				WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
			}, err
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

func Test_getCreateTableStmt(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		colDef    []ColumnDefinition
		dialect   string
		want      string
	}{
		{
			name:      "standard create table",
			tableName: "test_table",
			colDef: []ColumnDefinition{
				{"col1", "INT64", true, 1},
				{"col2", "STRING(MAX)", false, 2},
			},
			dialect: constants.DIALECT_GOOGLESQL,
			want:    "CREATE TABLE `test_table` (\n`col1` INT64 NOT NULL ,`col2` STRING(MAX)) PRIMARY KEY (`col1`,`col2`)",
		},
		{
			name:      "Postgres Dialect",
			tableName: "test_table",
			colDef: []ColumnDefinition{
				{"col1", "INT64", true, 1},
				{"col2", "STRING(MAX)", false, 2},
			},
			dialect: constants.DIALECT_POSTGRESQL,
			want:    "CREATE TABLE `test_table` (\n`col1` INT64 NOT NULL ,`col2` STRING(MAX)) PRIMARY KEY (`col1`,`col2`)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getCreateTableStmt(tt.tableName, tt.colDef, tt.dialect)
			if got != tt.want {
				t.Errorf("getCreateTableStmt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_printColumnDef(t *testing.T) {
	tests := []struct {
		name string
		c    ColumnDefinition
		want string
	}{
		{
			name: "not null",
			c:    ColumnDefinition{Name: "col1", Type: "INT64", NotNull: true},
			want: "`col1` INT64 NOT NULL ",
		},
		{
			name: "nullable",
			c:    ColumnDefinition{Name: "col2", Type: "STRING(MAX)", NotNull: false},
			want: "`col2` STRING(MAX)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := printColumnDef(tt.c); got != tt.want {
				t.Errorf("printColumnDef() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_quote(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want string
	}{
		{
			name: "quote string",
			s:    "test_column",
			want: "`test_column`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quote(tt.s); got != tt.want {
				t.Errorf("quote() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringToBool(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{
			name: "true",
			s:    "true",
			want: true,
		},
		{
			name: "false",
			s:    "false",
			want: false,
		},
		{
			name: "empty",
			s:    "",
			want: false,
		},
		{
			name: "invalid",
			s:    "invalid",
			want: false,
		},
		{
			name: "mixed case true",
			s:    "TrUe",
			want: true,
		},
		{
			name: "whitespace",
			s:    "  true  ",
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StringToBool(tt.s); got != tt.want {
				t.Errorf("StringToBool() = %v, want %v", got, tt.want)
			}
		})
	}
}
