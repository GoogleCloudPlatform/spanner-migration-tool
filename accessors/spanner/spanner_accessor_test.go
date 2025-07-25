// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package spanneraccessor

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
	"google.golang.org/api/iterator"
)

const TablePerDbError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Cannot add table table_999: too many tables (limit 5000)."
const ColumnPerTableError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Table LargeTable has too many columns; the limit is 1024."
const InterleaveDepthError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Table level8 is too deeply nested; the limit is 8 tables."
const ColumnKeyPerTableError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = Table cart_extended has too many keys (17); the limit is 16."
const TableNameError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = table name not valid: CustomerOrderTransactionHistoryRecords2023ForAnalysisAndArchivingIncludingSensitiveDataAndSecureProcessingProceduressrdfgdnhydbtsfvfs."
const ColumnNameError = "can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = Column name not valid: large_column.CustomerOrderTransactionHistoryRecords2023ForAnalysisAndArchivingIncludingSensitiveDataAndSecureProcessingProceduressrdfgdnhydbtsfvfs."

const TablePerDbExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Cannot add table table_999: too many tables (limit 5000)."
const ColumnPerTableExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Table LargeTable has too many columns; the limit is 1024."
const InterleaveDepthExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = FailedPrecondition desc = Table level8 is too deeply nested; the limit is 8 tables."
const ColumnKeyPerTableExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = Table cart_extended has too many keys (17); the limit is 16."
const TableNameExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = table name not valid: CustomerOrderTransactionHistoryRecords2023ForAnalysisAndArchivingIncludingSensitiveDataAndSecureProcessingProceduressrdfgdnhydbtsfvfs."
const ColumnNameExpectError = "can't build CreateDatabaseRequest: can't create/update database: can't create database: can't build CreateDatabaseRequest: rpc error: code = InvalidArgument desc = Column name not valid: large_column.CustomerOrderTransactionHistoryRecords2023ForAnalysisAndArchivingIncludingSensitiveDataAndSecureProcessingProceduressrdfgdnhydbtsfvfs."

// GenerateRandomString generates a random string of a specified length.
func GenerateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var seededRand = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	randomString := make([]byte, length)
	for i := range randomString {
		randomString[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(randomString)
}

// GenerateColumnDefsForTable generates an array of column definitions for a table
// based on the specified length.
func GenerateColumnDefsForTable(count int) map[string]ddl.ColumnDef {
	colums := make(map[string]ddl.ColumnDef)
	for i := 1; i <= count; i++ {
		colName := fmt.Sprintf("col%d", i)
		colId := fmt.Sprintf("c%d", i)
		colums[colId] = ddl.ColumnDef{Name: colName, Id: colId, T: ddl.Type{Name: ddl.Int64}}
	}
	return colums
}

// GenerateColIds generates an array of column ids for a table
// based on the specified length.
func GenerateColIds(count int) []string {
	var colIds []string
	for i := 1; i <= count; i++ {
		colId := fmt.Sprintf("c%d", i)
		colIds = append(colIds, colId)
	}
	return colIds
}

// GeneratePrimaryColIds generates an array of primary columns ids for a table
// based on the specified length.
func GeneratePrimaryColIds(count int) []ddl.IndexKey {
	var primaryKeys []ddl.IndexKey
	for i := 1; i <= count; i++ {
		colId := fmt.Sprintf("c%d", i)
		primaryKeys = append(primaryKeys, ddl.IndexKey{ColId: colId})
	}
	return primaryKeys
}

// GenerateSpSchema generates a schema consisting of a specified number of tables.
// Each table in the schema is defined by unique properties including identifiers,
// primary keys, columns, and foreign keys, which are set based on the
// iteration index and relationships with other tables.
func GenerateSpSchema(count int) map[string]ddl.CreateTable {
	spschema := make(map[string]ddl.CreateTable)
	for i := 1; i <= count; i++ {
		tableId := fmt.Sprintf("t%d", i)
		tableName := fmt.Sprintf("table%d", i)
		referTableId := fmt.Sprintf("t%d", i-1)
		spschema[tableId] = ddl.CreateTable{
			Name:        "table1",
			Id:          tableName,
			PrimaryKeys: GeneratePrimaryColIds(i),
			ColIds:      GenerateColIds(i + 1),
			ColDefs:     GenerateColumnDefsForTable(i + 1),
			ForeignKeys: GenerateForeignKeys(i-1, referTableId),
		}
	}

	return spschema
}

// GenerateForeignKeys generates an array of foreign keys for a table
// based on the specified length.
func GenerateForeignKeys(count int, referTableId string) []ddl.Foreignkey {
	if count != 0 {
		var colIds []string
		var referColumnIds []string
		for i := 1; i <= count; i++ {
			colId := fmt.Sprintf("c%d", i)
			colIds = append(colIds, colId)
			referColumnIds = append(referColumnIds, colId)
		}
		fname := fmt.Sprintf("level%d_ibfk_1", count)
		return []ddl.Foreignkey{{
			Name:           fname,
			ColIds:         colIds,
			ReferColumnIds: referColumnIds,
			ReferTableId:   referTableId,
			Id:             GenerateRandomString(2),
			OnDelete:       "NO ACTION",
			OnUpdate:       "NO ACTION",
		}}
	} else {
		return nil
	}

}

// GenerateTables generates an array of tables
// based on the specified length.
func GenerateTables(count int) ddl.Schema {
	tables := make(ddl.Schema)

	for i := 1; i <= count; i++ {
		tableName := fmt.Sprintf("table%d", i)
		tableId := fmt.Sprintf("t%d", i)
		tables[tableId] = ddl.CreateTable{Name: tableName, Id: tableId, PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}}, ColIds: []string{"c1"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1": {Name: "col1", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
			}}
	}
	return tables
}

func init() {
	logger.Log = zap.NewNop()
}

func TestMain(m *testing.M) {
	res := m.Run()
	os.Exit(res)
}

func TestSpannerAccessorImpl_GetDatabaseDialect(t *testing.T) {
	testCases := []struct {
		name        string
		acm         spanneradmin.AdminClientMock
		expectError bool
		want        string
	}{
		{
			name: "Basic",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
			},
			expectError: false,
			want:        "google_standard_sql",
		},
		{
			name: "Pg Dialect",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_POSTGRESQL}, nil
				},
			},
			expectError: false,
			want:        "postgresql",
		},
		{
			name: "Unspecified Dialect",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED}, nil
				},
			},
			expectError: false,
			want:        "database_dialect_unspecified",
		},
		{
			name: "Error case",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return nil, fmt.Errorf("test-error")
				},
			},
			expectError: true,
			want:        "",
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		got, err := spA.GetDatabaseDialect(ctx, "testUri")
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}
}

func TestSpannerAccessorImpl_CheckExistingDb(t *testing.T) {
	testCases := []struct {
		name        string
		acm         spanneradmin.AdminClientMock
		expectError bool
		want        bool
	}{
		{
			name: "Basic",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return nil, nil
				},
			},
			expectError: false,
			want:        true,
		},
		{
			name: "Database not found error",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return nil, fmt.Errorf("database not found")
				},
			},
			expectError: false,
			want:        false,
		},
		{
			name: "Could not get db info",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return nil, fmt.Errorf("failed to connect")
				},
			},
			expectError: true,
			want:        false,
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		got, err := spA.CheckExistingDb(ctx, "testUri")
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}
}

func TestSpannerAccessorImpl_CreateEmptyDatabase(t *testing.T) {
	testCases := []struct {
		name        string
		acm         spanneradmin.AdminClientMock
		expectError bool
		want        string
	}{
		{
			name: "Basic",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectError: false,
		},
		{
			name: "Create database returns error",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf("test error")
				},
			},
			expectError: true,
		},
		{
			name: "Wait returns error",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) {
							return nil, fmt.Errorf("test error")
						},
					}, nil
				},
			},
			expectError: true,
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		err := spA.CreateEmptyDatabase(ctx, "projects/test-project/instances/test-instance/databases/mydb", constants.DIALECT_GOOGLESQL)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestSpannerAccessorImpl_CreateChangeStream(t *testing.T) {
	testCases := []struct {
		name        string
		acm         spanneradmin.AdminClientMock
		expectError bool
		want        string
	}{
		{
			name: "Basic",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
					}, nil
				},
			},
			expectError: false,
		},
		{
			name: "Update database ddl returns error",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return nil, fmt.Errorf("test error")
				},
			},
			expectError: true,
		},
		{
			name: "Wait returns error",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error {
							return fmt.Errorf("test error")
						},
					}, nil
				},
			},
			expectError: true,
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		err := spA.CreateChangeStream(ctx, "my-changestream", "projects/test-project/instances/test-instance/databases/mydb")
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestSpannerAccessorImpl_GetSpannerLeaderLocation(t *testing.T) {
	testCases := []struct {
		name        string
		iac         spinstanceadmin.InstanceAdminClientMock
		expectError bool
		want        string
	}{
		{
			name: "Basic",
			iac: spinstanceadmin.InstanceAdminClientMock{
				GetInstanceMock: func(ctx context.Context, req *instancepb.GetInstanceRequest, opts ...gax.CallOption) (*instancepb.Instance, error) {
					return &instancepb.Instance{Config: "projects/test-project/instanceConfigs/test-config"}, nil
				},
				GetInstanceConfigMock: func(ctx context.Context, req *instancepb.GetInstanceConfigRequest, opts ...gax.CallOption) (*instancepb.InstanceConfig, error) {
					return &instancepb.InstanceConfig{Replicas: []*instancepb.ReplicaInfo{
						&instancepb.ReplicaInfo{
							Location:              "us-east1",
							DefaultLeaderLocation: false,
						},
						&instancepb.ReplicaInfo{
							Location:              "india1",
							DefaultLeaderLocation: true,
						},
						&instancepb.ReplicaInfo{
							Location:              "europe2",
							DefaultLeaderLocation: false,
						},
					}}, nil
				},
			},
			expectError: false,
			want:        "india1",
		},
		{
			name: "GetInstanceMock returns error",
			iac: spinstanceadmin.InstanceAdminClientMock{
				GetInstanceMock: func(ctx context.Context, req *instancepb.GetInstanceRequest, opts ...gax.CallOption) (*instancepb.Instance, error) {
					return nil, fmt.Errorf("test-error")
				},
				GetInstanceConfigMock: func(ctx context.Context, req *instancepb.GetInstanceConfigRequest, opts ...gax.CallOption) (*instancepb.InstanceConfig, error) {
					return nil, nil
				},
			},
			expectError: true,
			want:        "",
		},
		{
			name: "GetInstanceConfigMock returns error",
			iac: spinstanceadmin.InstanceAdminClientMock{
				GetInstanceMock: func(ctx context.Context, req *instancepb.GetInstanceRequest, opts ...gax.CallOption) (*instancepb.Instance, error) {
					return &instancepb.Instance{Config: "projects/test-project/instanceConfigs/test-config"}, nil
				},
				GetInstanceConfigMock: func(ctx context.Context, req *instancepb.GetInstanceConfigRequest, opts ...gax.CallOption) (*instancepb.InstanceConfig, error) {
					return nil, fmt.Errorf("test-error")
				},
			},
			expectError: true,
			want:        "",
		},
		{
			name: "No leader found returns error",
			iac: spinstanceadmin.InstanceAdminClientMock{
				GetInstanceMock: func(ctx context.Context, req *instancepb.GetInstanceRequest, opts ...gax.CallOption) (*instancepb.Instance, error) {
					return &instancepb.Instance{Config: "projects/test-project/instanceConfigs/test-config"}, nil
				},
				GetInstanceConfigMock: func(ctx context.Context, req *instancepb.GetInstanceConfigRequest, opts ...gax.CallOption) (*instancepb.InstanceConfig, error) {
					return &instancepb.InstanceConfig{Replicas: []*instancepb.ReplicaInfo{
						&instancepb.ReplicaInfo{
							Location:              "us-east1",
							DefaultLeaderLocation: false,
						},
						&instancepb.ReplicaInfo{
							Location:              "india1",
							DefaultLeaderLocation: false,
						},
						&instancepb.ReplicaInfo{
							Location:              "europe2",
							DefaultLeaderLocation: false,
						},
					}}, nil
				},
			},
			expectError: true,
			want:        "",
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		spA := SpannerAccessorImpl{InstanceClient: &tc.iac}
		got, err := spA.GetSpannerLeaderLocation(ctx, "projects/test-project/instances/test-instance")
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}
}

func TestSpannerAccessorImpl_CreateDatabase(t *testing.T) {
	testCases := []struct {
		name          string
		acm           spanneradmin.AdminClientMock
		dialect       string
		migrationType string
		expectError   bool
	}{
		{
			name: "GoogleSql Dataflow",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectError:   false,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
		},
		{
			name: "Pg Dataflow",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
					}, nil
				},
			},
			expectError:   false,
			dialect:       "postgresql",
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql bulk",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectError:   false,
			dialect:       "google_standard_sql",
			migrationType: "bulk",
		},
		{
			name: "Pg bulk",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
					}, nil
				},
			},
			expectError:   false,
			dialect:       "postgresql",
			migrationType: "bulk",
		},
		{
			name: "GoogleSql Dataflow create database error",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf("error")
				},
			},
			expectError:   true,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
		},
		{
			name: "Pg Dataflow update error",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return nil, fmt.Errorf("error")
				},
			},
			expectError:   true,
			dialect:       "postgresql",
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql Dataflow operation error",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) {
							return nil, fmt.Errorf("error")
						},
					}, nil
				},
			},
			expectError:   true,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		dbURI := "projects/project-id/instances/instance-id/databases/database-id"
		conv := internal.MakeConv()
		conv.SpDialect = tc.dialect
		conv.SpSchema = map[string]ddl.CreateTable{
			"t1": {
				Name:        "table1",
				Id:          "t1",
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
				ColIds:      []string{"c1"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "col1", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
				},
			},
		}
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		err := spA.CreateDatabase(ctx, dbURI, conv, "", tc.migrationType)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestSpannerAccessorImpl_CreateDatabase_exceeds_and_hit_limits(t *testing.T) {
	testCases := []struct {
		name             string
		acm              spanneradmin.AdminClientMock
		dialect          string
		migrationType    string
		SpSchema         ddl.Schema
		expectedErrorMsg string
		expectError      bool
	}{
		{
			name: "GoogleSql with table more than 5000",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf(TablePerDbError)
				},
			},
			expectedErrorMsg: TablePerDbExpectError,
			expectError:      true,
			dialect:          "google_standard_sql",
			SpSchema:         GenerateTables(5005),
			migrationType:    "dataflow",
		},
		{
			name: "GoogleSql with 5000 tables",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectedErrorMsg: "",
			expectError:      false,
			dialect:          "google_standard_sql",
			SpSchema:         GenerateTables(5000),
			migrationType:    "dataflow",
		},
		{
			name: "GoogleSql with table has more than 1024 columns",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf(ColumnPerTableError)
				},
			},
			expectedErrorMsg: ColumnPerTableExpectError,
			expectError:      true,
			dialect:          "google_standard_sql",
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:        "table 1",
					Id:          "t1",
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					ColIds:      GenerateColIds(1030),
					ColDefs:     GenerateColumnDefsForTable(1030),
				},
			},
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql with table has 1024 columns",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectedErrorMsg: "",
			expectError:      false,
			dialect:          "google_standard_sql",
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:        "table 1",
					Id:          "t1",
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					ColIds:      GenerateColIds(1024),
					ColDefs:     GenerateColumnDefsForTable(1024),
				},
			},
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql with table interleaving depth is 7",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectedErrorMsg: "",
			expectError:      false,
			dialect:          "google_standard_sql",
			SpSchema:         GenerateSpSchema(7),
			migrationType:    "dataflow",
		},
		{
			name: "GoogleSql with table interleaving depth is more than 7",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf(InterleaveDepthError)
				},
			},
			expectedErrorMsg: InterleaveDepthExpectError,
			expectError:      true,
			dialect:          "google_standard_sql",
			SpSchema:         GenerateSpSchema(8),
			migrationType:    "dataflow",
		},
		{
			name: "GoogleSql with table has 16 columns as key",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectError:      false,
			expectedErrorMsg: "",
			dialect:          "google_standard_sql",
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:        "table 1",
					Id:          "t1",
					PrimaryKeys: GeneratePrimaryColIds(16),
					ColIds:      GenerateColIds(18),
					ColDefs:     GenerateColumnDefsForTable(18),
				},
			},
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql with table has more than 16 columns as key",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf(ColumnKeyPerTableError)
				},
			},
			expectedErrorMsg: ColumnKeyPerTableExpectError,
			expectError:      true,
			dialect:          "google_standard_sql",
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:        "table 1",
					Id:          "t1",
					PrimaryKeys: GeneratePrimaryColIds(17),
					ColIds:      GenerateColIds(18),
					ColDefs:     GenerateColumnDefsForTable(18),
				},
			},
			migrationType: "dataflow",
		},

		{
			name: "GoogleSql with table name more than 128 character",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf(TableNameError)
				},
			},
			expectedErrorMsg: TableNameExpectError,
			expectError:      true,
			dialect:          "google_standard_sql",
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:        GenerateRandomString(130),
					Id:          "t1",
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					ColIds:      []string{"c1"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "col1", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql with table name has 128 character",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectError:      false,
			expectedErrorMsg: "",
			dialect:          "google_standard_sql",
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:        GenerateRandomString(128),
					Id:          "t1",
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					ColIds:      []string{"c1"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "col1", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql with column name more than 128 character",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf(ColumnNameError)
				},
			},
			expectedErrorMsg: ColumnNameExpectError,
			expectError:      true,
			dialect:          "google_standard_sql",
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:        "table 1",
					Id:          "t1",
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					ColIds:      []string{"c1"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: GenerateRandomString(130), Id: "c1", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			migrationType: "dataflow",
		},

		{
			name: "GoogleSql with column name has 128 character",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
			},
			expectedErrorMsg: "",
			expectError:      false,
			dialect:          "google_standard_sql",
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Name:        "table 1",
					Id:          "t1",
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					ColIds:      []string{"c1"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: GenerateRandomString(128), Id: "c1", T: ddl.Type{Name: ddl.Int64}},
					},
				},
			},
			migrationType: "dataflow",
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		dbURI := "projects/project-id/instances/instance-id/databases/database-id"
		conv := internal.MakeConv()
		conv.SpDialect = tc.dialect
		conv.SpSchema = tc.SpSchema
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		err := spA.CreateDatabase(ctx, dbURI, conv, "", tc.migrationType)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		if err != nil {
			assert.Equal(t, tc.expectedErrorMsg, err.Error(), tc.name)
		}
	}
}

func TestSpannerAccessorImpl_CreateOrUpdateDatabase(t *testing.T) {
	testCases := []struct {
		name                    string
		acm                     spanneradmin.AdminClientMock
		dialect                 string
		migrationType           string
		expectError             bool
		tablesExistingOnSpanner []string
	}{
		{
			name: "GoogleSql Dataflow db does not exist",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return nil, fmt.Errorf("database not found")
				},
			},
			expectError:             false,
			dialect:                 "google_standard_sql",
			migrationType:           "dataflow",
			tablesExistingOnSpanner: []string{},
		},
		{
			name: "GoogleSql Dataflow db exists",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
			},
			expectError:             true,
			dialect:                 "google_standard_sql",
			migrationType:           "dataflow",
			tablesExistingOnSpanner: []string{},
		},
		{
			name: "Postgres Dataflow db exists",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
			},
			expectError:             true,
			dialect:                 "google_standard_sql",
			migrationType:           "dataflow",
			tablesExistingOnSpanner: []string{},
		},
		{
			name: "Postgres bulk db exists",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
					}, nil
				},
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
			},
			expectError:             false,
			dialect:                 "google_standard_sql",
			migrationType:           "bulk",
			tablesExistingOnSpanner: []string{},
		},
		{
			name: "GoogleSql Dataflow db get database error",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return nil, fmt.Errorf("error")
				},
			},
			expectError:             true,
			dialect:                 "google_standard_sql",
			migrationType:           "dataflow",
			tablesExistingOnSpanner: []string{},
		},
		{
			name: "GoogleSql Dataflow db ddl statements nto empty",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
			},
			expectError:             true,
			dialect:                 "google_standard_sql",
			migrationType:           "dataflow",
			tablesExistingOnSpanner: []string{"table_a"},
		},
		{
			name: "Postgres bulk db exists update ddl error",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return nil, fmt.Errorf("error")
				},
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
			},
			expectError:             true,
			dialect:                 "google_standard_sql",
			migrationType:           "bulk",
			tablesExistingOnSpanner: []string{},
		},
		{
			name: "GoogleSql Dataflow db does not exist create error",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return nil, fmt.Errorf("error")
				},
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return nil, fmt.Errorf("database not found")
				},
			},
			expectError:             true,
			dialect:                 "google_standard_sql",
			migrationType:           "dataflow",
			tablesExistingOnSpanner: []string{},
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		dbURI := "projects/project-id/instances/instance-id/databases/database-id"
		conv := internal.MakeConv()
		conv.SpSchema["t1"] = ddl.CreateTable{
			Name:        "table_a",
			ColIds:      []string{"c1"},
			ColDefs:     map[string]ddl.ColumnDef{"c1": {Name: "col1", T: ddl.Type{Name: ddl.String, Len: 10}}},
			PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			Id:          "t1",
		}
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		err := spA.CreateOrUpdateDatabase(ctx, dbURI, "", conv, tc.migrationType, tc.tablesExistingOnSpanner)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestSpannerAccessorImpl_UpdateDatabase(t *testing.T) {
	testCases := []struct {
		name        string
		acm         spanneradmin.AdminClientMock
		expectError bool
	}{
		{
			name: "Update Database successful",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
					}, nil
				},
			},
			expectError: false,
		},
		{
			name: "Update Database request error",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return nil, fmt.Errorf("Error")
				},
			},
			expectError: true,
		},
		{
			name: "Update Database operation error",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return fmt.Errorf("error") },
					}, nil
				},
			},
			expectError: true,
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		dbURI := "projects/project-id/instances/instance-id/databases/database-id"
		conv := internal.MakeConv()
		conv.SpSchema["t1"] = ddl.CreateTable{
			Name:        "table_a",
			ColIds:      []string{"c1"},
			ColDefs:     map[string]ddl.ColumnDef{"c1": {Name: "col1", T: ddl.Type{Name: ddl.String, Len: 10}}},
			PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			Id:          "t1",
		}
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		err := spA.UpdateDatabase(ctx, dbURI, conv, "")
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestSpannerAccessorImpl_UpdateDDLForeignKey(t *testing.T) {
	schemaWithStatements := map[string]ddl.CreateTable{
		"table_id": {
			Name: "table1",
			Id:   "table_id",
		},
		"table_id2": {
			Name:        "table2",
			Id:          "table_id2",
			ParentTable: ddl.InterleavedParent{Id: "table1", OnDelete: constants.FK_CASCADE, InterleaveType: "IN PARENT"},
			ForeignKeys: []ddl.Foreignkey{
				{
					Name:           "fk",
					ColIds:         []string{"columns"},
					ReferTableId:   "table1",
					ReferColumnIds: []string{"column"},
					Id:             "table_id",
				},
			},
		},
		"table_id3": {
			Name:        "table3",
			Id:          "table_id3",
			ParentTable: ddl.InterleavedParent{Id: "table1", OnDelete: constants.FK_NO_ACTION, InterleaveType: "IN PARENT"},
			ForeignKeys: []ddl.Foreignkey{
				{
					Name:           "fk2",
					ColIds:         []string{"columns"},
					ReferTableId:   "table1",
					ReferColumnIds: []string{"column"},
					Id:             "table_id",
				},
			},
		},
	}
	testCases := []struct {
		name          string
		acm           spanneradmin.AdminClientMock
		dialect       string
		migrationType string
		spSchema      ddl.Schema
	}{
		{
			name: "Update DDL ForeignKey successful pg dataflow",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
					}, nil
				},
			},
			dialect:       "postgresql",
			spSchema:      schemaWithStatements,
			migrationType: "dataflow",
		},
		{
			name: "Update DDL ForeignKey successful pg dataflow no statement",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
					}, nil
				},
			},
			dialect:       "postgresql",
			spSchema:      map[string]ddl.CreateTable{},
			migrationType: "dataflow",
		},
		{
			name: "Update DDL ForeignKey successful google_standard_sql",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return nil },
					}, nil
				},
			},
			dialect:       "google_standard_sql",
			spSchema:      schemaWithStatements,
			migrationType: "dataflow",
		},
		{
			name: "Update DDL ForeignKey update database error",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return nil, fmt.Errorf("error")
				},
			},
			dialect:       "postgresql",
			spSchema:      schemaWithStatements,
			migrationType: "dataflow",
		},
		{
			name: "Update DDL ForeignKey operation error",
			acm: spanneradmin.AdminClientMock{
				UpdateDatabaseDdlMock: func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (spanneradmin.UpdateDatabaseDdlOperation, error) {
					return &spanneradmin.UpdateDatabaseDdlOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) error { return fmt.Errorf("error") },
					}, nil
				},
			},
			dialect:       "postgresql",
			spSchema:      schemaWithStatements,
			migrationType: "dataflow",
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		dbURI := "projects/project-id/instances/instance-id/databases/database-id"
		conv := internal.MakeConv()
		conv.SpDialect = tc.dialect
		conv.SpSchema = tc.spSchema
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		spA.UpdateDDLForeignKeys(ctx, dbURI, conv, "", tc.migrationType)
	}
}

func TestValidateDML(t *testing.T) {
	ctx := context.Background()
	t.Run("Valid DML", func(t *testing.T) {
		mockClient := spannerclient.SpannerClientMock{
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
		spannerAccessor := &SpannerAccessorImpl{SpannerClient: mockClient}
		isValid, err := spannerAccessor.ValidateDML(ctx, "SELECT 1")
		assert.True(t, isValid)
		assert.Nil(t, err)
	})

	t.Run("Invalid DML", func(t *testing.T) {
		mockClient := spannerclient.SpannerClientMock{
			SingleMock: func() spannerclient.ReadOnlyTransaction {
				return &spannerclient.ReadOnlyTransactionMock{
					QueryMock: func(ctx context.Context, stmt spanner.Statement) spannerclient.RowIterator {
						return &spannerclient.RowIteratorMock{
							NextMock: func() (*spanner.Row, error) {
								return nil, fmt.Errorf("invalid DML") // Simulate a DML error
							},
							StopMock: func() {},
						}
					},
				}
			},
		}
		spannerAccessor := &SpannerAccessorImpl{SpannerClient: mockClient}
		isValid, err := spannerAccessor.ValidateDML(ctx, "INVALID DML")
		assert.False(t, isValid)
		assert.NotNil(t, err) // Expect an error
	})
}

func TestFetchCreateDatabaseStatement(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		databaseName string
		want         string
	}{
		{
			name:         "PostgreSQL dialect",
			dialect:      constants.DIALECT_POSTGRESQL,
			databaseName: "mypgdb",
			want:         `CREATE DATABASE "mypgdb"`,
		},
		{
			name:         "Google Standard SQL dialect",
			dialect:      constants.DIALECT_GOOGLESQL,
			databaseName: "mydb",
			want:         "CREATE DATABASE `mydb`",
		},
		{
			name:         "Empty dialect (defaults to Google Standard SQL)",
			dialect:      "",
			databaseName: "anotherdb",
			want:         "CREATE DATABASE `anotherdb`",
		},
		{
			name:         "Database name with special characters for GoogleSQL",
			dialect:      constants.DIALECT_GOOGLESQL,
			databaseName: "my-db_123",
			want:         "CREATE DATABASE `my-db_123`",
		},
		{
			name:         "Database name with special characters for PostgreSQL",
			dialect:      constants.DIALECT_POSTGRESQL,
			databaseName: "my-pg_db",
			want:         `CREATE DATABASE "my-pg_db"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fetchCreateDatabaseStatement(tt.dialect, tt.databaseName)
			assert.Equal(t, tt.want, got)
		})
	}
}
