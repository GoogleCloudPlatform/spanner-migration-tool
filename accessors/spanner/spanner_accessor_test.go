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
	"google.golang.org/api/iterator"
)

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
		err := spA.CreateEmptyDatabase(ctx, "projects/test-project/instances/test-instance/databases/mydb")
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
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		err := spA.CreateDatabase(ctx, dbURI, conv, "", tc.migrationType)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestSpannerAccessorImpl_CreateOrUpdateDatabase(t *testing.T) {
	testCases := []struct {
		name          string
		acm           spanneradmin.AdminClientMock
		dialect       string
		migrationType string
		expectError   bool
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
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return &databasepb.GetDatabaseDdlResponse{}, nil
				},
			},
			expectError:   false,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
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
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return &databasepb.GetDatabaseDdlResponse{}, nil
				},
			},
			expectError:   true,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
		},
		{
			name: "Postgres Dataflow db exists",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return &databasepb.GetDatabaseDdlResponse{}, nil
				},
			},
			expectError:   true,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
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
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return &databasepb.GetDatabaseDdlResponse{}, nil
				},
			},
			expectError:   false,
			dialect:       "google_standard_sql",
			migrationType: "bulk",
		},
		{
			name: "GoogleSql Dataflow db get database error",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return nil, fmt.Errorf("error")
				},
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return &databasepb.GetDatabaseDdlResponse{}, nil
				},
			},
			expectError:   true,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql Dataflow db ddl statements nto empty",
			acm: spanneradmin.AdminClientMock{
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return &databasepb.GetDatabaseDdlResponse{Statements: []string{"string"}}, nil
				},
			},
			expectError:   true,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
		},
		{
			name: "GoogleSql Dataflow db get database ddl error",
			acm: spanneradmin.AdminClientMock{
				CreateDatabaseMock: func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (spanneradmin.CreateDatabaseOperation, error) {
					return &spanneradmin.CreateDatabaseOperationMock{
						WaitMock: func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) { return nil, nil },
					}, nil
				},
				GetDatabaseMock: func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
					return &databasepb.Database{DatabaseDialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
				},
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return nil, fmt.Errorf("error")
				},
			},
			expectError:   true,
			dialect:       "google_standard_sql",
			migrationType: "dataflow",
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
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return &databasepb.GetDatabaseDdlResponse{}, nil
				},
			},
			expectError:   true,
			dialect:       "google_standard_sql",
			migrationType: "bulk",
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
				GetDatabaseDdlMock: func(ctx context.Context, req *databasepb.GetDatabaseDdlRequest, opts ...gax.CallOption) (*databasepb.GetDatabaseDdlResponse, error) {
					return &databasepb.GetDatabaseDdlResponse{}, nil
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
		spA := SpannerAccessorImpl{AdminClient: &tc.acm}
		err := spA.CreateOrUpdateDatabase(ctx, dbURI, "", conv, tc.migrationType)
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
			ParentTable: ddl.InterleavedParent{Id: "table1", OnDelete: constants.FK_CASCADE},
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
			ParentTable: ddl.InterleavedParent{Id: "table1", OnDelete: constants.FK_NO_ACTION},
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
