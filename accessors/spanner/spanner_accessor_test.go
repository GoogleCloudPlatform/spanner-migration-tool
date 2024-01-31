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

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
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
	spA := SpannerAccessorImpl{}
	for _, tc := range testCases {
		got, err := spA.GetDatabaseDialect(ctx, &tc.acm, "testUri")
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
	spA := SpannerAccessorImpl{}
	for _, tc := range testCases {
		got, err := spA.CheckExistingDb(ctx, &tc.acm, "testUri")
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
	spA := SpannerAccessorImpl{}
	for _, tc := range testCases {
		err := spA.CreateEmptyDatabase(ctx, &tc.acm, "projects/test-project/instances/test-instance/databases/mydb")
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
	spA := SpannerAccessorImpl{}
	for _, tc := range testCases {
		err := spA.CreateChangeStream(ctx, &tc.acm, "my-changestream", "projects/test-project/instances/test-instance/databases/mydb")
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
	spA := SpannerAccessorImpl{}
	for _, tc := range testCases {
		got, err := spA.GetSpannerLeaderLocation(ctx, &tc.iac, "projects/test-project/instances/test-instance")
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}
}
