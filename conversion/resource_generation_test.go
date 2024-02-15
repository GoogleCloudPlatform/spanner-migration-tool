// // Copyright 2023 Google LLC
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //      http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

package conversion

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"cloud.google.com/go/datastream/apiv1/datastreampb"
	datastreamclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/datastream"
	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
	datastream_accessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/datastream"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	storageaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestValidateResourceGeneration(t *testing.T) {
	vrg := ValidateResourcesImpl{
		SpInstanceAdmin: &spinstanceadmin.InstanceAdminClientMock{},
	}
	ctx := context.Background()
	sourceProfile := profiles.SourceProfile{}
	conv := internal.MakeConv()
	testCases := []struct{
		name              	  string
		sam					  spanneraccessor.SpannerAccessorMock
		createResourcesError  error
		expectError 		  bool
	}{
		{
			name : "Basic",
			sam : spanneraccessor.SpannerAccessorMock{
				GetSpannerLeaderLocationMock: func(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error){
					return "region", nil
				},
			},
			createResourcesError: nil,
			expectError: false,
		},
		{
			name : "Spanner Region error",
			sam : spanneraccessor.SpannerAccessorMock{
				GetSpannerLeaderLocationMock: func(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error){
					return "", fmt.Errorf("error")
				},
			},
			createResourcesError: nil,
			expectError: true,
		},
		{
			name : "create resources error",
			sam : spanneraccessor.SpannerAccessorMock{
				GetSpannerLeaderLocationMock: func(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error){
					return "region", nil
				},
			},
			createResourcesError: fmt.Errorf("error"),
			expectError: true,
		},
	}
	for _, tc := range testCases {
		vrg.SpAcc = &tc.sam
		var m = MockCreateResources{}
		m.On("CreateResourcesForShardedMigration", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.createResourcesError)
		vrg.CreateResources = &m
		err := vrg.ValidateResourceGeneration(ctx, "project-id", "instance-id", sourceProfile, conv)
		assert.Equal(t, tc.expectError, err != nil)
	}
}

func TestCreateResourcesForShardedMigration(t *testing.T) {
	cr := CreateResourcesImpl{}
	ctx := context.Background()
	validGetResourcesForGeneration := []*ConnectionProfileReq{{ConnectionProfile: ConnectionProfile{}}}
	validConnectionProfileReq := &ConnectionProfileReq{ConnectionProfile: ConnectionProfile{}, Error: nil, Ctx: ctx}
	errorConnectionProfileReq := &ConnectionProfileReq{ConnectionProfile: ConnectionProfile{}, Error: fmt.Errorf("error"), Ctx: ctx}
	sourceProfile := profiles.SourceProfile{}
	testCases := []struct{
		name              	   			string
		validateOnly		   			bool
		resourcesForGeneration 			[]*ConnectionProfileReq
		resourcesForGenerationError 	error 
		prepareResourcesResult			common.TaskResult[*ConnectionProfileReq]
		runParallelTasksForSourceError	error
		runParallelTasksForTargetError	error
		connectionProfileCleanUpError   error
		expectError 		   			bool
	}{
		{
			name : "Basic ValidateOnly true",
			validateOnly: true,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			expectError: false,
		},
		{
			name : "Basic ValidateOnly false",
			validateOnly: true,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			expectError: false,
		},
		{
			name : "getResourcesForCreation error",
			validateOnly: true,
			resourcesForGeneration: []*ConnectionProfileReq{},
			resourcesForGenerationError: fmt.Errorf("error"),
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			expectError: true,
		},
		{
			name : "Run Parallel Tasks error Source",
			validateOnly: true,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			runParallelTasksForSourceError: fmt.Errorf("error"),
			expectError: true,
		},
		{
			name : "Run Parallel Tasks error Source",
			validateOnly: false,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			runParallelTasksForSourceError: fmt.Errorf("error"),
			expectError: true,
		},
		{
			name : "Run Parallel Tasks error Source Connection Profile Cleanup error",
			validateOnly: false,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: fmt.Errorf("error"),
			runParallelTasksForSourceError: fmt.Errorf("error"),
			expectError: true,
		},
		{
			name : "Run Parallel Tasks error Target",
			validateOnly: false,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			runParallelTasksForTargetError: fmt.Errorf("error"),
			expectError: true,
		},

		{
			name : "Run Parallel Tasks error Target Connection Profile Cleanup error",
			validateOnly: false,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: fmt.Errorf("error"),
			runParallelTasksForTargetError: fmt.Errorf("error"),
			expectError: true,
		},
		{
			name : "Validate Only true, multiple errors",
			validateOnly: true,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*ConnectionProfileReq]{Result: errorConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			expectError: true,
		},
	}
	for _, tc := range testCases {
		mrg := MockResourceGeneration{}
		mrg.On("getResourcesForCreation", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.resourcesForGeneration, tc.resourcesForGeneration, tc.resourcesForGenerationError)
		mrg.On("PrepareMinimalDowntimeResources", mock.Anything, mock.Anything).Return(tc.prepareResourcesResult)
		mrg.On("connectionProfileCleanUp", mock.Anything, mock.Anything).Return(tc.connectionProfileCleanUpError)

		mrpt :=common.MockRunParallelTasks[*ConnectionProfileReq, *ConnectionProfileReq]{}
		mrpt.On("RunParallelTasks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]common.TaskResult[*ConnectionProfileReq]{tc.prepareResourcesResult, tc.prepareResourcesResult}, tc.runParallelTasksForSourceError).Once()
		mrpt.On("RunParallelTasks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]common.TaskResult[*ConnectionProfileReq]{tc.prepareResourcesResult}, tc.runParallelTasksForTargetError).Once()
		cr.ResourceGenerator = &mrg
		cr.RunParallel = &mrpt
		err := cr.CreateResourcesForShardedMigration(ctx, "project-id", "instance-id", tc.validateOnly, "region", sourceProfile)
		assert.Equal(t, tc.expectError, err != nil)
	}
}


func TestPrepareMinimalDowntimeResources(t *testing.T) {
	rg := ResourceGenerationImpl{
		DsClient: &datastreamclient.DatastreamClientMock{},
		StorageClient: &storageclient.StorageClientMock{},
	}
	ctx := context.Background()
	mutex := sync.Mutex{}
	validConnectionProfileReq := ConnectionProfileReq{
		ConnectionProfile: ConnectionProfile{
			DatashardId: "datashard-id",
			ProjectId: "project-id",
			Region: "region",
			Id: "id",
			ValidateOnly: true,
			Port: "3306",
			Host: "0.0.0.0",
			User: "root",
			Password: "password",
		},
		Ctx: ctx,
	}
	testCases := []struct{
		name              	  		string
		sam					  		storageaccessor.StorageAccessorMock
		dsAcc  				  		datastream_accessor.DatastreamAccessorMock
		validateOnly 				bool
		isSource   			  		bool
		connectionProfileRequest    ConnectionProfileReq
		expectError 		  		bool
	}{
		{
			name : "Basic source false validate only true",
			sam: storageaccessor.StorageAccessorMock{
				CreateGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return nil
				},
			},
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				CreateConnectionProfileMock: func (ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error){
					return &datastreampb.ConnectionProfile{}, nil
				},
			},
			validateOnly: true,
			isSource: false,
			connectionProfileRequest: validConnectionProfileReq,
			expectError: false,
		},
		{
			name : "Basic source false validate only false",
			sam: storageaccessor.StorageAccessorMock{
				CreateGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return nil
				},
			},
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				CreateConnectionProfileMock: func (ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error){
					return &datastreampb.ConnectionProfile{}, nil
				},
			},
			validateOnly: false,
			isSource: false,
			connectionProfileRequest: validConnectionProfileReq,
			expectError: false,
		},
		{
			name : "Basic source true validate only true",
			sam: storageaccessor.StorageAccessorMock{
				CreateGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return nil
				},
			},
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				CreateConnectionProfileMock: func (ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error){
					return &datastreampb.ConnectionProfile{}, nil
				},
			},
			validateOnly: true,
			isSource: true,
			connectionProfileRequest: validConnectionProfileReq,
			expectError: false,
		},
		{
			name : "Basic source true validate only false",
			sam: storageaccessor.StorageAccessorMock{
				CreateGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return nil
				},
			},
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				CreateConnectionProfileMock: func (ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error){
					return &datastreampb.ConnectionProfile{}, nil
				},
			},
			validateOnly: false,
			isSource: true,
			connectionProfileRequest: validConnectionProfileReq,
			expectError: false,
		},
		{
			name : "create gcs error",
			sam: storageaccessor.StorageAccessorMock{
				CreateGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return fmt.Errorf("error")
				},
			},
			validateOnly: false,
			isSource: false,
			connectionProfileRequest: validConnectionProfileReq,
			expectError: true,
		},
		{
			name : "create connection profile error",
			sam: storageaccessor.StorageAccessorMock{
				CreateGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return nil
				},
			},
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				CreateConnectionProfileMock: func (ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error){
					return nil, fmt.Errorf("error")
				},
			},
			validateOnly: false,
			isSource: true,
			connectionProfileRequest: validConnectionProfileReq,
			expectError: false,
		},
	}
	for _, tc := range testCases {
		tc.connectionProfileRequest.ConnectionProfile.ValidateOnly = tc.validateOnly
		rg.DsAcc = &tc.dsAcc
		rg.StorageAcc = &tc.sam
		res := rg.PrepareMinimalDowntimeResources(&tc.connectionProfileRequest, &mutex)
		assert.Equal(t, tc.expectError, res.Err != nil)
	}
}