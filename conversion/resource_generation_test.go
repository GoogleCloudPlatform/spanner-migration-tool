// // Copyright 2024 Google LLC
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

package conversion_test

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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestValidateResourceGeneration(t *testing.T) {
	vrg := conversion.ValidateResourcesImpl{
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
		var m = conversion.MockValidateOrCreateResources{}
		m.On("ValidateOrCreateResourcesForShardedMigration", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.createResourcesError)
		vrg.ValidateOrCreateResources = &m
		err := vrg.ValidateResourceGeneration(ctx, "project-id", "instance-id", sourceProfile, conv)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestCreateResourcesForShardedMigration(t *testing.T) {
	cr := conversion.ValidateOrCreateResourcesImpl{}
	ctx := context.Background()
	validGetResourcesForGeneration := []*conversion.ConnectionProfileReq{{ConnectionProfile: conversion.ConnectionProfile{}}}
	validConnectionProfileReq := &conversion.ConnectionProfileReq{ConnectionProfile: conversion.ConnectionProfile{}, Error: nil, Ctx: ctx}
	errorConnectionProfileReq := &conversion.ConnectionProfileReq{ConnectionProfile: conversion.ConnectionProfile{}, Error: fmt.Errorf("error"), Ctx: ctx}
	sourceProfile := profiles.SourceProfile{}
	testCases := []struct{
		name              	   			string
		validateOnly		   			bool
		resourcesForGeneration 			[]*conversion.ConnectionProfileReq
		resourcesForGenerationError 	error 
		prepareResourcesResult			common.TaskResult[*conversion.ConnectionProfileReq]
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
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			expectError: false,
		},
		{
			name : "Basic ValidateOnly false",
			validateOnly: true,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			expectError: false,
		},
		{
			name : "getResourcesForCreation error",
			validateOnly: true,
			resourcesForGeneration: []*conversion.ConnectionProfileReq{},
			resourcesForGenerationError: fmt.Errorf("error"),
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			expectError: true,
		},
		{
			name : "Run Parallel Tasks error Source",
			validateOnly: true,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			runParallelTasksForSourceError: fmt.Errorf("error"),
			expectError: true,
		},
		{
			name : "Run Parallel Tasks error Source",
			validateOnly: false,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			runParallelTasksForSourceError: fmt.Errorf("error"),
			expectError: true,
		},
		{
			name : "Run Parallel Tasks error Source Connection Profile Cleanup error",
			validateOnly: false,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: fmt.Errorf("error"),
			runParallelTasksForSourceError: fmt.Errorf("error"),
			expectError: true,
		},
		{
			name : "Run Parallel Tasks error Target",
			validateOnly: false,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			runParallelTasksForTargetError: fmt.Errorf("error"),
			expectError: true,
		},

		{
			name : "Run Parallel Tasks error Target Connection Profile Cleanup error",
			validateOnly: false,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: validConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: fmt.Errorf("error"),
			runParallelTasksForTargetError: fmt.Errorf("error"),
			expectError: true,
		},
		{
			name : "Validate Only true, multiple errors",
			validateOnly: true,
			resourcesForGeneration: validGetResourcesForGeneration,
			resourcesForGenerationError: nil,
			prepareResourcesResult: common.TaskResult[*conversion.ConnectionProfileReq]{Result: errorConnectionProfileReq, Err: nil},
			connectionProfileCleanUpError: nil,
			expectError: true,
		},
	}
	for _, tc := range testCases {
		mrg := conversion.MockResourceGeneration{}
		mrg.On("GetResourcesForCreation", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.resourcesForGeneration, tc.resourcesForGeneration, tc.resourcesForGenerationError)
		mrg.On("PrepareMinimalDowntimeResources", mock.Anything, mock.Anything).Return(tc.prepareResourcesResult)
		mrg.On("ConnectionProfileCleanUp", mock.Anything, mock.Anything).Return(tc.connectionProfileCleanUpError)

		mrpt :=common.MockRunParallelTasks[*conversion.ConnectionProfileReq, *conversion.ConnectionProfileReq]{}
		mrpt.On("RunParallelTasks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]common.TaskResult[*conversion.ConnectionProfileReq]{tc.prepareResourcesResult, tc.prepareResourcesResult}, tc.runParallelTasksForSourceError).Once()
		mrpt.On("RunParallelTasks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]common.TaskResult[*conversion.ConnectionProfileReq]{tc.prepareResourcesResult}, tc.runParallelTasksForTargetError).Once()
		cr.ResourceGenerator = &mrg
		cr.RunParallel = &mrpt
		err := cr.ValidateOrCreateResourcesForShardedMigration(ctx, "project-id", "instance-id", tc.validateOnly, "region", sourceProfile)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}


func TestPrepareMinimalDowntimeResources(t *testing.T) {
	rg := conversion.ResourceGenerationImpl{
		DsClient: &datastreamclient.DatastreamClientMock{},
		StorageClient: &storageclient.StorageClientMock{},
	}
	ctx := context.Background()
	mutex := sync.Mutex{}
	validConnectionProfileReq := conversion.ConnectionProfileReq{
		ConnectionProfile: conversion.ConnectionProfile{
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
		connectionProfileRequest    conversion.ConnectionProfileReq
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
			expectError: true,
		},
	}
	for _, tc := range testCases {
		tc.connectionProfileRequest.ConnectionProfile.ValidateOnly = tc.validateOnly
		tc.connectionProfileRequest.ConnectionProfile.IsSource = tc.isSource
		rg.DsAcc = &tc.dsAcc
		rg.StorageAcc = &tc.sam
		res := rg.PrepareMinimalDowntimeResources(&tc.connectionProfileRequest, &mutex)
		assert.Equal(t, tc.expectError, res.Err != nil, tc.name)
	}
}

func TestConnectionProfileCleanUp(t *testing.T) {
	connProfile := conversion.ConnectionProfileReq{
		ConnectionProfile: conversion.ConnectionProfile{
			ProjectId: "project-id",
			Region: "region",
			Id: "id",
			BucketName: "bucket-name",
		}}
	rg := conversion.ResourceGenerationImpl{
		DsClient: &datastreamclient.DatastreamClientMock{},
		StorageClient: &storageclient.StorageClientMock{},
	}
	ctx := context.Background()
	testCases := []struct{
		name              	  string
		sam					  storageaccessor.StorageAccessorMock
		dsAcc  				  datastream_accessor.DatastreamAccessorMock
		expectError 		  bool
	}{
		{
			name : "Basic",
			sam : storageaccessor.StorageAccessorMock{
				DeleteGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return nil
				},
			},
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				DeleteConnectionProfileMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, id string, projectId string, region string) error{
					return nil
				},
			},
			expectError: false,
		},
		{
			name : "delete connection profile error",
			sam : storageaccessor.StorageAccessorMock{
				DeleteGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return fmt.Errorf("error")
				},
			},
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				DeleteConnectionProfileMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, id string, projectId string, region string) error{
					return nil
				},
			},
			expectError: true,
		},
		{
			name : "delete gcs bucket error",
			sam : storageaccessor.StorageAccessorMock{
				DeleteGCSBucketMock: func(ctx context.Context, sc storageclient.StorageClient, req storageaccessor.StorageBucketMetadata) error{
					return nil
				},
			},
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				DeleteConnectionProfileMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, id string, projectId string, region string) error{
					return fmt.Errorf("error")
				},
			},
			expectError: true,
		},
	}
	for _, tc := range testCases {
		rg.DsAcc = &tc.dsAcc
		rg.StorageAcc = &tc.sam
		err := rg.ConnectionProfileCleanUp(ctx, []*conversion.ConnectionProfileReq{&connProfile})
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}


func TestGetResourcesForCreation(t *testing.T) {
	rg := conversion.ResourceGenerationImpl{
		DsClient: &datastreamclient.DatastreamClientMock{},
	}
	ctx := context.Background()
	testCases := []struct{
		name              	  string
		dsAcc  				  datastream_accessor.DatastreamAccessorMock
		srcProfile    		  profiles.DatastreamConnProfile
		dstProfile    		  profiles.DatastreamConnProfile
		validateOnly 		  bool
		expectError 		  bool
	}{
		{
			name : "Basic both profiles exist validate only false",
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				ConnectionProfileExistsMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error){
					return true, nil
				},
			},
			srcProfile: profiles.DatastreamConnProfile{
				Name: "src-profile",
				Location: "region",
			},
			dstProfile: profiles.DatastreamConnProfile{
				Name: "dst-profile",
				Location: "region",
			},
			validateOnly: false,
			expectError: false,
		},
		{
			name : "Basic both profiles exist validate only true",
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				ConnectionProfileExistsMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error){
					return true, nil
				},
			},
			srcProfile: profiles.DatastreamConnProfile{
				Name: "src-profile",
				Location: "region",
			},
			dstProfile: profiles.DatastreamConnProfile{
				Name: "dst-profile",
				Location: "region",
			},
			validateOnly: true,
			expectError: false,
		},
		{
			name : "Basic both profiles do not exist validate only false",
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				ConnectionProfileExistsMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error){
					return false, nil
				},
			},
			srcProfile: profiles.DatastreamConnProfile{
				Name: "src-profile",
				Location: "region",
			},
			dstProfile: profiles.DatastreamConnProfile{
				Name: "dst-profile",
				Location: "region",
			},
			validateOnly: false,
			expectError: false,
		},
		{
			name : "Basic both profiles do not exist validate only true",
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				ConnectionProfileExistsMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error){
					return false, nil
				},
			},
			srcProfile: profiles.DatastreamConnProfile{
				Name: "src-profile",
				Location: "region",
			},
			dstProfile: profiles.DatastreamConnProfile{
				Name: "dst-profile",
				Location: "region",
			},
			validateOnly: true,
			expectError: false,
		},
		{
			name : "Both profiles do not exist validate only true location and name missing",
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				ConnectionProfileExistsMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error){
					return false, nil
				},
			},
			srcProfile: profiles.DatastreamConnProfile{},
			dstProfile: profiles.DatastreamConnProfile{},
			validateOnly: true,
			expectError: false,
		},
		{
			name : "Both profiles do not exist validate only true location missing",
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				ConnectionProfileExistsMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error){
					return false, nil
				},
			},
			srcProfile: profiles.DatastreamConnProfile{
				Name: "src-profile",
			},
			dstProfile: profiles.DatastreamConnProfile{
				Name: "dst-profile",
			},
			validateOnly: false,
			expectError: false,
		},
		{
			name : "connection profile exists error",
			dsAcc: datastream_accessor.DatastreamAccessorMock{
				ConnectionProfileExistsMock: func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error){
					return false, fmt.Errorf("error")
				},
			},
			srcProfile: profiles.DatastreamConnProfile{
				Name: "src-profile",
				Location: "region",
			},
			dstProfile: profiles.DatastreamConnProfile{
				Name: "dst-profile",
				Location: "region",
			},
			validateOnly: false,
			expectError: true,
		},
	}
	for _, tc := range testCases {
		sourceProfile := profiles.SourceProfile{
			Config: profiles.SourceProfileConfig{
				ShardConfigurationDataflow: profiles.ShardConfigurationDataflow{
					DataShards: []*profiles.DataShard{
						{
							SrcConnectionProfile: tc.srcProfile,
							DstConnectionProfile: tc.dstProfile,
						},
					},					
				},
			},
		}
		rg.DsAcc = &tc.dsAcc
		_, _, err := rg.GetResourcesForCreation(ctx, "project-id", sourceProfile, "region", tc.validateOnly)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestNewValidateResourcesImpl(t *testing.T) {
	spAcc := spanneraccessor.SpannerAccessorMock{}
	spInAdmin := spinstanceadmin.InstanceAdminClientMock{}
	dsAcc := datastream_accessor.DatastreamAccessorMock{}
	dsClient := datastreamclient.DatastreamClientMock{}
	storageAcc := storageaccessor.StorageAccessorMock{}
	stoargeClient := storageclient.StorageClientMock{}
	vr := conversion.NewValidateResourcesImpl(&spAcc, &spInAdmin, &dsAcc, &dsClient, &storageAcc, &stoargeClient)
	assert.Equal(t, vr.SpAcc, &spAcc)
	assert.Equal(t, vr.SpInstanceAdmin, &spInAdmin)
	rg := vr.ValidateOrCreateResources.(*conversion.ValidateOrCreateResourcesImpl).ResourceGenerator.(*conversion.ResourceGenerationImpl)
	assert.Equal(t, rg.DsAcc, &dsAcc)
	assert.Equal(t, rg.DsClient, &dsClient)
	assert.Equal(t, rg.StorageAcc, &storageAcc)
	assert.Equal(t, rg.StorageClient, rg.StorageClient)
}