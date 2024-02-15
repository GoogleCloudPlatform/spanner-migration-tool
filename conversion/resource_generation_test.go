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

	// "errors"
	"testing"

	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	// "github.com/stretchr/testify/mock"
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

// func TestConnectionProfileExistsSuccessFalse(t *testing.T) {
// 	var m mockTestConnectionProfileExistsStruct
// 	ctx := context.Background()

// 	dsClient := &datastream.Client{}
// 	getConnProfilesRegionResult := []string{"cnProfile1", "cnProfile2", "cnProfile3"}
// 	var connectionProfiles map[string][]string = make(map[string][]string)
// 	m.On("getConnProfilesRegion", ctx, "project-id", "region", dsClient).Return(getConnProfilesRegionResult, nil)
// 	r := ResourceGenerationStruct{
// 		resourceGenerator: &m,
// 	}
// 	result, err := r.connectionProfileExists(ctx, "project-id", "cnProfile4", "region", connectionProfiles, dsClient)
// 	assert.Equal(t, result, false)
// 	assert.Nil(t, err)
// }

// func TestConnectionProfileExistsFailure(t *testing.T) {
// 	var m mockTestConnectionProfileExistsStruct
// 	ctx := context.Background()

// 	dsClient := &datastream.Client{}
// 	var connectionProfiles map[string][]string = make(map[string][]string)

// 	error:= errors.New("mock error")
// 	m.On("getConnProfilesRegion", ctx, "project-id", "region", dsClient).Return([]string{}, error)
// 	r := ResourceGenerationStruct{
// 		resourceGenerator: &m,
// 	}
// 	result, err := r.connectionProfileExists(ctx, "project-id", "cnProfile1", "region", connectionProfiles, dsClient)
// 	assert.Equal(t, err, error)
// 	assert.Equal(t, result, false)
// }

// type mockTestGetResourcesForCreationStruct struct{
// 	resourceGenerationInterface
// 	mock.Mock
// }

// func (m *mockTestGetResourcesForCreationStruct) connectionProfileExists(ctx context.Context, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string, dsClient *datastream.Client) (bool, error){
// 	args := m.Called(ctx, projectId, profileName, profileLocation, connectionProfiles, dsClient)
// 	return args.Get(0).(bool), args.Error(1)
// }

// func TestGetResourcesForCreationSuccess(t *testing.T) {
// 	var m mockTestGetResourcesForCreationStruct
// 	ctx := context.Background()

// 	dsClient := &datastream.Client{}
// 	var connectionProfiles map[string][]string = make(map[string][]string)

// 	error:= errors.New("mock error")
// 	m.On("connectionProfileExists", ctx, "project-id", "region", dsClient).Return([]string{}, error)
// 	r := ResourceGenerationStruct{
// 		resourceGenerator: &m,
// 	}
// 	result, err := r.connectionProfileExists(ctx, "project-id", "cnProfile1", "region", connectionProfiles, dsClient)
// 	assert.Equal(t, err, error)
// 	assert.Equal(t, result, false)
// }
