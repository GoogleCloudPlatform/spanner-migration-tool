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
	"errors"
	"testing"

	datastream "cloud.google.com/go/datastream/apiv1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockTestConnectionProfileExistsStruct struct{
	resourceGenerationInterface
	mock.Mock
}

func (m *mockTestConnectionProfileExistsStruct) getConnProfilesRegion(ctx context.Context, projectId string, region string, dsClient *datastream.Client) ([]string, error) {
	args := m.Called(ctx, projectId, region, dsClient)
	return args.Get(0).([]string), args.Error(1)
}

func TestConnectionProfileExistsSuccessTrue(t *testing.T) {
	var m mockTestConnectionProfileExistsStruct
	ctx := context.Background()

	dsClient := &datastream.Client{}
	getConnProfilesRegionResult := []string{"cnProfile1", "cnProfile2", "cnProfile3"}
	var connectionProfiles map[string][]string = make(map[string][]string)
	m.On("getConnProfilesRegion", ctx, "project-id", "region", dsClient).Return(getConnProfilesRegionResult, nil)
	r := ResourceGenerationStruct{}
	result, err := r.connectionProfileExists(ctx, "project-id", "cnProfile1", "region", connectionProfiles, dsClient, &m)
	assert.Equal(t, result, true)
	assert.Nil(t, err)
}

func TestConnectionProfileExistsSuccessFalse(t *testing.T) {
	var m mockTestConnectionProfileExistsStruct
	ctx := context.Background()

	dsClient := &datastream.Client{}
	getConnProfilesRegionResult := []string{"cnProfile1", "cnProfile2", "cnProfile3"}
	var connectionProfiles map[string][]string = make(map[string][]string)
	m.On("getConnProfilesRegion", ctx, "project-id", "region", dsClient).Return(getConnProfilesRegionResult, nil)
	r := ResourceGenerationStruct{}
	result, err := r.connectionProfileExists(ctx, "project-id", "cnProfile4", "region", connectionProfiles, dsClient, &m)
	assert.Equal(t, result, false)
	assert.Nil(t, err)
}

func TestConnectionProfileExistsFailure(t *testing.T) {
	var m mockTestConnectionProfileExistsStruct
	ctx := context.Background()

	dsClient := &datastream.Client{}
	var connectionProfiles map[string][]string = make(map[string][]string)

	error:= errors.New("mock error")
	m.On("getConnProfilesRegion", ctx, "project-id", "region", dsClient).Return([]string{}, error)
	r := ResourceGenerationStruct{}
	result, err := r.connectionProfileExists(ctx, "project-id", "cnProfile1", "region", connectionProfiles, dsClient, &m)
	assert.Equal(t, err, error)
	assert.Equal(t, result, false)
}

type mockTestGetResourcesForCreationStruct struct{
	resourceGenerationInterface
	mock.Mock
}

func (m *mockTestGetResourcesForCreationStruct) connectionProfileExists (ctx context.Context, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string, dsClient *datastream.Client, s resourceGenerationInterface) (bool, error){
	args := m.Called(ctx, projectId, profileName, profileLocation, connectionProfiles, dsClient, s)
	return args.Get(0).(bool), args.Error(1)
}

func TestGetResourcesForCreationSuccess(t *testing.T) {
	var m mockTestGetResourcesForCreationStruct
	ctx := context.Background()

	dsClient := &datastream.Client{}
	var connectionProfiles map[string][]string = make(map[string][]string)

	error:= errors.New("mock error")
	m.On("connectionProfileExists", ctx, "project-id", "region", dsClient).Return([]string{}, error)
	r := ResourceGenerationStruct{}
	result, err := r.connectionProfileExists(ctx, "project-id", "cnProfile1", "region", connectionProfiles, dsClient, &m)
	assert.Equal(t, err, error)
	assert.Equal(t, result, false)
}
