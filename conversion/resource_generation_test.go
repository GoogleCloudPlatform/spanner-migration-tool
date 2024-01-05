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
	"testing"

	datastream "cloud.google.com/go/datastream/apiv1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockGetConnProfilesRegionStruct struct{mock.Mock}
func (m *mockGetConnProfilesRegionStruct) getConnProfilesRegion(ctx context.Context, projectId string, region string, dsClient *datastream.Client) ([]string, error) {
	args := m.Called(ctx, projectId, region, dsClient)
	return args.Get(0).([]string), args.Error(1)
}

type mockConnectionProfileExistsStruct struct {mock.Mock}

type mockGetResourcesForCreationStruct struct {mock.Mock}
func TestGetResourcesForCreation(t *testing.T) {
	var m mockGetConnProfilesRegionStruct
	ctx := context.Background()
	dsClient := GetDatastreamClient(ctx)
	getConnProfilesRegionResult := []string{"cnProfile1", "cnProfile2", "cnProfile3"}
	var connectionProfiles map[string][]string = make(map[string][]string)
	m.On("getConnProfilesRegion", ctx, "project-id", "region", dsClient).Return(getConnProfilesRegionResult, nil)
	c := connectionProfileExistsStruct{}
	result, err := c.connectionProfileExists(ctx, "project-id", "cnProfile1", "region", connectionProfiles, dsClient, &m)
	assert.Equal(t, result, true)
	assert.Equal(t, err, nil)
}