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

package resourcemanager_accessor_test

import (
	"context"
	"testing"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	resourcemanager_client_test "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/resourcemanager/resourcemanager_test"
	resourcemanager_accessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/resourcemanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetProjectNumberResource(t *testing.T) {
	ctx := context.Background()
	rma := resourcemanager_accessor.ResourceManagerProjectsAccessorImpl{}
	testCases := []struct {
		name                       string
		expectedProjNumberResource string
		passedProjName             string
		expectError                bool
	}{
		{
			name:                       "basic correct",
			expectedProjNumberResource: "projects/123",
			passedProjName:             "projects/test",
			expectError:                false,
		},
	}
	for _, tc := range testCases {
		mockClient := resourcemanager_client_test.ResourcemanagerProjectsClientMock{}
		req := resourcemanagerpb.GetProjectRequest{Name: tc.passedProjName}
		if tc.expectError == false {
			mockClient.On("GetProject", mock.Anything, &req, mock.Anything).Return(&resourcemanagerpb.Project{Name: tc.expectedProjNumberResource,
				Parent: "folders/123", ProjectId: tc.passedProjName}, nil)
		} else {
			mockClient.On("GetProject", mock.Anything, &req, mock.Anything).Return(nil, "error")

		}
		projectNumber := rma.GetProjectNumberResource(ctx, &mockClient, tc.passedProjName)
		assert.Equal(t, tc.expectedProjNumberResource, projectNumber)

	}
}
