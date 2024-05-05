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
package resourcemanagerclient_test

import (
	"context"

	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/mock"
)

// Mock that implements the ResourceManagerProjectsClient interface.
// Pass in unit tests where ResourceManagerProjectsClient is an input parameter.
type ResourcemanagerProjectsClientMock struct {
	mock.Mock
}

// GetProject implements resourcemanagerclient.ResourcemanagerProjectsClient.
func (m ResourcemanagerProjectsClientMock) GetProject(ctx context.Context, req *resourcemanagerpb.GetProjectRequest, opts ...gax.CallOption) (*resourcemanagerpb.Project, error) {
	args := m.Called(ctx, req, opts)
	// Avoid panic for typeassertion due to null pointer.
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*resourcemanagerpb.Project), args.Error(1)
}
