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
package resourcemanagerclient

import (
	"context"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/googleapis/gax-go/v2"
)

// Use this interface instead of datastream.FlexTemplatesClient to support mocking.
type ResourcemanagerProjectsClient interface {
	GetProject(ctx context.Context, req *resourcemanagerpb.GetProjectRequest, opts ...gax.CallOption) (*resourcemanagerpb.Project, error)
}

// This implements the ResourcemanagerProjectsClient interface. This is the primary implementation that should be used in all places other than tests.
type ResourcemanagerProjectsClientImpl struct {
	client *resourcemanager.ProjectsClient
}

func NewResourcemanagerProjectsClientImpl(ctx context.Context) (*ResourcemanagerProjectsClientImpl, error) {
	c, err := GetOrCreateClient(ctx)
	if err != nil {
		return nil, err
	}
	return &ResourcemanagerProjectsClientImpl{client: c}, nil
}

func (c *ResourcemanagerProjectsClientImpl) GetProject(ctx context.Context, req *resourcemanagerpb.GetProjectRequest, opts ...gax.CallOption) (*resourcemanagerpb.Project, error) {
	return c.client.GetProject(ctx, req, opts...)
}
