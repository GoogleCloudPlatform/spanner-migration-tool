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

package resourcemanager_accessor

import (
	"context"
	"fmt"
	"sync"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	resourcemanagerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/resourcemanager"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
)

// ProjectNumberResourceCache Maps Project-Id to ProjectNumber to avoid redundant calls to resource manager.
var ProjectNumberResourceCache sync.Map

// ResourceManagerProjectsAccessor The ResourceManagerAccessor provides methods that internally use the resourceManagerProjectsClient.
type ResourceManagerProjectsAccessor interface {
	GetProjectNumberResource(ctx context.Context, rmProjectsClient resourcemanager.ProjectsClient, projectID string) string
}
type ResourceManagerProjectsAccessorImpl struct{}

// GetProjectNumberResource returns a string that encodes the project number like `projects/12345`.
func (rm *ResourceManagerProjectsAccessorImpl) GetProjectNumberResource(ctx context.Context, rmProjectsClient resourcemanagerclient.ResourcemanagerProjectsClient, projectID string) string {
	projectNumberResource, found := ProjectNumberResourceCache.Load(projectID)
	if found {
		return projectNumberResource.(string)
	}

	// `GetProjectRequest` has out of box retries.
	// Ref - https://github.com/googleapis/googleapis/blob/master/google/cloud/resourcemanager/v3/cloudresourcemanager_v3_grpc_service_config.json
	req := resourcemanagerpb.GetProjectRequest{Name: projectID}
	project, err := rmProjectsClient.GetProject(ctx, &req)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Could not query resourcemanager to get project number. Defaulting to ProjectId=%s. error=%v",
			projectID, err))
		return projectID
	}
	projectNumberResource = project.GetName()
	ProjectNumberResourceCache.Store(projectID, projectNumberResource)
	return projectNumberResource.(string)
}
