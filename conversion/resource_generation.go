// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package conversion handles initial setup for the command line tool
// and web APIs.

// TODO:(searce) Organize code in go style format to make this file more readable.
//
//	public constants first
//	key public type definitions next (although often it makes sense to put them next to public functions that use them)
//	then public functions (and relevant type definitions)
//	and helper functions and other non-public definitions last (generally in order of importance)
package conversion

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/datastream/apiv1/datastreampb"
	ds "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/datastream"
	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
	datastream_accessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/datastream"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	storageaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/google/uuid"
)

var (
	resourcesForCleanup []*ConnectionProfileReq
)

type ResourceGenerationInterface interface {
	ConnectionProfileCleanUp(ctx context.Context, profiles []*ConnectionProfileReq) error
	GetResourcesForCreation(ctx context.Context, projectId string, sourceProfile profiles.SourceProfile, region string, validateOnly bool) ([]*ConnectionProfileReq, []*ConnectionProfileReq, error)
	PrepareMinimalDowntimeResources(createResourceData *ConnectionProfileReq, mutex *sync.Mutex) common.TaskResult[*ConnectionProfileReq]
}

type ResourceGenerationImpl struct {
	DsAcc             datastream_accessor.DatastreamAccessor
	DsClient  		  ds.DatastreamClient
	StorageAcc        storageaccessor.StorageAccessor
	StorageClient     storageclient.StorageClient
}

type ValidateOrCreateResourcesInterface interface {
	ValidateOrCreateResourcesForShardedMigration(ctx context.Context, projectId string, instanceName string, validateOnly bool, region string, sourceProfile profiles.SourceProfile) error
}

type ValidateOrCreateResourcesImpl struct{
	ResourceGenerator ResourceGenerationInterface
	RunParallel       common.RunParallelTasksInterface[*ConnectionProfileReq, *ConnectionProfileReq]
}

type ValidateResourcesInterface interface {
	ValidateResourceGeneration(ctx context.Context, projectId string, instanceId string, sourceProfile profiles.SourceProfile, conv *internal.Conv) error
}

type ValidateResourcesImpl struct{
	SpAcc                spanneraccessor.SpannerAccessor
	SpInstanceAdmin      spinstanceadmin.InstanceAdminClient
	ValidateOrCreateResources      ValidateOrCreateResourcesInterface
}

func NewValidateResourcesImpl(spAcc spanneraccessor.SpannerAccessor, spInstanceAdmin spinstanceadmin.InstanceAdminClient, dsAcc datastream_accessor.DatastreamAccessor, dsClient ds.DatastreamClient, storageAcc storageaccessor.StorageAccessor, storageClient storageclient.StorageClient) *ValidateResourcesImpl {
    return &ValidateResourcesImpl{
		SpAcc: spAcc,
		SpInstanceAdmin: spInstanceAdmin,
		ValidateOrCreateResources: NewValidateOrCreateResourcesImpl(dsAcc, dsClient, storageAcc, storageClient),
	}
}

func NewValidateOrCreateResourcesImpl(dsAcc datastream_accessor.DatastreamAccessor, dsClient ds.DatastreamClient, storageAcc storageaccessor.StorageAccessor, storageClient storageclient.StorageClient) *ValidateOrCreateResourcesImpl{
	return &ValidateOrCreateResourcesImpl{
		ResourceGenerator: NewResourceGenerationImpl(dsAcc, dsClient, storageAcc, storageClient),
		RunParallel: &common.RunParallelTasksImpl[*ConnectionProfileReq, *ConnectionProfileReq]{},
	}
}

func NewResourceGenerationImpl(dsAcc datastream_accessor.DatastreamAccessor, dsClient ds.DatastreamClient, storageAcc storageaccessor.StorageAccessor, storageClient storageclient.StorageClient) *ResourceGenerationImpl{
	return &ResourceGenerationImpl{
			DsAcc: dsAcc,
			DsClient: dsClient,
			StorageAcc: storageAcc,
			StorageClient: storageClient,
		}
}

// Method to validate if in a minimal downtime migration, required resources can be generated
func (v *ValidateResourcesImpl) ValidateResourceGeneration(ctx context.Context, projectId string, instanceId string, sourceProfile profiles.SourceProfile, conv *internal.Conv) error {
	spannerRegion, err := v.SpAcc.GetSpannerLeaderLocation(ctx, v.SpInstanceAdmin, "projects/" + projectId + "/instances/" + instanceId)
	if err != nil {
		err = fmt.Errorf("unable to fetch Spanner Region: %v", err)
		return err
	}
	conv.SpRegion = spannerRegion
	err = v.ValidateOrCreateResources.ValidateOrCreateResourcesForShardedMigration(ctx, projectId, instanceId, true, spannerRegion, sourceProfile)
	if err != nil {
		err = fmt.Errorf("unable to create connection profiles: %v", err)
		return err
	}
	conv.ResourceValidation = true
	return nil
}

// 1. If destination connection profile needs to be created, creates a gcs bucket
// 2. Creates the connection profile needed for migration
func (r ResourceGenerationImpl) PrepareMinimalDowntimeResources(createResourceData *ConnectionProfileReq, mutex *sync.Mutex) common.TaskResult[*ConnectionProfileReq] {
	req := &datastreampb.CreateConnectionProfileRequest{
		Parent:              fmt.Sprintf("projects/%s/locations/%s", createResourceData.ConnectionProfile.ProjectId, createResourceData.ConnectionProfile.Region),
		ConnectionProfileId: createResourceData.ConnectionProfile.Id,
		ConnectionProfile: &datastreampb.ConnectionProfile{
			DisplayName:  createResourceData.ConnectionProfile.Id,
			Connectivity: &datastreampb.ConnectionProfile_StaticServiceIpConnectivity{},
		},
		ValidateOnly: createResourceData.ConnectionProfile.ValidateOnly,
	}

	// If destination source profile is to be created, create a gcs bucket first
	var bucketName string
	if !createResourceData.ConnectionProfile.IsSource {
		bucketName = strings.ToLower("GCS-" + createResourceData.ConnectionProfile.Id)
		err := r.StorageAcc.CreateGCSBucket(createResourceData.Ctx, r.StorageClient, storageaccessor.StorageBucketMetadata{
			BucketName:    bucketName,
			ProjectID:     createResourceData.ConnectionProfile.ProjectId,
			Location:      createResourceData.ConnectionProfile.Region,
		})
		if err != nil {
			createResourceData.Error = err
			return common.TaskResult[*ConnectionProfileReq]{Result: createResourceData, Err: err}
		}
	}
	createResourceData.ConnectionProfile.BucketName = bucketName

	// Set Profile for resource creation
	setConnectionProfileFromRequest(createResourceData, req)

	// Create or Validate Resource
	_, err := r.DsAcc.CreateConnectionProfile(createResourceData.Ctx, r.DsClient, req)
	if err != nil {
		createResourceData.Error = err
		return common.TaskResult[*ConnectionProfileReq]{Result: createResourceData, Err: err}
	}

	if !createResourceData.ConnectionProfile.ValidateOnly {
		fmt.Printf("Connection Profile for Datashard %v has been created: %v\n", createResourceData.ConnectionProfile.DatashardId, createResourceData.ConnectionProfile.Id)
		// In case of failure, add resources to be cleaned up
		resourcesForCleanup = append(resourcesForCleanup, createResourceData)
	} else {
		fmt.Printf("Connection Profile for Datashard %v has been validated: %v\n", createResourceData.ConnectionProfile.DatashardId, createResourceData.ConnectionProfile.Id)
	}

	return common.TaskResult[*ConnectionProfileReq]{Result: createResourceData, Err: nil}
}

// If any of the resource creation fails, deletes all resources that were created
func (r ResourceGenerationImpl) ConnectionProfileCleanUp(ctx context.Context, profiles []*ConnectionProfileReq) error {
	for _, profile := range profiles {
		err := r.DsAcc.DeleteConnectionProfile(ctx, r.DsClient, profile.ConnectionProfile.ProjectId, profile.ConnectionProfile.Region, profile.ConnectionProfile.Id)
		if err != nil {
			return err
		}

		if profile.ConnectionProfile.BucketName != "" {
			err := r.StorageAcc.DeleteGCSBucket(ctx, r.StorageClient, storageaccessor.StorageBucketMetadata{BucketName:profile.ConnectionProfile.BucketName})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Returns source and destination connection profiles to be created
func (r ResourceGenerationImpl) GetResourcesForCreation(ctx context.Context, projectId string, sourceProfile profiles.SourceProfile, region string, validateOnly bool) ([]*ConnectionProfileReq, []*ConnectionProfileReq, error) {
	var sourceProfilesToCreate []*ConnectionProfileReq
	var dstProfilesToCreate []*ConnectionProfileReq

	// Map for each region with list of all connection profiles
	var connectionProfiles map[string][]string = make(map[string][]string)
	var err error = nil

	for _, profile := range sourceProfile.Config.ShardConfigurationDataflow.DataShards {
		// Check if source profile needs to be created
		sourceProfile, err := getSourceConnectionProfileForCreation(ctx, projectId, profile, region, validateOnly, connectionProfiles, r.DsAcc, r.DsClient)
		if err != nil {
			return sourceProfilesToCreate, dstProfilesToCreate, err
		}
		if sourceProfile!= nil {
			sourceProfilesToCreate = append(sourceProfilesToCreate, sourceProfile)
		}

		// Check if destination profile needs to be created
		dstProfile, err := getDstConnectionProfileForCreation(ctx, projectId, profile, region, validateOnly, connectionProfiles, r.DsAcc, r.DsClient)
		if err != nil {
			return sourceProfilesToCreate, dstProfilesToCreate, err
		}
		if dstProfile!= nil {
			dstProfilesToCreate = append(dstProfilesToCreate, dstProfile)
		}
	}
	return sourceProfilesToCreate, dstProfilesToCreate, err
}

// 1. For each datashard, check if source and destination connection profile exists or not
// 2. If source connection profile doesn't exists create it or validate if creation is possible.
// 3. If validation is false and destination connection profile doesn't exists create a corresponding gcs bucket and then a destination connection profile
func (c *ValidateOrCreateResourcesImpl) ValidateOrCreateResourcesForShardedMigration(ctx context.Context, projectId string, instanceName string, validateOnly bool, region string, sourceProfile profiles.SourceProfile) error {
	var sourceProfilesToCreate []*ConnectionProfileReq
	var dstProfilesToCreate []*ConnectionProfileReq

	// Fetches list with resources which do not exist and need to be created
	sourceProfilesToCreate, dstProfilesToCreate, err := c.ResourceGenerator.GetResourcesForCreation(ctx, projectId, sourceProfile, region, validateOnly)
	if err != nil {
		return fmt.Errorf("resource generation failed %s", err)
	}

	// If validating resource creation, validate for all connection profiles. If creating, return error for the first resource creation that fails.
	fastExit := false
	if !validateOnly {
		fastExit = true
	}

	var errorsList []error = []error{}

	// Create or validate source connection profiles in parallel threads
	resSourceProfiles, resCreationErr := c.RunParallel.RunParallelTasks(sourceProfilesToCreate, 20, c.ResourceGenerator.PrepareMinimalDowntimeResources, fastExit)
	// If creation failed, perform cleanup of resources
	if resCreationErr != nil && !validateOnly {
		err = c.ResourceGenerator.ConnectionProfileCleanUp(ctx, resourcesForCleanup)
		if err != nil {
			return fmt.Errorf("resource generation failed due to %s, resources created could not be cleaned up, please cleanup manually: %s", resCreationErr.Error(), err.Error())
		} else {
			return resCreationErr
		}
	} else if resCreationErr != nil {
		return resCreationErr
	}
	for _, resource := range resSourceProfiles {
		if resource.Result.Error != nil && validateOnly {
			// If validation failed, append to list of errors
			errorsList = append(errorsList, resource.Result.Error)
		}
	}

	// Create destination connection profiles in parallel threads
	if !validateOnly {
		_, resCreationErr := c.RunParallel.RunParallelTasks(dstProfilesToCreate, 20, c.ResourceGenerator.PrepareMinimalDowntimeResources, fastExit)
		if resCreationErr != nil {
			err = c.ResourceGenerator.ConnectionProfileCleanUp(ctx, resourcesForCleanup)
			if err != nil {
				return fmt.Errorf("resource generation failed due to %s, resources created could not be cleaned up, please cleanup manually: %s", resCreationErr.Error(), err.Error())
			} else {
				return resCreationErr
			}
		}
	}

	// If the errors occurred during validation of resource creation, return all errors
	if len(errorsList) != 0 {
		return multiError(errorsList)
	}
	// cleanup resources for cleanup if migration is successful
	resourcesForCleanup = nil
	return nil
}

// checks if source connection profile exists, if not, returns a request reuired to create it
func getSourceConnectionProfileForCreation(ctx context.Context, projectId string, profile *profiles.DataShard, region string, validateOnly bool, connectionProfiles map[string][]string, dsAcc datastream_accessor.DatastreamAccessor, dsClient ds.DatastreamClient) (*ConnectionProfileReq, error) {
	sourceProfileExists := false
	if profile.SrcConnectionProfile.Name != "" {
		// If location is not provided set it to spanner region
		if profile.SrcConnectionProfile.Location == "" {
			profile.SrcConnectionProfile.Location = region
		}
		var err error
		// Check if source connection profile exists
		sourceProfileExists, err = dsAcc.ConnectionProfileExists(ctx, dsClient, projectId, profile.SrcConnectionProfile.Name, profile.SrcConnectionProfile.Location, connectionProfiles)
		if err != nil {
			return nil, err
		}
	}

	if !sourceProfileExists {
		id := profile.SrcConnectionProfile.Name
		if id == "" {
			id = "hb-cnp-" + uuid.New().String()
			profile.SrcConnectionProfile.Name = id
		}
		if profile.SrcConnectionProfile.Location == "" {
			profile.SrcConnectionProfile.Location = region
		}
		req:= &ConnectionProfileReq{
			ConnectionProfile: ConnectionProfile{
				ProjectId:    projectId,
				DatashardId:  profile.DataShardId,
				Id:           profile.SrcConnectionProfile.Name,
				IsSource:     true,
				Host:         profile.SrcConnectionProfile.Host,
				Port:         profile.SrcConnectionProfile.Port,
				Password:     profile.SrcConnectionProfile.Password,
				User:         profile.SrcConnectionProfile.User,
				Region:       profile.SrcConnectionProfile.Location,
				ValidateOnly: validateOnly},
			Ctx: ctx,
		}
		return req, nil
	}
	return nil, nil
}

// checks if target connection profile exists, if not, returns a request reuired to create it
func getDstConnectionProfileForCreation(ctx context.Context, projectId string, profile *profiles.DataShard, region string, validateOnly bool, connectionProfiles map[string][]string, dsAcc datastream_accessor.DatastreamAccessor, dsClient ds.DatastreamClient) (*ConnectionProfileReq, error) {
	dstProfileExists := false
	var err error
	// Destination connection profiles do not need to be validated as for their creation gcs bucket will also be created
	if profile.DstConnectionProfile.Name != "" && !validateOnly {
		// Check if destination connection profile exists
		dstProfileExists, err = dsAcc.ConnectionProfileExists(ctx, dsClient, projectId, profile.DstConnectionProfile.Name, profile.DstConnectionProfile.Location, connectionProfiles)
		if err != nil {
			return nil, err
		}
	}

	if !dstProfileExists && !validateOnly {
		id := profile.DstConnectionProfile.Name
		if id == "" {
			id = "hb-cnp-" + uuid.New().String()
			profile.DstConnectionProfile.Name = id
		}
		if profile.DstConnectionProfile.Location == "" {
			profile.DstConnectionProfile.Location = region
		}
		req := &ConnectionProfileReq{
			ConnectionProfile: ConnectionProfile{
				ProjectId:    projectId,
				DatashardId:  profile.DataShardId,
				Id:           id,
				IsSource:     false,
				Region:       profile.DstConnectionProfile.Location,
				ValidateOnly: false},
			Ctx: ctx,
		}
		return req, nil
	}
	return nil, nil
}

// Sets Profile for resource creation
func setConnectionProfileFromRequest(details *ConnectionProfileReq, req *datastreampb.CreateConnectionProfileRequest) error {
	if details.ConnectionProfile.IsSource {
		port, err := strconv.ParseInt((details.ConnectionProfile.Port), 10, 32)
		if err != nil {
			return err
		}
		req.ConnectionProfile.Profile = &datastreampb.ConnectionProfile_MysqlProfile{
			MysqlProfile: &datastreampb.MysqlProfile{
				Hostname: details.ConnectionProfile.Host,
				Port:     int32(port),
				Username: details.ConnectionProfile.User,
				Password: details.ConnectionProfile.Password,
			},
		}
	} else {
		req.ConnectionProfile.Profile = &datastreampb.ConnectionProfile_GcsProfile{
			GcsProfile: &datastreampb.GcsProfile{
				Bucket:   details.ConnectionProfile.BucketName,
				RootPath: "/",
			},
		}
		return nil
	}
	return nil
}

// Clubs multiple errors into one error
func multiError(errorMessages []error) error {
	var errorStrings []string
	for _, err := range errorMessages {
		errorStrings = append(errorStrings, err.Error())
	}
	return errors.New(strings.Join(errorStrings, "\n "))
}
