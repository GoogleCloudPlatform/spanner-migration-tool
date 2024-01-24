// Copyright 2023 Google LLC
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

	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/datastream/apiv1/datastreampb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

type resourceGenerationInterface interface {
	multiError(errorMessages []error) error
	getConnProfilesRegion(ctx context.Context, projectId string, region string, dsClient *datastream.Client) ([]string, error)
	connectionProfileExists(ctx context.Context, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string, dsClient *datastream.Client, s resourceGenerationInterface) (bool, error)
	getResourcesForCreation(ctx context.Context, projectId string, sourceProfile profiles.SourceProfile, region string, validateOnly bool, dsClient *datastream.Client) ([]*ConnectionProfileReq, []*ConnectionProfileReq, error)
	PrepareMinimalDowntimeResources(createResourceData *ConnectionProfileReq, mutex *sync.Mutex) common.TaskResult[*ConnectionProfileReq]
	setConnectionProfileFromRequest(details *ConnectionProfileReq, req *datastreampb.CreateConnectionProfileRequest) error
	GetSpannerRegion(ctx context.Context, projectId string, instanceName string) (string, error)
	connectionProfileCleanUp(ctx context.Context, profiles []*ConnectionProfileReq) error
	ValidateResourceGeneration(ctx context.Context, projectId string, instanceId string, sourceProfile profiles.SourceProfile, conv *internal.Conv) error
}

type ResourceGenerationStruct struct {
}

type MigrationResources struct{}

func (r ResourceGenerationStruct) multiError(errorMessages []error) error {
	var errorStrings []string
	for _, err := range errorMessages {
		errorStrings = append(errorStrings, err.Error())
	}
	return errors.New(strings.Join(errorStrings, "\n "))
}

// Method to validate if in a minimal downtime migration, required resources can be generated
func (r ResourceGenerationStruct) ValidateResourceGeneration(ctx context.Context, projectId string, instanceId string, sourceProfile profiles.SourceProfile, conv *internal.Conv) error {
	resGenerator := ResourceGenerationStruct{}
	spannerRegion, err := resGenerator.GetSpannerRegion(ctx, projectId, instanceId)
	dsClient := GetDatastreamClient(ctx)
	if err != nil {
		err = fmt.Errorf("unable to fetch Spanner Region: %v", err)
		return err
	}
	conv.SpRegion = spannerRegion
	err = resGenerator.CreateResourcesForShardedMigration(ctx, projectId, instanceId, true, spannerRegion, sourceProfile, dsClient)
	if err != nil {
		err = fmt.Errorf("unable to create connection profiles: %v", err)
		return err
	}
	conv.ResourceValidation = true
	return nil
}

// 1. If destination connection profile needs to be created, creates a gcs bucket
// 2. Creates the connection profile needed for migration
func (r ResourceGenerationStruct) PrepareMinimalDowntimeResources(createResourceData *ConnectionProfileReq, mutex *sync.Mutex) common.TaskResult[*ConnectionProfileReq] {
	dsClient, err := datastream.NewClient(createResourceData.Ctx)
	if err != nil {
		createResourceData.Error = err
		return common.TaskResult[*ConnectionProfileReq]{Result: createResourceData, Err: err}
	}
	defer dsClient.Close()

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
		err = utils.CreateGCSBucket(bucketName, createResourceData.ConnectionProfile.ProjectId, createResourceData.ConnectionProfile.Region)
		if err != nil {
			createResourceData.Error = err
			return common.TaskResult[*ConnectionProfileReq]{Result: createResourceData, Err: err}
		}
	}
	createResourceData.ConnectionProfile.BucketName = bucketName

	// Set Profile for resource creation
	r.setConnectionProfileFromRequest(createResourceData, req)

	// Create or Validate Resource
	op, err := dsClient.CreateConnectionProfile(createResourceData.Ctx, req)
	if err != nil {
		createResourceData.Error = err
		return common.TaskResult[*ConnectionProfileReq]{Result: createResourceData, Err: err}
	}
	_, err = op.Wait(createResourceData.Ctx)
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

// Sets Profile for resource creation
func (r ResourceGenerationStruct) setConnectionProfileFromRequest(details *ConnectionProfileReq, req *datastreampb.CreateConnectionProfileRequest) error {
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

func (r ResourceGenerationStruct) GetSpannerRegion(ctx context.Context, projectId string, instanceName string) (string, error) {
	instanceAdmin, _ := instance.NewInstanceAdminClient(ctx)
	defer instanceAdmin.Close()
	region := ""
	spannerInstance, err := instanceAdmin.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: "projects/" + projectId + "/instances/" + instanceName})
	if err != nil {
		return region, err
	}
	instanceConfig, err := instanceAdmin.GetInstanceConfig(ctx, &instancepb.GetInstanceConfigRequest{Name: spannerInstance.Config})
	if err != nil {
		return region, err
	}
	for _, replica := range instanceConfig.Replicas {
		if replica.DefaultLeaderLocation {
			region = replica.Location
		}
	}
	return region, nil
}

// Returns connection profiles for a given region
func (r *ResourceGenerationStruct) getConnProfilesRegion(ctx context.Context, projectId string, region string, dsClient *datastream.Client) ([]string, error) {
	profilesIt := dsClient.ListConnectionProfiles(ctx, &datastreampb.ListConnectionProfilesRequest{Parent: "projects/" + projectId + "/locations/" + region})
	var profiles []string = []string{}
	for {
		resp, err := profilesIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return profiles, err
		} else {
			profiles = append(profiles, strings.Split(resp.Name, "/")[5])
		}
	}
	return profiles, nil
}

// returns true if connection profile exists else false
func (r *ResourceGenerationStruct) connectionProfileExists(ctx context.Context, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string, dsClient *datastream.Client, s resourceGenerationInterface) (bool, error) {
	// Check if connection profiles for the given region are fetched. if not, fetch them
	profiles, ok := connectionProfiles[profileLocation]
	var err error = nil
	if !ok {
		profiles, err = s.getConnProfilesRegion(ctx, projectId, profileLocation, dsClient)
		if err != nil {
			return false, err
		}
		connectionProfiles[profileLocation] = profiles
	}

	// Check if connection profile exists in the provided region
	for _, element := range profiles {
		if element == profileName {
			return true, nil
		}
	}

	return false, nil
}

// If any of the resource creation fails, deletes all resources that were created
func (r ResourceGenerationStruct) connectionProfileCleanUp(ctx context.Context, profiles []*ConnectionProfileReq) error {
	dsClient := GetDatastreamClient(ctx)
	for _, profile := range profiles {
		op, err := dsClient.DeleteConnectionProfile(ctx, &datastreampb.DeleteConnectionProfileRequest{
			Name: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", profile.ConnectionProfile.ProjectId, profile.ConnectionProfile.Region, profile.ConnectionProfile.Id),
		})

		if err != nil {
			return err
		}

		err = op.Wait(ctx)
		if err != nil {
			return err
		}

		if profile.ConnectionProfile.BucketName != "" {
			gcsClient, err := storage.NewClient(ctx)

			if err != nil {
				return err
			}

			if err := gcsClient.Bucket(profile.ConnectionProfile.BucketName).Delete(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// Returns source and destination connection profiles to be created
func (r *ResourceGenerationStruct) getResourcesForCreation(ctx context.Context, projectId string, sourceProfile profiles.SourceProfile, region string, validateOnly bool, dsClient *datastream.Client) ([]*ConnectionProfileReq, []*ConnectionProfileReq, error) {
	var sourceProfilesToCreate []*ConnectionProfileReq
	var dstProfilesToCreate []*ConnectionProfileReq

	// Map for each region with list of all connection profiles
	var connectionProfiles map[string][]string = make(map[string][]string)
	var err error = nil

	s := ResourceGenerationStruct{}
	for _, profile := range sourceProfile.Config.ShardConfigurationDataflow.DataShards {
		sourceProfileExists := false
		dstProfileExists := false
		if profile.SrcConnectionProfile.Name != "" {
			// If location is not provided set it to spanner region
			if profile.SrcConnectionProfile.Location == "" {
				profile.SrcConnectionProfile.Location = region
			}
			// Check if source connection profile exists
			sourceProfileExists, err = s.connectionProfileExists(ctx, projectId, profile.SrcConnectionProfile.Name, profile.SrcConnectionProfile.Location, connectionProfiles, dsClient, &s)
			if err != nil {
				return sourceProfilesToCreate, dstProfilesToCreate, err
			}
		}

		// Destination connection profiles do not need to be validated as for their creation gcs bucket will also be created
		if profile.DstConnectionProfile.Name != "" && !validateOnly {
			// Check if destination connection profile exists
			dstProfileExists, err = s.connectionProfileExists(ctx, projectId, profile.DstConnectionProfile.Name, profile.DstConnectionProfile.Location, connectionProfiles, dsClient, &s)
			if err != nil {
				return sourceProfilesToCreate, dstProfilesToCreate, err
			}
		}
		if !sourceProfileExists {
			id := profile.SrcConnectionProfile.Name
			if id == "" {
				id = "CN-" + uuid.New().String()
				profile.SrcConnectionProfile.Name = id
			}
			if profile.SrcConnectionProfile.Location == "" {
				profile.SrcConnectionProfile.Location = region
			}
			sourceProfilesToCreate = append(sourceProfilesToCreate, &ConnectionProfileReq{
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
			})
		}
		if !dstProfileExists && !validateOnly {
			id := profile.DstConnectionProfile.Name
			if id == "" {
				id = "CN-" + uuid.New().String()
				profile.DstConnectionProfile.Name = id
			}
			if profile.DstConnectionProfile.Location == "" {
				profile.DstConnectionProfile.Location = region
			}
			dstProfilesToCreate = append(dstProfilesToCreate, &ConnectionProfileReq{
				ConnectionProfile: ConnectionProfile{
					ProjectId:    projectId,
					DatashardId:  profile.DataShardId,
					Id:           id,
					IsSource:     false,
					Region:       profile.DstConnectionProfile.Location,
					ValidateOnly: false},
				Ctx: ctx,
			})
		}
	}
	return sourceProfilesToCreate, dstProfilesToCreate, err
}

// 1. For each datashard, check if source and destination connection profile exists or not
// 2. If source connection profile doesn't exists create it or validate if creation is possible.
// 3. If validation is false and destination connection profile doesn't exists create a corresponding gcs bucket and then a destination connection profile
func (r ResourceGenerationStruct) CreateResourcesForShardedMigration(ctx context.Context, projectId string, instanceName string, validateOnly bool, region string, sourceProfile profiles.SourceProfile, dsClient *datastream.Client) error {
	var sourceProfilesToCreate []*ConnectionProfileReq
	var dstProfilesToCreate []*ConnectionProfileReq

	// Fetches list with resources which do not exist and need to be created
	s := ResourceGenerationStruct{}
	sourceProfilesToCreate, dstProfilesToCreate, err := s.getResourcesForCreation(ctx, projectId, sourceProfile, region, validateOnly, dsClient)
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
	resSourceProfiles, resCreationErr := common.RunParallelTasks(sourceProfilesToCreate, 20, r.PrepareMinimalDowntimeResources, fastExit)
	// If creation failed, perform cleanup of resources
	if resCreationErr != nil && !validateOnly {
		err = r.connectionProfileCleanUp(ctx, resourcesForCleanup)
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
		_, resCreationErr := common.RunParallelTasks(dstProfilesToCreate, 20, r.PrepareMinimalDowntimeResources, fastExit)
		if resCreationErr != nil {
			err = r.connectionProfileCleanUp(ctx, resourcesForCleanup)
			if err != nil {
				return fmt.Errorf("resource generation failed due to %s, resources created could not be cleaned up, please cleanup manually: %s", resCreationErr.Error(), err.Error())
			} else {
				return resCreationErr
			}
		}
	}

	// If the errors occurred during validation of resource creation, return all errors
	if len(errorsList) != 0 {
		return r.multiError(errorsList)
	}
	// cleanup resources for cleanup if migration is successful
	resourcesForCleanup = nil
	return nil
}
