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

package datastream_accessor

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/datastream/apiv1/datastreampb"
	datastreamclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/datastream"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"google.golang.org/api/iterator"
)

// The DatastreamAccessor provides methods that internally use the datstreamclient. Methods should only contain generic logic here that can be used by multiple workflows.
type DatastreamAccessor interface {
	FetchTargetBucketAndPath(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectID string, datastreamDestinationConnCfg streaming.DstConnCfg) (string, string, error)
	DeleteConnectionProfile(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, id string, projectId string, region string) error
	GetConnProfilesRegion(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, region string) ([]string, error)
	CreateConnectionProfile(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error)
	ConnectionProfileExists(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error)
}
type DatastreamAccessorImpl struct{}

// FetchTargetBucketAndPath fetches the bucket and path name from a Datastream destination config.
func (da *DatastreamAccessorImpl) FetchTargetBucketAndPath(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectID string, datastreamDestinationConnCfg streaming.DstConnCfg) (string, string, error) {
	if datastreamClient == nil {
		return "", "", fmt.Errorf("datastream client could not be created")
	}
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamDestinationConnCfg.Location, datastreamDestinationConnCfg.Name)
	// `GetConnectionProfile` has out of box retries. Ref - https://github.com/googleapis/googleapis/blob/master/google/cloud/datastream/v1/datastream_grpc_service_config.json
	res, err := datastreamClient.GetConnectionProfile(ctx, dstProf)
	if err != nil {
		return "", "", fmt.Errorf("could not get connection profiles: %v", err)
	}
	// Fetch the GCS path from the target connection profile.
	// The Get calls for Google Cloud Storage API have out of box retries.
	// Reference - https://cloud.google.com/storage/docs/retry-strategy#idempotency-operations
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	bucketName := gcsProfile.Bucket
	prefix := gcsProfile.RootPath + datastreamDestinationConnCfg.Prefix
	prefix = utils.ConcatDirectoryPath(prefix, "data/")
	return bucketName, prefix, nil
}

// Deletes a connection Profile
func (da *DatastreamAccessorImpl) DeleteConnectionProfile(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, id string, projectId string, region string) error {
	op, err := datastreamClient.DeleteConnectionProfile(ctx, &datastreampb.DeleteConnectionProfileRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectId, region, id),
	})
	if err != nil {
		return err
	}

	err = op.Wait(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Creates new connection Profile
func (da *DatastreamAccessorImpl) CreateConnectionProfile(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error) {
	op, err := datastreamClient.CreateConnectionProfile(ctx, req)
	if err != nil {
		return nil, err
	}

	return op.Wait(ctx)
}

// Gets all connection profiles in a region
func (da *DatastreamAccessorImpl) GetConnProfilesRegion(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, region string) ([]string, error) {
	profilesIt := datastreamClient.ListConnectionProfiles(ctx, &datastreampb.ListConnectionProfilesRequest{Parent: "projects/" + projectId + "/locations/" + region})
	var profiles []string = []string{}
	for {
		resp, err := profilesIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		} else {
			profiles = append(profiles, strings.Split(resp.Name, "/")[5])
		}
	}
	return profiles, nil
}

// returns true if connection profile exists in a provided region else false
func (da *DatastreamAccessorImpl) ConnectionProfileExists(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error) {
	// Check if connection profiles for the given region are fetched. if not, fetch them
	profiles, ok := connectionProfiles[profileLocation]
	var err error = nil
	if !ok {
		profiles, err = da.GetConnProfilesRegion(ctx, datastreamClient, projectId, profileLocation)
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
