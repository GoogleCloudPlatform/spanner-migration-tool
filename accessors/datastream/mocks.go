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

	"cloud.google.com/go/datastream/apiv1/datastreampb"
	datastreamclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/datastream"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

type DatastreamAccessorMock struct {
	FetchTargetBucketAndPathMock func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectID string, datastreamDestinationConnCfg streaming.DstConnCfg) (string, string, error)
	DeleteConnectionProfileMock func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, id string, projectId string, region string) error
	GetConnProfilesRegionMock func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, region string) ([]string, error)
	CreateConnectionProfileMock func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error)
	ConnectionProfileExistsMock func(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error)
}

func (dam *DatastreamAccessorMock) FetchTargetBucketAndPath(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectID string, datastreamDestinationConnCfg streaming.DstConnCfg) (string, string, error) {
	return dam.FetchTargetBucketAndPathMock(ctx, datastreamClient, projectID, datastreamDestinationConnCfg)
}

func (dam *DatastreamAccessorMock) DeleteConnectionProfile(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, id string, projectId string, region string) error {
	return dam.DeleteConnectionProfileMock(ctx, datastreamClient, id, projectId, region)
}

func (dam *DatastreamAccessorMock) GetConnProfilesRegion(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, region string) ([]string, error) {
	return dam.GetConnProfilesRegionMock(ctx, datastreamClient, projectId, region)
}

func (dam *DatastreamAccessorMock) CreateConnectionProfile(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, req *datastreampb.CreateConnectionProfileRequest) (*datastreampb.ConnectionProfile, error) {
	return dam.CreateConnectionProfileMock(ctx, datastreamClient, req)
}

func (dam *DatastreamAccessorMock) ConnectionProfileExists(ctx context.Context, datastreamClient datastreamclient.DatastreamClient, projectId string, profileName string, profileLocation string, connectionProfiles map[string][]string) (bool, error) {
	return dam.ConnectionProfileExistsMock(ctx, datastreamClient, projectId, profileName, profileLocation, connectionProfiles)
}
