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
	"testing"

	"cloud.google.com/go/datastream/apiv1/datastreampb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/datastream"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/stretchr/testify/assert"
)

func TestFetchTargetBucketAndPath(t *testing.T) {
	dstConfig := streaming.DstConnCfg{
		Location: "region-X",
		Name: "profile-name",
		Prefix: "/",
	}
	ctx := context.Background()
	da := DatastreamAccessorImpl{}
	testCases := []struct{
		name              	string
		dsm					datastream.DatastreamClientMock	
		expectedBucketName  string
		expectedPrefix		string	
		expectError 		bool
	}{
		{
			name: "basic correct",
			dsm: datastream.DatastreamClientMock{
				GetConnectionProfileMock: func(ctx context.Context, connectionName string) (*datastreampb.ConnectionProfile, error){
					return &datastreampb.ConnectionProfile{Profile: &datastreampb.ConnectionProfile_GcsProfile{GcsProfile: &datastreampb.GcsProfile{Bucket: "bucket", RootPath: "/"}}}, nil
				},
			},
			expectedBucketName: "bucket",
			expectedPrefix: "/data/",
			expectError: false,
		},
		{
			name: "get connection profile error",
			dsm: datastream.DatastreamClientMock{
				GetConnectionProfileMock: func(ctx context.Context, connectionName string) (*datastreampb.ConnectionProfile, error){
					return nil, fmt.Errorf("error")
				},
			},
			expectedBucketName: "",
			expectedPrefix: "",
			expectError: true,
		},
	}
	for _, tc := range testCases {
		bucketName, prefix, err := da.FetchTargetBucketAndPath(ctx, &tc.dsm, "project-id", dstConfig)
		assert.Equal(t, tc.expectError, err != nil)
		assert.Equal(t, tc.expectedBucketName, bucketName)
		assert.Equal(t, tc.expectedPrefix, prefix)
	}
}

func TestDeleteConnectionProfile(t *testing.T) {
	ctx := context.Background()
	da := DatastreamAccessorImpl{}
	testCases := []struct{
		name              	string
		dsm					datastream.DatastreamClientMock	
		expectedBucketName  string
		expectedPrefix		string	
		expectError 		bool
	}{
		{
			name: "basic correct",
			dsm: datastream.DatastreamClientMock{
				DeleteConnectionProfileMock: func(ctx context.Context, deleteRequest *datastreampb.DeleteConnectionProfileRequest) (datastream.DeleteConnectionProfileOperation, error){
					return &datastream.DeleteConnectionProfileOperationMock{
						WaitMock: func(ctx context.Context) error { return nil },
					}, nil
				},
			},
			expectError: false,
		},
		{
			name: "delete connection profile error",
			dsm: datastream.DatastreamClientMock{
				DeleteConnectionProfileMock: func(ctx context.Context, deleteRequest *datastreampb.DeleteConnectionProfileRequest) (datastream.DeleteConnectionProfileOperation, error){
					return nil, fmt.Errorf("error")
				},
			},
			expectError: true,
		},
		{
			name: "operation wait error",
			dsm: datastream.DatastreamClientMock{
				DeleteConnectionProfileMock: func(ctx context.Context, deleteRequest *datastreampb.DeleteConnectionProfileRequest) (datastream.DeleteConnectionProfileOperation, error){
					return nil, fmt.Errorf("error")
				},
			},
			expectError: true,
		},
	}
	for _, tc := range testCases {
		err := da.DeleteConnectionProfile(ctx, &tc.dsm, "id", "project-id", "region")
		assert.Equal(t, tc.expectError, err != nil)
	}
}

func TestCreateConnectionProfile(t *testing.T) {
	ctx := context.Background()
	da := DatastreamAccessorImpl{}
	testCases := []struct{
		name              	string
		dsm					datastream.DatastreamClientMock	
		expectedBucketName  string
		expectedPrefix		string	
		expectError 		bool
	}{
		{
			name: "basic correct",
			dsm: datastream.DatastreamClientMock{
				CreateConnectionProfileMock: func(ctx context.Context, createRequest *datastreampb.CreateConnectionProfileRequest) (datastream.CreateConnectionProfileOperation, error){
					return &datastream.CreateConnectionProfileOperationMock{
						WaitMock: func(ctx context.Context) (*datastreampb.ConnectionProfile, error) { return &datastreampb.ConnectionProfile{}, nil },
					}, nil
				},
			},
			expectError: false,
		},
		{
			name: "create connection profile error",
			dsm: datastream.DatastreamClientMock{
				CreateConnectionProfileMock: func(ctx context.Context, createRequest *datastreampb.CreateConnectionProfileRequest) (datastream.CreateConnectionProfileOperation, error){
					return nil, fmt.Errorf("error")
				},
			},
			expectError: true,
		},
		{
			name: "operation wait error",
			dsm: datastream.DatastreamClientMock{
				CreateConnectionProfileMock: func(ctx context.Context, createRequest *datastreampb.CreateConnectionProfileRequest) (datastream.CreateConnectionProfileOperation, error){
					return &datastream.CreateConnectionProfileOperationMock{
						WaitMock: func(ctx context.Context) (*datastreampb.ConnectionProfile, error) { return nil, fmt.Errorf("error") },
					}, nil
				},
			},
			expectError: true,
		},
	}
	for _, tc := range testCases {
		_, err := da.CreateConnectionProfile(ctx, &tc.dsm, &datastreampb.CreateConnectionProfileRequest{})
		assert.Equal(t, tc.expectError, err != nil)
	}
}
