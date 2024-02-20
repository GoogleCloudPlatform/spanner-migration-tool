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

package datastream_accessor_test

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/datastream/apiv1/datastreampb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/datastream"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/operation"
	datastream_accessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/datastream"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestFetchTargetBucketAndPath(t *testing.T) {
	dstConfig := streaming.DstConnCfg{
		Location: "region-X",
		Name:     "profile-name",
		Prefix:   "/",
	}
	ctx := context.Background()
	da := datastream_accessor.DatastreamAccessorImpl{}
	testCases := []struct {
		name               string
		dsm                datastreamclient.DatastreamClientMock
		connectionProfile  *datastreampb.ConnectionProfile
		getProfileErr      error
		expectedBucketName string
		expectedPrefix     string
		expectError        bool
	}{
		{
			name:               "basic correct",
			connectionProfile:  &datastreampb.ConnectionProfile{Profile: &datastreampb.ConnectionProfile_GcsProfile{GcsProfile: &datastreampb.GcsProfile{Bucket: "bucket", RootPath: "/"}}},
			getProfileErr:      nil,
			expectedBucketName: "bucket",
			expectedPrefix:     "/data/",
			expectError:        false,
		},
		{
			name:               "get connection profile error",
			connectionProfile:  nil,
			getProfileErr:      fmt.Errorf("error"),
			expectedBucketName: "",
			expectedPrefix:     "",
			expectError:        true,
		},
		{
			name:               "empty string",
			connectionProfile:  &datastreampb.ConnectionProfile{Profile: &datastreampb.ConnectionProfile_GcsProfile{GcsProfile: &datastreampb.GcsProfile{Bucket: "", RootPath: ""}}},
			getProfileErr:      nil,
			expectedBucketName: "",
			expectedPrefix:     "data/",
			expectError:        false,
		},
	}
	for _, tc := range testCases {
		dsm := datastreamclient.DatastreamClientMock{}
		dsm.On("GetConnectionProfile", mock.Anything, mock.Anything).Return(tc.connectionProfile, tc.getProfileErr)
		bucketName, prefix, err := da.FetchTargetBucketAndPath(ctx, &dsm, "project-id", dstConfig)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		assert.Equal(t, tc.expectedBucketName, bucketName, tc.name)
		assert.Equal(t, tc.expectedPrefix, prefix, tc.name)
	}
}

func TestDeleteConnectionProfile(t *testing.T) {
	ctx := context.Background()
	da := datastream_accessor.DatastreamAccessorImpl{}
	testCases := []struct {
		name                 string
		op                   *operation.MockNilOperation
		deleteConnProfileErr error
		expectError          bool
	}{
		{
			name: "basic correct",
			op: &operation.MockNilOperation{
				RetErr: nil,
			},
			expectError: false,
		},
		{
			name:                 "delete connection profile error",
			op:                   nil,
			deleteConnProfileErr: fmt.Errorf("error"),
			expectError:          true,
		},
		{
			name: "operation wait error",
			op: &operation.MockNilOperation{
				RetErr: fmt.Errorf("error"),
			},
			expectError: true,
		},
	}
	for _, tc := range testCases {
		dsm := datastreamclient.DatastreamClientMock{}
		op := operation.NewNilOperationWrapper(tc.op)
		dsm.On("DeleteConnectionProfile", mock.Anything, mock.Anything).Return(&op, tc.deleteConnProfileErr)
		err := da.DeleteConnectionProfile(ctx, &dsm, "id", "project-id", "region")
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestCreateConnectionProfile(t *testing.T) {
	ctx := context.Background()
	da := datastream_accessor.DatastreamAccessorImpl{}
	testCases := []struct {
		name               string
		op                 operation.MockOperation[datastreampb.ConnectionProfile]
		createProfileError error
		expectError        bool
	}{
		{
			name: "basic correct",
			op: operation.MockOperation[datastreampb.ConnectionProfile]{
				RetVal: &datastreampb.ConnectionProfile{},
			},
			createProfileError: nil,
			expectError:        false,
		},
		{
			name: "create connection profile error",
			op: operation.MockOperation[datastreampb.ConnectionProfile]{
				RetVal: &datastreampb.ConnectionProfile{},
			},
			createProfileError: fmt.Errorf("error"),
			expectError:        true,
		},
		{
			name: "operation wait error",
			op: operation.MockOperation[datastreampb.ConnectionProfile]{
				RetVal: &datastreampb.ConnectionProfile{},
				RetErr: fmt.Errorf("error"),
			},
			createProfileError: nil,
			expectError:        true,
		},
	}
	for _, tc := range testCases {
		dsm := datastreamclient.DatastreamClientMock{}
		op := operation.NewOperationWrapper[datastreampb.ConnectionProfile](tc.op)
		dsm.On("CreateConnectionProfile", mock.Anything, mock.Anything).Return(&op, tc.createProfileError)
		_, err := da.CreateConnectionProfile(ctx, &dsm, &datastreampb.CreateConnectionProfileRequest{})
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}
