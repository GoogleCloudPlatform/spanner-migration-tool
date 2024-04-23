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
package datastreamclient_test

import (
	"context"

	datastream "cloud.google.com/go/datastream/apiv1"
	datastreampb "cloud.google.com/go/datastream/apiv1/datastreampb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/operation"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/mock"
)

// Mock that implements the DatastreamClient interface.
// Pass in unit tests where DatastreamClient is an input parameter.
type DatastreamClientMock struct {
	mock.Mock
}

func (m *DatastreamClientMock) CreateStream(ctx context.Context, req *datastreampb.CreateStreamRequest, opts ...gax.CallOption) (*operation.OperationWrapper[datastreampb.Stream], error) {
	args := m.Called(ctx, req, opts)
	// Avoid panic for typeassertion due to null pointer.
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*operation.OperationWrapper[datastreampb.Stream]), args.Error(1)
}
func (m *DatastreamClientMock) UpdateStream(ctx context.Context, req *datastreampb.UpdateStreamRequest, opts ...gax.CallOption) (*operation.OperationWrapper[datastreampb.Stream], error) {
	args := m.Called(ctx, req, opts)
	// Avoid panic for typeassertion due to null pointer.
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*operation.OperationWrapper[datastreampb.Stream]), args.Error(1)
}
func (m *DatastreamClientMock) GetConnectionProfile(ctx context.Context, connectionName string) (*datastreampb.ConnectionProfile, error) {
	args := m.Called(ctx, connectionName)
	return args.Get(0).(*datastreampb.ConnectionProfile), args.Error(1)
}

func (m *DatastreamClientMock) ListConnectionProfiles(ctx context.Context, listRequest *datastreampb.ListConnectionProfilesRequest, opts ...gax.CallOption) *datastream.ConnectionProfileIterator {
	args := m.Called(ctx, listRequest, opts)
	return args.Get(0).(*datastream.ConnectionProfileIterator)
}

func (m *DatastreamClientMock) DeleteConnectionProfile(ctx context.Context, deleteRequest *datastreampb.DeleteConnectionProfileRequest) (*operation.NilOperationWrapper, error) {
	args := m.Called(ctx, deleteRequest)
	return args.Get(0).(*operation.NilOperationWrapper), args.Error(1)
}

func (m *DatastreamClientMock) CreateConnectionProfile(ctx context.Context, createRequest *datastreampb.CreateConnectionProfileRequest) (*operation.OperationWrapper[datastreampb.ConnectionProfile], error) {
	args := m.Called(ctx, createRequest)
	return args.Get(0).(*operation.OperationWrapper[datastreampb.ConnectionProfile]), args.Error(1)
}
