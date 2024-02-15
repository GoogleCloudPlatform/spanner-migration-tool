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
package datastream

import (
	"context"

	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/datastream/apiv1/datastreampb"
	"github.com/googleapis/gax-go/v2"
)

// Mock that implements the DataflowClient interface.
// Pass in unit tests where DataflowClient is an input parameter.
type DatastreamClientMock struct {
	GetConnectionProfileMock func (ctx context.Context, connectionName string)  (*datastreampb.ConnectionProfile, error)
	ListConnectionProfilesMock func (ctx context.Context, listRequest *datastreampb.ListConnectionProfilesRequest, opts ...gax.CallOption) *datastream.ConnectionProfileIterator
	DeleteConnectionProfileMock func (ctx context.Context, deleteRequest *datastreampb.DeleteConnectionProfileRequest) (DeleteConnectionProfileOperation, error)
	CreateConnectionProfileMock func(ctx context.Context, createRequest *datastreampb.CreateConnectionProfileRequest) (CreateConnectionProfileOperation, error)
}

func (dsm *DatastreamClientMock) GetConnectionProfile(ctx context.Context, connectionName string)  (*datastreampb.ConnectionProfile, error) {
	return dsm.GetConnectionProfileMock(ctx, connectionName)
}

func (dsm *DatastreamClientMock) ListConnectionProfiles(ctx context.Context, listRequest *datastreampb.ListConnectionProfilesRequest, opts ...gax.CallOption) *datastream.ConnectionProfileIterator {
	return dsm.ListConnectionProfilesMock(ctx, listRequest, opts...)
}

func (dsm *DatastreamClientMock) DeleteConnectionProfile(ctx context.Context, deleteRequest *datastreampb.DeleteConnectionProfileRequest) (DeleteConnectionProfileOperation, error) {
	return dsm.DeleteConnectionProfileMock(ctx, deleteRequest)
}

type DeleteConnectionProfileOperationMock struct{
	WaitMock func(ctx context.Context) error
}

func (dso *DeleteConnectionProfileOperationMock) Wait(ctx context.Context) error {
	return dso.WaitMock(ctx)
}

func (dsm *DatastreamClientMock) CreateConnectionProfile(ctx context.Context, createRequest *datastreampb.CreateConnectionProfileRequest) (CreateConnectionProfileOperation, error) {
	return dsm.CreateConnectionProfileMock(ctx, createRequest)
}

type CreateConnectionProfileOperationMock struct{
	WaitMock func(ctx context.Context) (*datastreampb.ConnectionProfile, error)
}

func (dso *CreateConnectionProfileOperationMock) Wait(ctx context.Context) (*datastreampb.ConnectionProfile, error) {
	return dso.WaitMock(ctx)
}


