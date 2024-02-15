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

// Use this interface instead of dsClient.Client to support mocking.

type DatastreamClient interface {
	GetConnectionProfile(ctx context.Context, connectionName string)  (*datastreampb.ConnectionProfile, error)
	ListConnectionProfiles(ctx context.Context, listRequest *datastreampb.ListConnectionProfilesRequest, opts ...gax.CallOption) *datastream.ConnectionProfileIterator
	DeleteConnectionProfile(ctx context.Context, deleteRequest *datastreampb.DeleteConnectionProfileRequest) (DeleteConnectionProfileOperation, error)
	CreateConnectionProfile(ctx context.Context, createRequest *datastreampb.CreateConnectionProfileRequest) (CreateConnectionProfileOperation, error)
}

type DatastreamClientImpl struct {
	client *datastream.Client
}

func NewDatastreamClientImpl(ctx context.Context) (*DatastreamClientImpl, error) {
	c, err := GetOrCreateClient(ctx)
	if err != nil {
		return nil, err
	}
	return &DatastreamClientImpl{client: c}, nil
}

func (c *DatastreamClientImpl) GetConnectionProfile(ctx context.Context, connectionName string)  (*datastreampb.ConnectionProfile, error) {
	return c.client.GetConnectionProfile(ctx, &datastreampb.GetConnectionProfileRequest{Name: connectionName})
}

type DeleteConnectionProfileOperation interface {
	Wait(ctx context.Context) error
}

type DeleteConnectionProfileOperationImpl struct {
	dcpo *datastream.DeleteConnectionProfileOperation
}

func (d *DeleteConnectionProfileOperationImpl) Wait(ctx context.Context) error {
	return d.dcpo.Wait(ctx)
}

func (c *DatastreamClientImpl) DeleteConnectionProfile(ctx context.Context, deleteRequest *datastreampb.DeleteConnectionProfileRequest) (DeleteConnectionProfileOperation, error) {
	op, err := c.client.DeleteConnectionProfile(ctx, deleteRequest)
	if err != nil {
		return nil, err
	}
	return &DeleteConnectionProfileOperationImpl{dcpo: op}, nil
}

func (c *DatastreamClientImpl) ListConnectionProfiles(ctx context.Context, listRequest *datastreampb.ListConnectionProfilesRequest, opts ...gax.CallOption) *datastream.ConnectionProfileIterator {
	return c.client.ListConnectionProfiles(ctx, listRequest, opts...)
}

type CreateConnectionProfileOperation interface {
	Wait(ctx context.Context) (*datastreampb.ConnectionProfile, error)
}

type CreateConnectionProfileOperationImpl struct {
	ccpo *datastream.CreateConnectionProfileOperation
}

func (c *CreateConnectionProfileOperationImpl) Wait(ctx context.Context) (*datastreampb.ConnectionProfile, error) {
	return c.ccpo.Wait(ctx)
}

func (c *DatastreamClientImpl) CreateConnectionProfile(ctx context.Context, createRequest *datastreampb.CreateConnectionProfileRequest) (CreateConnectionProfileOperation, error) {
	op, err := c.client.CreateConnectionProfile(ctx, createRequest)
	if err != nil {
		return nil, err
	}
	return &CreateConnectionProfileOperationImpl{ccpo: op}, nil
}
