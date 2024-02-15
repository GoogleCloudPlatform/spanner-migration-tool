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
package datastreamclient

import (
	"context"

	datastream "cloud.google.com/go/datastream/apiv1"
	datastreampb "cloud.google.com/go/datastream/apiv1/datastreampb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/operation"
	"github.com/googleapis/gax-go/v2"
)

// Use this interface instead of datastream.FlexTemplatesClient to support mocking.
type DatastreamClient interface {
	CreateStream(ctx context.Context, req *datastreampb.CreateStreamRequest, opts ...gax.CallOption) (*operation.OperationWrapper[datastreampb.Stream], error)
	UpdateStream(ctx context.Context, req *datastreampb.UpdateStreamRequest, opts ...gax.CallOption) (*operation.OperationWrapper[datastreampb.Stream], error)
}

// This implements the DatastreamClient interface. This is the primary implementation that should be used in all places other than tests.
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

func (c *DatastreamClientImpl) CreateStream(ctx context.Context, req *datastreampb.CreateStreamRequest, opts ...gax.CallOption) (*operation.OperationWrapper[datastreampb.Stream], error) {
	o, e := c.client.CreateStream(ctx, req, opts...)
	if o == nil {
		return nil, e
	} else {
		ret := operation.NewOperationWrapper[datastreampb.Stream](o)
		return &ret, nil
	}
}
func (c *DatastreamClientImpl) UpdateStream(ctx context.Context, req *datastreampb.UpdateStreamRequest, opts ...gax.CallOption) (*operation.OperationWrapper[datastreampb.Stream], error) {
	o, e := c.client.UpdateStream(ctx, req, opts...)
	if o == nil {
		return nil, e
	} else {
		ret := operation.NewOperationWrapper[datastreampb.Stream](o)
		return &ret, nil
	}

}
