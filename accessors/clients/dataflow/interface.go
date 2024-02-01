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
package dataflowclient

import (
	"context"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"github.com/googleapis/gax-go/v2"
)

// Use this interface instead of dataflow.FlexTemplatesClient to support mocking.
type DataflowClient interface {
	LaunchFlexTemplate(ctx context.Context, req *dataflowpb.LaunchFlexTemplateRequest, opts ...gax.CallOption) (*dataflowpb.LaunchFlexTemplateResponse, error)
}

// This implements the DataflowClient interface. This is the primary implementation that should be used in all places other than tests.
type DataflowClientImpl struct {
	client *dataflow.FlexTemplatesClient
}

func NewDataflowClientImpl(ctx context.Context) (*DataflowClientImpl, error) {
	c, err := GetOrCreateClient(ctx)
	if err != nil {
		return nil, err
	}
	return &DataflowClientImpl{client: c}, nil
}

func (c *DataflowClientImpl) LaunchFlexTemplate(ctx context.Context, req *dataflowpb.LaunchFlexTemplateRequest, opts ...gax.CallOption) (*dataflowpb.LaunchFlexTemplateResponse, error) {
	return c.client.LaunchFlexTemplate(ctx, req, opts...)
}
