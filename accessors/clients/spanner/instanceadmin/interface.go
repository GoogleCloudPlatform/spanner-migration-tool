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
package spinstanceadmin

import (
	"context"

	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/googleapis/gax-go/v2"
)

// Use this interface instead of instance.InstanceAdminClient to support mocking.
type InstanceAdminClient interface {
	GetInstance(ctx context.Context, req *instancepb.GetInstanceRequest, opts ...gax.CallOption) (*instancepb.Instance, error)
	GetInstanceConfig(ctx context.Context, req *instancepb.GetInstanceConfigRequest, opts ...gax.CallOption) (*instancepb.InstanceConfig, error)
}

// This implements the InstanceAdminClient interface. This is the primary implementation that should be used in all places other than tests.
type InstanceAdminClientImpl struct {
	client *instance.InstanceAdminClient
}

func NewInstanceAdminClientImpl(ctx context.Context) (*InstanceAdminClientImpl, error) {
	c, err := GetOrCreateClient(ctx)
	if err != nil {
		return nil, err
	}
	return &InstanceAdminClientImpl{client: c}, nil
}

func (c *InstanceAdminClientImpl) GetInstance(ctx context.Context, req *instancepb.GetInstanceRequest, opts ...gax.CallOption) (*instancepb.Instance, error) {
	return c.client.GetInstance(ctx, req, opts...)
}

func (c *InstanceAdminClientImpl) GetInstanceConfig(ctx context.Context, req *instancepb.GetInstanceConfigRequest, opts ...gax.CallOption) (*instancepb.InstanceConfig, error) {
	return c.client.GetInstanceConfig(ctx, req, opts...)
}
