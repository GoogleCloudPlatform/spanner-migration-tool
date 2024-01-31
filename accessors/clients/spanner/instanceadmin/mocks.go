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

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/googleapis/gax-go/v2"
)

type InstanceAdminClientMock struct {
	GetInstanceMock       func(ctx context.Context, req *instancepb.GetInstanceRequest, opts ...gax.CallOption) (*instancepb.Instance, error)
	GetInstanceConfigMock func(ctx context.Context, req *instancepb.GetInstanceConfigRequest, opts ...gax.CallOption) (*instancepb.InstanceConfig, error)
}

func (iac *InstanceAdminClientMock) GetInstance(ctx context.Context, req *instancepb.GetInstanceRequest, opts ...gax.CallOption) (*instancepb.Instance, error) {
	return iac.GetInstanceMock(ctx, req, opts...)
}

func (iac *InstanceAdminClientMock) GetInstanceConfig(ctx context.Context, req *instancepb.GetInstanceConfigRequest, opts ...gax.CallOption) (*instancepb.InstanceConfig, error) {
	return iac.GetInstanceConfigMock(ctx, req, opts...)
}
