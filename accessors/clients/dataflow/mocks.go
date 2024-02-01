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

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"github.com/googleapis/gax-go/v2"
)

// Mock that implements the DataflowClient interface.
// Pass in unit tests where DataflowClient is an input parameter.
type DataflowClientMock struct {
	LaunchFlexTemplateMock func(ctx context.Context, req *dataflowpb.LaunchFlexTemplateRequest, opts ...gax.CallOption) (*dataflowpb.LaunchFlexTemplateResponse, error)
}

func (dcm *DataflowClientMock) LaunchFlexTemplate(ctx context.Context, req *dataflowpb.LaunchFlexTemplateRequest, opts ...gax.CallOption) (*dataflowpb.LaunchFlexTemplateResponse, error) {
	return dcm.LaunchFlexTemplateMock(ctx, req, opts...)
}
