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
package dataflowaccessor

import (
	"context"

	dataflowclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/dataflow"
)

// Mock that implements the DataflowAccessor interface.
// Pass in unit tests where DataflowAccessor is an input parameter.
type DataflowAccessorMock struct {
	LaunchFlexTemplateMock func(ctx context.Context, c dataflowclient.DataflowClient, parameters map[string]string, cfg DataflowTuningConfig) (string, string, error)
}

func (d *DataflowAccessorMock) LaunchDataflowTemplate(ctx context.Context, c dataflowclient.DataflowClient, parameters map[string]string, cfg DataflowTuningConfig) (string, string, error) {
	return d.LaunchFlexTemplateMock(ctx, c, parameters, cfg)
}
