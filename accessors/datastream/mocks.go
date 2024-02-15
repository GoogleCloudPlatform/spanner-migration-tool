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

package datastream_accessor

// import (
// 	"context"

// 	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/datastream"
// 	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
// )

// // Mock that implements the DatastreamAccessor interface.
// // Pass in unit tests where DatastreamAccessor is an input parameter.
// type DatastreamAccessorMock struct {
// 	FetchTargetBucketAndPathMock func(ctx context.Context, datastreamClient datastream.DatastreamClient, projectID string, datastreamDestinationConnCfg streaming.DstConnCfg) (string, string, error)
// }

// func (d *DatastreamAccessorMock) FetchTargetBucketAndPath (ctx context.Context, datastreamClient datastream.DatastreamClient, projectID string, datastreamDestinationConnCfg streaming.DstConnCfg) (string, string, error) {
// 	return d.FetchTargetBucketAndPathMock(ctx, datastreamClient, projectID, datastreamDestinationConnCfg)
// }
