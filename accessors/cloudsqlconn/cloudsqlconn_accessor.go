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

package cloudsqlconnaccessor

import (
	"context"
	"net"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cloudsql"
)

// The DataflowAccessor provides methods that internally use the dataflow client. Methods should only contain generic logic here that can be used by multiple workflows.
type CloudSqlConnAccessor interface {
	Dial(ctx context.Context, d cloudsql.CloudSqlConnDialerImpl, instance string, opts ...cloudsqlconn.DialOption) (conn net.Conn, err error)
}

type CloudSqlConnAccessorImpl struct {}

func (csca *CloudSqlConnAccessorImpl) Dial(ctx context.Context, d cloudsql.CloudSqlConnDialer, instance string, opts ...cloudsqlconn.DialOption) (conn net.Conn, err error) {
	return d.Dial(ctx, instance, opts...)
}