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
package cloudsql

import (
	"context"
	"net"

	"cloud.google.com/go/cloudsqlconn"
)

// Use this interface instead of storage.Client to support mocking.
type CloudSqlConnDialer interface {
	Dial(ctx context.Context, instance string, opts ...cloudsqlconn.DialOption) (conn net.Conn, err error)
}

type CloudSqlConnDialerImpl struct {
	Dialer *cloudsqlconn.Dialer
}

func NewCloudSqlConnDialerImpl(ctx context.Context) (*CloudSqlConnDialerImpl, error) {
	d, err := GetOrCreateClient(ctx)
	if err != nil {
		return nil, err
	}
	return &CloudSqlConnDialerImpl{Dialer: d}, nil
}

func (c *CloudSqlConnDialerImpl) Dial(ctx context.Context, instance string, opts ...cloudsqlconn.DialOption) (conn net.Conn, err error){
	return c.Dialer.Dial(ctx, instance, opts...)
}