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
package spanneradmin

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/googleapis/gax-go/v2"
)

// Mock that implements the AdminClient interface.
// Pass in unit tests where AdminClient is an input parameter.
type AdminClientMock struct {
	GetDatabaseMock       func(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error)
	CreateDatabaseMock    func(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (CreateDatabaseOperation, error)
	UpdateDatabaseDdlMock func(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (UpdateDatabaseDdlOperation, error)
}

func (acm *AdminClientMock) GetDatabase(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
	return acm.GetDatabaseMock(ctx, req, opts...)
}

func (acm *AdminClientMock) CreateDatabase(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (CreateDatabaseOperation, error) {
	return acm.CreateDatabaseMock(ctx, req, opts...)
}

func (acm *AdminClientMock) UpdateDatabaseDdl(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (UpdateDatabaseDdlOperation, error) {
	return acm.UpdateDatabaseDdlMock(ctx, req, opts...)
}

// Mock that implements the CreateDatabaseOperation interface.
// Pass in unit tests where CreateDatabaseOperation is an input parameter.
type CreateDatabaseOperationMock struct {
	WaitMock func(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error)
}

func (dbo *CreateDatabaseOperationMock) Wait(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) {
	return dbo.WaitMock(ctx, opts...)
}

// Mock that implements the UpdateDatabaseDdlOperation interface.
// Pass in unit tests where UpdateDatabaseDdlOperation is an input parameter.
type UpdateDatabaseDdlOperationMock struct {
	WaitMock func(ctx context.Context, opts ...gax.CallOption) error
}

func (dbo *UpdateDatabaseDdlOperationMock) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return dbo.WaitMock(ctx, opts...)
}
