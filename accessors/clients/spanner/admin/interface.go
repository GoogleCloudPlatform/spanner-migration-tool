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

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/googleapis/gax-go/v2"
)

type AdminClient interface {
	GetDatabase(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error)
	CreateDatabase(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (CreateDatabaseOperation, error)
	UpdateDatabaseDdl(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (UpdateDatabaseDdlOperation, error)
}

type CreateDatabaseOperation interface {
	Wait(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error)
}

type UpdateDatabaseDdlOperation interface {
	Wait(ctx context.Context, opts ...gax.CallOption) error
}

type AdminClientImpl struct {
	adminClient *database.DatabaseAdminClient
}

func NewAdminClientImpl(ctx context.Context) (*AdminClientImpl, error) {
	c, err := GetOrCreateClient(ctx)
	if err != nil {
		return nil, err
	}
	return &AdminClientImpl{adminClient: c}, nil
}

func (c *AdminClientImpl) GetDatabase(ctx context.Context, req *databasepb.GetDatabaseRequest, opts ...gax.CallOption) (*databasepb.Database, error) {
	return c.adminClient.GetDatabase(ctx, req, opts...)
}

func (c *AdminClientImpl) CreateDatabase(ctx context.Context, req *databasepb.CreateDatabaseRequest, opts ...gax.CallOption) (CreateDatabaseOperation, error) {
	op, err := c.adminClient.CreateDatabase(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	return &CreateDatabaseOperationImpl{dbo: op}, nil
}

func (c *AdminClientImpl) UpdateDatabaseDdl(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest, opts ...gax.CallOption) (UpdateDatabaseDdlOperation, error) {
	op, err := c.adminClient.UpdateDatabaseDdl(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	return &UpdateDatabaseDdlImpl{dbo: op}, nil
}

type CreateDatabaseOperationImpl struct {
	dbo *database.CreateDatabaseOperation
}

func (c *CreateDatabaseOperationImpl) Wait(ctx context.Context, opts ...gax.CallOption) (*databasepb.Database, error) {
	return c.dbo.Wait(ctx, opts...)
}

type UpdateDatabaseDdlImpl struct {
	dbo *database.UpdateDatabaseDdlOperation
}

func (c *UpdateDatabaseDdlImpl) Wait(ctx context.Context, opts ...gax.CallOption) error {
	return c.dbo.Wait(ctx, opts...)
}
