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
package spanneraccessor

import (
	"context"

	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
)

type SpannerAccessorMock struct {
	GetDatabaseDialectMock          func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (string, error)
	CheckExistingDbMock             func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error)
	CreateEmptyDatabaseMock         func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error
	GetSpannerLeaderLocationMock    func(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error)
	CheckIfChangeStreamExistsMock   func(ctx context.Context, changeStreamName, dbURI string) (bool, error)
	ValidateChangeStreamOptionsMock func(ctx context.Context, changeStreamName, dbURI string) error
	CreateChangeStreamMock          func(ctx context.Context, adminClient spanneradmin.AdminClient, changeStreamName, dbURI string) error
}

func (sam *SpannerAccessorMock) GetDatabaseDialect(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (string, error) {
	return sam.GetDatabaseDialectMock(ctx, adminClient, dbURI)
}

func (sam *SpannerAccessorMock) CheckExistingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error) {
	return sam.CheckExistingDbMock(ctx, adminClient, dbURI)
}

func (sam *SpannerAccessorMock) CreateEmptyDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error {
	return sam.CreateEmptyDatabaseMock(ctx, adminClient, dbURI)
}

func (sam *SpannerAccessorMock) GetSpannerLeaderLocation(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error) {
	return sam.GetSpannerLeaderLocationMock(ctx, instanceClient, instanceURI)
}

func (sam *SpannerAccessorMock) CheckIfChangeStreamExists(ctx context.Context, changeStreamName, dbURI string) (bool, error) {
	return sam.CheckIfChangeStreamExistsMock(ctx, changeStreamName, dbURI)
}

func (sam *SpannerAccessorMock) ValidateChangeStreamOptions(ctx context.Context, changeStreamName, dbURI string) error {
	return sam.ValidateChangeStreamOptionsMock(ctx, changeStreamName, dbURI)
}

func (sam *SpannerAccessorMock) CreateChangeStream(ctx context.Context, adminClient spanneradmin.AdminClient, changeStreamName, dbURI string) error {
	return sam.CreateChangeStreamMock(ctx, adminClient, changeStreamName, dbURI)
}
