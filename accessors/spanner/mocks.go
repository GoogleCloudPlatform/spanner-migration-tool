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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

// Mock that implements the SpannerAccessor interface.
// Pass in unit tests where SpannerAccessor is an input parameter.
type SpannerAccessorMock struct {
	GetDatabaseDialectMock          	func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (string, error)
	CheckExistingDbMock             	func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error)
	CreateEmptyDatabaseMock         	func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error
	GetSpannerLeaderLocationMock    	func(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error)
	CheckIfChangeStreamExistsMock   	func(ctx context.Context, changeStreamName, dbURI string) (bool, error)
	ValidateChangeStreamOptionsMock 	func(ctx context.Context, changeStreamName, dbURI string) error
	CreateChangeStreamMock          	func(ctx context.Context, adminClient spanneradmin.AdminClient, changeStreamName, dbURI string) error
	CreateDatabaseMock					func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, driver string, migrationType string) error 
	UpdateDatabaseMock					func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, driver string) error
	CreateOrUpdateDatabaseMock			func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI, driver string, conv *internal.Conv, migrationType string) error 
	VerifyDbMock						func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (dbExists bool, err error)
	ValidateDDLMock						func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error
	UpdateDDLForeignKeysMock			func(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, driver string, migrationType string)
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

func (sam *SpannerAccessorMock) CreateDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, driver string, migrationType string) error {
	return sam.CreateDatabaseMock(ctx,adminClient, dbURI, conv, driver, migrationType)
}
func (sam *SpannerAccessorMock) UpdateDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, driver string) error{
	return sam.UpdateDatabaseMock(ctx, adminClient, dbURI, conv, driver)
}
func (sam *SpannerAccessorMock) CreateOrUpdateDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI, driver string, conv *internal.Conv, migrationType string) error {
	return sam.CreateOrUpdateDatabaseMock(ctx,adminClient, dbURI, driver, conv, migrationType)
}
func (sam *SpannerAccessorMock) VerifyDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (dbExists bool, err error){
	return sam.VerifyDbMock(ctx, adminClient, dbURI)
}
func (sam *SpannerAccessorMock) ValidateDDL(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error{
	return sam.ValidateDDLMock(ctx, adminClient, dbURI)
}
func (sam *SpannerAccessorMock) UpdateDDLForeignKeys(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, driver string, migrationType string) {}