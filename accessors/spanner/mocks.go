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
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

// Mock that implements the SpannerAccessor interface.
// Pass in unit tests where SpannerAccessor is an input parameter.
type SpannerAccessorMock struct {
	GetDatabaseDialectMock          func(ctx context.Context, dbURI string) (string, error)
	CheckExistingDbMock             func(ctx context.Context, dbURI string) (bool, error)
	CreateEmptyDatabaseMock         func(ctx context.Context, dbURI string) error
	GetSpannerLeaderLocationMock    func(ctx context.Context, instanceURI string) (string, error)
	CheckIfChangeStreamExistsMock   func(ctx context.Context, changeStreamName, dbURI string) (bool, error)
	ValidateChangeStreamOptionsMock func(ctx context.Context, changeStreamName, dbURI string) error
	CreateChangeStreamMock          func(ctx context.Context, changeStreamName, dbURI string) error
	CreateDatabaseMock              func(ctx context.Context, dbURI string, conv *internal.Conv, driver string, migrationType string) error
	UpdateDatabaseMock              func(ctx context.Context, dbURI string, conv *internal.Conv, driver string) error
	CreateOrUpdateDatabaseMock      func(ctx context.Context, dbURI, driver string, conv *internal.Conv, migrationType string) error
	VerifyDbMock                    func(ctx context.Context, dbURI string) (dbExists bool, err error)
	ValidateDDLMock                 func(ctx context.Context, dbURI string) error
	UpdateDDLForeignKeysMock        func(ctx context.Context, dbURI string, conv *internal.Conv, driver string, migrationType string)
	DropDatabaseMock                func(ctx context.Context, dbURI string) error
	ValidateDMLMock                 func(ctx context.Context, query string) (bool, error)
	TableExistsMock                 func(ctx context.Context, tableName string) (bool, error)
	GetDatabaseNameMock             func() string
	RefreshMock                     func(ctx context.Context, dbURI string)
	SetSpannerClientMock            func(spannerClient spannerclient.SpannerClient)
	GetSpannerClientMock            func() spannerclient.SpannerClient
}

func (sam *SpannerAccessorMock) GetDatabaseDialect(ctx context.Context, dbURI string) (string, error) {
	return sam.GetDatabaseDialectMock(ctx, dbURI)
}

func (sam *SpannerAccessorMock) CheckExistingDb(ctx context.Context, dbURI string) (bool, error) {
	return sam.CheckExistingDbMock(ctx, dbURI)
}

func (sam *SpannerAccessorMock) CreateEmptyDatabase(ctx context.Context, dbURI string) error {
	return sam.CreateEmptyDatabaseMock(ctx, dbURI)
}

func (sam *SpannerAccessorMock) GetSpannerLeaderLocation(ctx context.Context, instanceURI string) (string, error) {
	return sam.GetSpannerLeaderLocationMock(ctx, instanceURI)
}

func (sam *SpannerAccessorMock) CheckIfChangeStreamExists(ctx context.Context, changeStreamName, dbURI string) (bool, error) {
	return sam.CheckIfChangeStreamExistsMock(ctx, changeStreamName, dbURI)
}

func (sam *SpannerAccessorMock) ValidateChangeStreamOptions(ctx context.Context, changeStreamName, dbURI string) error {
	return sam.ValidateChangeStreamOptionsMock(ctx, changeStreamName, dbURI)
}

func (sam *SpannerAccessorMock) CreateChangeStream(ctx context.Context, changeStreamName, dbURI string) error {
	return sam.CreateChangeStreamMock(ctx, changeStreamName, dbURI)
}

func (sam *SpannerAccessorMock) CreateDatabase(ctx context.Context, dbURI string, conv *internal.Conv, driver string, migrationType string) error {
	return sam.CreateDatabaseMock(ctx, dbURI, conv, driver, migrationType)
}
func (sam *SpannerAccessorMock) UpdateDatabase(ctx context.Context, dbURI string, conv *internal.Conv, driver string) error {
	return sam.UpdateDatabaseMock(ctx, dbURI, conv, driver)
}
func (sam *SpannerAccessorMock) CreateOrUpdateDatabase(ctx context.Context, dbURI, driver string, conv *internal.Conv, migrationType string) error {
	return sam.CreateOrUpdateDatabaseMock(ctx, dbURI, driver, conv, migrationType)
}
func (sam *SpannerAccessorMock) VerifyDb(ctx context.Context, dbURI string) (dbExists bool, err error) {
	return sam.VerifyDbMock(ctx, dbURI)
}
func (sam *SpannerAccessorMock) ValidateDDL(ctx context.Context, dbURI string) error {
	return sam.ValidateDDLMock(ctx, dbURI)
}
func (sam *SpannerAccessorMock) UpdateDDLForeignKeys(ctx context.Context, dbURI string, conv *internal.Conv, driver string, migrationType string) {
}

// DropDatabase implements SpannerAccessor.
func (sam *SpannerAccessorMock) DropDatabase(ctx context.Context, dbURI string) error {
	return sam.DropDatabaseMock(ctx, dbURI)
}

// ValidateDML implements SpannerAccessor.
func (sam *SpannerAccessorMock) ValidateDML(ctx context.Context, query string) (bool, error) {
	return sam.ValidateDMLMock(ctx, query)
}

func (sam *SpannerAccessorMock) TableExists(ctx context.Context, tableName string) (bool, error) {
	return sam.TableExistsMock(ctx, tableName)
}
func (sam *SpannerAccessorMock) GetDatabaseName() string {
	return sam.GetDatabaseNameMock()
}

func (sam *SpannerAccessorMock) Refresh(ctx context.Context, dbURI string) {
	sam.RefreshMock(ctx, dbURI)
}

func (sam *SpannerAccessorMock) SetSpannerClient(spannerClient spannerclient.SpannerClient) {
	sam.SetSpannerClientMock(spannerClient)
}

func (sam *SpannerAccessorMock) GetSpannerClient() spannerclient.SpannerClient {
	return sam.GetSpannerClientMock()
}
