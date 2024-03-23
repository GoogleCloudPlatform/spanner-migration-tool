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
package activity

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/dao"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestMain(m *testing.M) {
	res := m.Run()
	os.Exit(res)
}

type SpannerAccessorMock struct {
	spanneraccessor.SpannerAccessor
}

var getDatabaseDialectMock func(ctx context.Context, dbURI string) (string, error)

func (sam *SpannerAccessorMock) GetDatabaseDialect(ctx context.Context, dbURI string) (string, error) {
	return getDatabaseDialectMock(ctx, dbURI)
}

type DAOMock struct {
	dao.DAOImpl
}

var insertSMTJobEntryMock func(ctx context.Context, jobId string, jobName string, jobType string, dialect string, dbName string, jobData spanner.NullJSON) error

func (dao *DAOMock) InsertSMTJobEntry(ctx context.Context, jobId string, jobName string, jobType string, dialect string, dbName string, jobData spanner.NullJSON) error {
	return insertSMTJobEntryMock(ctx, jobId, jobName, jobType, dialect, dbName, jobData)
}

func TestCreateSmtJobEntryTransaction(t *testing.T) {
	testCases := []struct {
		name                   string
		getDatabaseDialectMock func(ctx context.Context, dbURI string) (string, error)
		insertSMTJobEntryMock  func(ctx context.Context, jobId string, jobName string, jobType string, dialect string, dbName string, jobData spanner.NullJSON) error
		expectError            bool
	}{
		{
			name: "No errors",
			getDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
				return "", nil
			},
			insertSMTJobEntryMock: func(ctx context.Context, jobId string, jobName string, jobType string, dialect string, dbName string, jobData spanner.NullJSON) error {
				return nil
			},
			expectError: false,
		},
		{
			name: "Fetch Dialect error",
			getDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
				return "", fmt.Errorf("test error")
			},
			insertSMTJobEntryMock: func(ctx context.Context, jobId string, jobName string, jobType string, dialect string, dbName string, jobData spanner.NullJSON) error {
				return nil
			},
			expectError: true,
		},
		{
			name: "Dao error",
			getDatabaseDialectMock: func(ctx context.Context, dbURI string) (string, error) {
				return "", nil
			},
			insertSMTJobEntryMock: func(ctx context.Context, jobId string, jobName string, jobType string, dialect string, dbName string, jobData spanner.NullJSON) error {
				return fmt.Errorf("test error")
			},
			expectError: true,
		},
	}
	ctx := context.Background()
	createSmtJobEntry := CreateSmtJobEntry{
		Input: &CreateSmtJobEntryInput{},
		DAO:   &DAOMock{},
		SpA:   &SpannerAccessorMock{},
	}
	for _, tc := range testCases {
		getDatabaseDialectMock = tc.getDatabaseDialectMock
		insertSMTJobEntryMock = tc.insertSMTJobEntryMock
		err := createSmtJobEntry.Transaction(ctx)
		assert.Equal(t, tc.expectError, err != nil)
	}
}
