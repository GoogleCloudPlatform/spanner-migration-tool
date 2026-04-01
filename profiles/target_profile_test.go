// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package profiles

import (
	"context"
	"fmt"
	"testing"

	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func TestNewTargetProfile(t *testing.T) {
	testCases := []struct {
		targetProfileString          string
		expectedTargetProfileDetails TargetProfileConnectionSpanner
		expectedDefaultIdentityOptions DefaultIdentityOptions
		expectedErr                  bool
	}{
		{
			targetProfileString: "",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{},
			expectedErr: false,
		},
		{
			targetProfileString: "instance=test-instance,defaultTimezone=America/New_York",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{
				Instance: "test-instance",
				DefaultTimezone: "America/New_York",
			},
			expectedErr: false,
		},
		{
			targetProfileString: "instance=test-instance,defaultIdentitySkipRange=10-50",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{
				Instance: "test-instance",
			},
			expectedDefaultIdentityOptions: DefaultIdentityOptions{
				SkipRangeMin: "10",
				SkipRangeMax: "50",
			},
			expectedErr: false,
		},
		{
			targetProfileString: "instance=test-instance,defaultIdentityStartCounterWith=100",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{
				Instance: "test-instance",
			},
			expectedDefaultIdentityOptions: DefaultIdentityOptions{
				StartCounterWith: "100",
			},
			expectedErr: false,
		},
		{
			targetProfileString: "instance=test-instance,defaultIdentitySkipRange=100-500,defaultIdentityStartCounterWith=10",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{
				Instance: "test-instance",
			},
			expectedDefaultIdentityOptions: DefaultIdentityOptions{
				SkipRangeMin: "100",
				SkipRangeMax: "500",
				StartCounterWith: "10",
			},
			expectedErr: false,
		},
		{
			targetProfileString: "project=test-project",
			expectedErr: true,
		},
		{
			targetProfileString: "instance=test-instance,dialect=not_a_real_dialect",
			expectedErr: true,
		},
		{
			targetProfileString: "instance=test-instance,defaultTimezone=not_a_real_timezone",
			expectedErr: true,
		},
		{
			targetProfileString: "instance=test-instance,defaultIdentitySkipRange=",
			expectedErr: true,
		},
		{
			targetProfileString: "instance=test-instance,defaultIdentityStartCounterWith=",
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		actual, err := NewTargetProfile(tc.targetProfileString)
		if tc.expectedErr {
			assert.Equal(t, TargetProfile{}, actual)
			assert.Error(t, err)
		} else {
			expectedTargetProfile := TargetProfile{
				Ty: TargetProfileTypeConnection,
				Conn: TargetProfileConnection{
					Ty: TargetProfileConnectionTypeSpanner,
					Sp: tc.expectedTargetProfileDetails,
				},
				DefaultIdentityOptions: tc.expectedDefaultIdentityOptions,
			}

			assert.Equal(t, expectedTargetProfile, actual)
			assert.NoError(t, err)
		}
	}
}

func TestExtractDefaultIdentityOptions(t *testing.T) {
	testCases := []struct {
		defaultIdentitySkipRangeStr string
		setEmptySkipRangeStr bool
		defaultIdentityStartCounterWithStr string
		setEmptyStartCounterWithStr bool
		expectedDefaultIdentityOptions DefaultIdentityOptions
		expectedErr                  bool
	}{
		{
			defaultIdentitySkipRangeStr: "10-50",
			expectedDefaultIdentityOptions: DefaultIdentityOptions{
				SkipRangeMin: "10",
				SkipRangeMax: "50",
			},
			expectedErr: false,
		},
		{
			defaultIdentityStartCounterWithStr: "100",
			expectedDefaultIdentityOptions: DefaultIdentityOptions{
				StartCounterWith: "100",
			},
			expectedErr: false,
		},
		{
			defaultIdentitySkipRangeStr: "100-500",
			defaultIdentityStartCounterWithStr: "10",
			expectedDefaultIdentityOptions: DefaultIdentityOptions{
				SkipRangeMin: "100",
				SkipRangeMax: "500",
				StartCounterWith: "10",
			},
			expectedErr: false,
		},
		{
			setEmptySkipRangeStr: true,
			expectedErr: true,
		},
		{
			defaultIdentitySkipRangeStr: "10",
			expectedErr: true,
		},
		{
			defaultIdentitySkipRangeStr: "10-100-1000",
			expectedErr: true,
		},
		{
			defaultIdentitySkipRangeStr: "-10-100",
			expectedErr: true,
		},
		{
			defaultIdentitySkipRangeStr: "abc-100",
			expectedErr: true,
		},
		{
			defaultIdentitySkipRangeStr: "10-abc",
			expectedErr: true,
		},
		{
			setEmptyStartCounterWithStr: true,
			expectedErr: true,
		},
		{
			defaultIdentityStartCounterWithStr: "abc",
			expectedErr: true,
		},
		{
			defaultIdentityStartCounterWithStr: "0",
			expectedErr: true,
		},
		{
			defaultIdentityStartCounterWithStr: "-100",
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		params := make(map[string]string)
		if tc.defaultIdentitySkipRangeStr != "" || tc.setEmptySkipRangeStr {
			params["defaultIdentitySkipRange"] = tc.defaultIdentitySkipRangeStr
		}
		if tc.defaultIdentityStartCounterWithStr != "" || tc.setEmptyStartCounterWithStr {
			params["defaultIdentityStartCounterWith"] = tc.defaultIdentityStartCounterWithStr
		}
		actual, err := extractDefaultIdentityOptions(params)
		if tc.expectedErr {
			assert.Error(t, err)
			assert.Equal(t, DefaultIdentityOptions{}, actual)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedDefaultIdentityOptions, actual)
		}
	}
}

type mockDatabaseAdminClient struct {
	GetDatabaseFunc func(ctx context.Context, req *adminpb.GetDatabaseRequest, opts ...gax.CallOption) (*adminpb.Database, error)
}

func (m *mockDatabaseAdminClient) GetDatabase(ctx context.Context, req *adminpb.GetDatabaseRequest, opts ...gax.CallOption) (*adminpb.Database, error) {
	if m.GetDatabaseFunc != nil {
		return m.GetDatabaseFunc(ctx, req, opts...)
	}
	return nil, fmt.Errorf("unimplemented")
}

func (m *mockDatabaseAdminClient) Close() error {
	return nil
}

func TestFetchTargetDialect_Validation(t *testing.T) {
	origNewDatabaseAdminClient := NewDatabaseAdminClient
	defer func() {
		NewDatabaseAdminClient = origNewDatabaseAdminClient
	}()

	trg := TargetProfile{
		Conn: TargetProfileConnection{
			Ty: TargetProfileConnectionTypeSpanner,
			Sp: TargetProfileConnectionSpanner{
				Project:  "test-project",
				Instance: "test-instance",
				Dbname:   "test-db",
			},
		},
	}

	// Test Case 1: Valid GoogleSQL
	NewDatabaseAdminClient = func(ctx context.Context) (DatabaseAdminClient, error) {
		return &mockDatabaseAdminClient{
			GetDatabaseFunc: func(ctx context.Context, req *adminpb.GetDatabaseRequest, opts ...gax.CallOption) (*adminpb.Database, error) {
				return &adminpb.Database{DatabaseDialect: adminpb.DatabaseDialect_GOOGLE_STANDARD_SQL}, nil
			},
		}, nil
	}
	dialect, err := trg.FetchTargetDialect(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "google_standard_sql", dialect)

	// Test Case 2: Valid PostgreSQL
	NewDatabaseAdminClient = func(ctx context.Context) (DatabaseAdminClient, error) {
		return &mockDatabaseAdminClient{
			GetDatabaseFunc: func(ctx context.Context, req *adminpb.GetDatabaseRequest, opts ...gax.CallOption) (*adminpb.Database, error) {
				return &adminpb.Database{DatabaseDialect: adminpb.DatabaseDialect_POSTGRESQL}, nil
			},
		}, nil
	}
	dialect, err = trg.FetchTargetDialect(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "postgresql", dialect)

	// Test Case 3: Invalid Unspecified Dialect
	NewDatabaseAdminClient = func(ctx context.Context) (DatabaseAdminClient, error) {
		return &mockDatabaseAdminClient{
			GetDatabaseFunc: func(ctx context.Context, req *adminpb.GetDatabaseRequest, opts ...gax.CallOption) (*adminpb.Database, error) {
				return &adminpb.Database{DatabaseDialect: adminpb.DatabaseDialect_DATABASE_DIALECT_UNSPECIFIED}, nil
			},
		}, nil
	}
	_, err = trg.FetchTargetDialect(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported database dialect")
}
