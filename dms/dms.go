// Copyright 2022 Google LLC
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
package dms

import (
	"context"
)

// Test MySQL Connectivity
func TestMySQLConnectionProfile(ctx context.Context, srcConn SrcConnCfg) error {
	return createMySQLConnectionProfile(ctx, srcConn, true)
}

// Create MySQL Connection Profile
func CreateMySQLConnectionProfile(ctx context.Context, srcConn SrcConnCfg) error {
	return createMySQLConnectionProfile(ctx, srcConn, false)
}

// Test Spanner Connectivity
func TestSpannerConnectionProfile(ctx context.Context, destConn DstConnCfg) error {
	return createSpannerConnectionProfile(ctx, destConn, true)
}

// Create Spanner Connection Profile
func CreateSpannerConnectionProfile(ctx context.Context, destConn DstConnCfg) error {
	return createSpannerConnectionProfile(ctx, destConn, false)
}

// Migrate
func Migrate(ctx context.Context, workspaceCfg ConversionWorkspaceCfg, jobCfg DMSJobCfg) error {
	commitID, err := createConversionWorkspace(ctx, workspaceCfg)
	if err != nil {
		return err
	}

	jobCfg.ConversionWorkspaceCommitID = commitID
	err = createDMSJob(ctx, jobCfg)
	if err != nil {
		return err
	}
	return launchDMSJob(ctx, jobCfg)
}

// Test MySQL Connection Profiles Bulk
// Create MySQL Connection Profiles Bulk
// Migrate Bulk
