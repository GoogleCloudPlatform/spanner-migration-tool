// Copyright 2020 Google LLC
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

//go:build !linux
// +build !linux

package postgres

import (
	"fmt"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
)

// DbDumpImpl Postgres specific implementation for DdlDumpImpl.
type DbDumpImpl struct{}

// GetToDdl functions below implement the common.DbDump interface
func (ddi DbDumpImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// ProcessDump calls processPgDump to read a Postgres dump file
func (ddi DbDumpImpl) ProcessDump(conv *internal.Conv, r *internal.Reader) error {
	conv.Unexpected("Import from PG dump is not supported in MAC and WINDOWS systems. Please use a LINUX system for this feature. You can easily do so from the GCP Cloud Shell!")
	return fmt.Errorf("Import from PG dump is not supported in MAC and WINDOWS systems. Please use a LINUX system for this feature. You can easily do so from the GCP Cloud Shell!")
}
