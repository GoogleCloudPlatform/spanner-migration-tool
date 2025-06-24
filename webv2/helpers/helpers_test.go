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

package helpers

import (
	"fmt"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/stretchr/testify/assert"
)

func TestGetSourceDatabaseFromDriver(t *testing.T) {
	testCases := []struct {
		name    string
		driver  string
		wantDb  string
		wantErr bool
		err     error
	}{
		{
			name:    "MySQL dump driver",
			driver:  constants.MYSQLDUMP,
			wantDb:  constants.MYSQL,
			wantErr: false,
		},
		{
			name:    "MySQL direct connection driver",
			driver:  constants.MYSQL,
			wantDb:  constants.MYSQL,
			wantErr: false,
		},
		{
			name:    "PostgreSQL dump driver",
			driver:  constants.PGDUMP,
			wantDb:  constants.POSTGRES,
			wantErr: false,
		},
		{
			name:    "PostgreSQL direct connection driver",
			driver:  constants.POSTGRES,
			wantDb:  constants.POSTGRES,
			wantErr: false,
		},
		{
			name:    "Oracle driver",
			driver:  constants.ORACLE,
			wantDb:  constants.ORACLE,
			wantErr: false,
		},
		{
			name:    "SQL Server driver",
			driver:  constants.SQLSERVER,
			wantDb:  constants.SQLSERVER,
			wantErr: false,
		},
		{
			name:    "Cassandra driver",
			driver:  constants.CASSANDRA,
			wantDb:  constants.CASSANDRA,
			wantErr: false,
		},
		{
			name:    "Unsupported driver",
			driver:  "unsupported",
			wantDb:  "",
			wantErr: true,
			err:     fmt.Errorf("unsupported driver type: unsupported"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotDb, gotErr := GetSourceDatabaseFromDriver(tc.driver)
			assert.Equal(t, tc.wantDb, gotDb)
			if tc.wantErr {
				assert.EqualError(t, gotErr, tc.err.Error())
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}