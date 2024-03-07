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

package conversion

import (
	"fmt"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)


func TestSchemaFromDatabase(t *testing.T) {
	targetProfile := profiles.TargetProfile{
		Conn: profiles.TargetProfileConnection{
			Sp: profiles.TargetProfileConnectionSpanner{
				Dialect: "google_standard_sql",
			},
		},
	}

	sourceProfileConfigBulk := profiles.SourceProfile{
		Ty: profiles.SourceProfileType(3),
		Config: profiles.SourceProfileConfig{
			ConfigType: "bulk",
		},
	}
	sourceProfileConfigDataflow := profiles.SourceProfile{
		Ty: profiles.SourceProfileType(3),
		Config: profiles.SourceProfileConfig{
			ConfigType: "dataflow",
		},
	}
	sourceProfileConfigDms := profiles.SourceProfile{
		Ty: profiles.SourceProfileType(3),
		Config: profiles.SourceProfileConfig{
			ConfigType: "dms",
		},
	}
	sourceProfileConfigInvalid := profiles.SourceProfile{
		Ty: profiles.SourceProfileType(3),
		Config: profiles.SourceProfileConfig{
			ConfigType: "invalid",
		},
	}
	sourceProfileCloudSql := profiles.SourceProfile{
		Ty: profiles.SourceProfileType(5),
	}
	sourceProfileCloudDefault := profiles.SourceProfile{}
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          				string
		sourceProfile 				profiles.SourceProfile
		getInfoError				error
		processSchemaError			error
		errorExpected 				bool
	}{
		{
			name: "successful source profile config for bulk migration",
			sourceProfile: sourceProfileConfigBulk,
			getInfoError: nil,
			processSchemaError: nil,
			errorExpected: false,
		},
		{
			name: "source profile config for bulk migration: get info error",
			sourceProfile: sourceProfileConfigBulk,
			getInfoError: fmt.Errorf("error"),
			processSchemaError: nil,
			errorExpected: true,
		},
		{
			name: "source profile config for bulk migration: process schema error",
			sourceProfile: sourceProfileConfigBulk,
			getInfoError: nil,
			processSchemaError: fmt.Errorf("error"),
			errorExpected: true,
		},
		{
			name: "successful source profile config for dataflow migration",
			sourceProfile: sourceProfileConfigDataflow,
			getInfoError: nil,
			processSchemaError: nil,
			errorExpected: false,
		},
		{
			name: "source profile config for dataflow migration: get info error",
			sourceProfile: sourceProfileConfigDataflow,
			getInfoError: fmt.Errorf("error"),
			processSchemaError: nil,
			errorExpected: true,
		},
		{
			name: "source profile config for dms migration",
			sourceProfile: sourceProfileConfigDms,
			getInfoError: nil,
			processSchemaError: nil,
			errorExpected: true,
		},
		{
			name: "invalid source profile config",
			sourceProfile: sourceProfileConfigInvalid,
			getInfoError: nil,
			processSchemaError: nil,
			errorExpected: true,
		},
		{
			name: "successful source profile cloud sql",
			sourceProfile: sourceProfileCloudSql,
			getInfoError: nil,
			processSchemaError: nil,
			errorExpected: false,
		},
		{
			name: "source profile cloud sql: get info error",
			sourceProfile: sourceProfileCloudSql,
			getInfoError: fmt.Errorf("error"),
			processSchemaError: nil,
			errorExpected: true,
		},
		{
			name: "successful source profile default",
			sourceProfile: sourceProfileCloudDefault,
			getInfoError: nil,
			processSchemaError: nil,
			errorExpected: false,
		},
		{
			name: "source profile default: get info error",
			sourceProfile: sourceProfileCloudDefault,
			getInfoError: fmt.Errorf("error"),
			processSchemaError: nil,
			errorExpected: true,
		},
	}

	for _, tc := range testCases {
		gim := MockGetInfo{}
		ps := common.MockProcessSchema{}

		gim.On("getInfoSchemaForShard", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mysql.InfoSchemaImpl{}, tc.getInfoError)
		gim.On("GetInfoSchemaFromCloudSQL", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mysql.InfoSchemaImpl{}, tc.getInfoError)
		gim.On("GetInfoSchema", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mysql.InfoSchemaImpl{}, tc.getInfoError)
		ps.On("ProcessSchema", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.processSchemaError)

		s := SchemaFromSourceImpl{}
		_, err := s.schemaFromDatabase(tc.sourceProfile, targetProfile, &gim, &ps)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
	}
}