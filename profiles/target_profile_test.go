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
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/stretchr/testify/assert"
)

func TestNewTargetProfile(t *testing.T) {
	testCases := []struct {
		targetProfileString          string
		expectedTargetProfileDetails TargetProfileConnectionSpanner
		expectedErr                  bool
	}{
		{
			targetProfileString: "",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{
				Dialect: constants.DIALECT_GOOGLESQL,
			},
			expectedErr: false,
		},
		{
			targetProfileString: "instance=test-instance,defaultTimezone=America/New_York",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{
				Instance: "test-instance",
				Dialect: constants.DIALECT_GOOGLESQL,
				DefaultTimezone: "America/New_York",
			},
			expectedErr: false,
		},
		{
			targetProfileString: "project=test-project",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{},
			expectedErr: true,
		},
		{
			targetProfileString: "instance=test-instance,dialect=not_a_real_dialect",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{},
			expectedErr: true,
		},
		{
			targetProfileString: "instance=test-instance,defaultTimezone=not_a_real_timezone",
			expectedTargetProfileDetails: TargetProfileConnectionSpanner{},
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
			}

			assert.Equal(t, expectedTargetProfile, actual)
			assert.NoError(t, err)
		}
	}
}
