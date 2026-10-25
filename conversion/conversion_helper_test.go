// Copyright 2026 Google LLC
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
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
)

func TestGetDbNameFromSQLConnectionStrOracle(t *testing.T) {
	tests := []struct {
		name string
		user string
	}{
		{"plain user", "SMT_USER"},
		{"common user with #", "C##SMT_TEST_USER"},
		{"user with #", "APP#1"},
		{"user with $", "HR$OWNER"},
		{"lower case user", "smt_user"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Same connection string format as profiles.GetSQLConnectionStr for Oracle.
			connStr := go_ora.BuildUrl("127.0.0.1", 1521, "XEPDB1", tc.user, "p@ss:w#rd", nil)
			assert.Equal(t, tc.user, getDbNameFromSQLConnectionStr(constants.ORACLE, connStr))
		})
	}
}
