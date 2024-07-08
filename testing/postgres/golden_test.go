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

package postgres_test

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	commonTesting "github.com/GoogleCloudPlatform/spanner-migration-tool/testing/common"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

const GoldenTestsDir = "../../test_data/goldens/postgres"

func formatDdl(ddl []string) string {
	return strings.ReplaceAll(strings.Join(ddl, "\n"), "\t", strings.Repeat(" ", 4))
}

func TestGoldens(t *testing.T) {
	logger.Log = zap.NewNop()
	entries, err := os.ReadDir(GoldenTestsDir)
	if err != nil {
		t.Fatalf("error when reading entries of golden tests dir %s: %s", GoldenTestsDir, err)
	}

	schemaToSpanner := common.SchemaToSpannerImpl{}

	for _, entry := range entries {
		if !entry.IsDir() {
			path := filepath.Join(GoldenTestsDir, entry.Name())
			testCases, err := commonTesting.GoldenTestCasesFrom(path)
			if err != nil {
				t.Fatalf("error when reading golden tests from path %s: %s", path, err)
			}

			t.Logf("executing %d test cases from %s", len(testCases), path)
			for _, testCase := range testCases {
				conv := internal.MakeConv()
				conv.SetLocation(time.UTC)
				conv.SetSchemaMode()

				err := common.ProcessDbDump(
					conv,
					internal.NewReader(bufio.NewReader(strings.NewReader(testCase.InputSchema)), nil),
					postgres.DbDumpImpl{})
				if err != nil {
					t.Fatalf("error when processing dump %s: %s", testCase.InputSchema, err)
				}

				err = schemaToSpanner.SchemaToSpannerDDL(conv, postgres.ToDdlImpl{})
				if err != nil {
					t.Fatalf("error when converting schema to spanner ddl %s: %s", testCase.InputSchema, err)
				}
				config := ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: true}

				config.SpDialect = constants.DIALECT_GOOGLESQL
				actual := ddl.GetDDL(config, conv.SpSchema, conv.SpSequences)
				assert.Equal(t, testCase.ExpectedGSQLSchema, formatDdl(actual))

				config.SpDialect = constants.DIALECT_POSTGRESQL
				actual = ddl.GetDDL(config, conv.SpSchema, conv.SpSequences)
				assert.Equal(t, testCase.ExpectedPSQLSchema, formatDdl(actual))
			}
		}
	}
}
