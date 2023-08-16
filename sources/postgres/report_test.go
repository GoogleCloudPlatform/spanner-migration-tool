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

package postgres

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/stretchr/testify/assert"
)

func TestReport(t *testing.T) {
	s := `
        CREATE TABLE bad_schema (
            a numeric,
            b integer NOT NULL,
            c integer[4][2],
            d circle);
        CREATE TABLE default_value (
            a text primary key,
            b bigint DEFAULT 42);
        CREATE TABLE excellent_schema (
            a text primary key,
            b bigint);
        CREATE TABLE foreign_key (
            a text primary key references excellent_schema(a),
            b bigint);
        CREATE TABLE no_pk (
            a bigint[],
            b integer NOT NULL,
            c text);`
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	common.ProcessDbDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil), DbDumpImpl{})
	conv.SetDataMode()

	badSchemaTableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "bad_schema")
	assert.Equal(t, nil, err)
	noPkTableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "no_pk")
	assert.Equal(t, nil, err)

	conv.Stats.Rows = map[string]int64{badSchemaTableId: 1000, noPkTableId: 5000}
	conv.Stats.GoodRows = map[string]int64{badSchemaTableId: 990, noPkTableId: 3000}
	conv.Stats.BadRows = map[string]int64{badSchemaTableId: 10, noPkTableId: 2000}
	badWrites := map[string]int64{badSchemaTableId: 50, noPkTableId: 0}
	conv.Stats.Unexpected["Testing unexpected messages"] = 5
	conv.Audit = internal.Audit{
		MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum(),
	}
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)
	actualStructuredReport := reports.GenerateStructuredReport(constants.PGDUMP, "sampleDB", conv, badWrites, true, true)
	var expectedStructuredReport reports.StructuredReport
	expectedBytes, _ := ioutil.ReadFile(filepath.Join("..", "..", "test_data", "postgres_structured_report.json"))
	_ = json.Unmarshal(expectedBytes, &expectedStructuredReport)
	assert.Equal(t, expectedStructuredReport, actualStructuredReport)
	reports.GenerateTextReport(actualStructuredReport, w)
	w.Flush()
	expectedBytes, _ = ioutil.ReadFile(filepath.Join("..", "..", "test_data", "postgres_text_report.txt"))
	expected := string(expectedBytes)
	actual := buf.String()
	assert.Equal(t, expected, actual)
}
