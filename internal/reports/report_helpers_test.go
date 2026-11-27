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

package reports

import (
	"fmt"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type testCol struct {
	name    string
	srcType schema.Type
	spType  ddl.Type
	issue   internal.SchemaIssue
}

// batchedIssueDescriptions builds a table with the given columns and returns
// the report descriptions for that table.
func batchedIssueDescriptions(cols []testCol) []string {
	tableId := "t1"
	conv := internal.MakeConv()
	srcTable := schema.Table{Name: "events", Id: tableId, ColDefs: map[string]schema.Column{}}
	spTable := ddl.CreateTable{Name: "events", Id: tableId, ColDefs: map[string]ddl.ColumnDef{}}
	issues := map[string][]internal.SchemaIssue{}
	for idx, c := range cols {
		colId := fmt.Sprintf("c%d", idx)
		srcTable.ColIds = append(srcTable.ColIds, colId)
		srcTable.ColDefs[colId] = schema.Column{Name: c.name, Id: colId, Type: c.srcType}
		spTable.ColIds = append(spTable.ColIds, colId)
		spTable.ColDefs[colId] = ddl.ColumnDef{Name: c.name, Id: colId, T: c.spType}
		issues[colId] = []internal.SchemaIssue{c.issue}
	}
	conv.SrcSchema[tableId] = srcTable
	conv.SpSchema[tableId] = spTable
	var descriptions []string
	for _, body := range buildTableReportBody(conv, tableId, issues, spTable, srcTable, nil, nil, nil) {
		for _, issue := range body.IssueBody {
			descriptions = append(descriptions, issue.Description)
		}
	}
	return descriptions
}

func TestBatchedIssuesListAllColumns(t *testing.T) {
	timeType := schema.Type{Name: "time"}
	yearType := schema.Type{Name: "year"}
	intType := schema.Type{Name: "int"}
	smallintType := schema.Type{Name: "smallint"}
	datetime2 := schema.Type{Name: "datetime2"}
	smalldatetime := schema.Type{Name: "smalldatetime"}
	strMax := ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	int64Type := ddl.Type{Name: ddl.Int64}
	tsType := ddl.Type{Name: ddl.Timestamp}
	tests := []struct {
		name     string
		cols     []testCol
		expected []string
	}{
		{
			name:     "Time same types",
			cols:     []testCol{{"t3", timeType, strMax, internal.Time}, {"t7", timeType, strMax, internal.Time}},
			expected: []string{"Table 'events': Columns 't3', 't7' of type time are mapped to string(max). Spanner does not support time/year types"},
		},
		{
			name:     "Time mixed types",
			cols:     []testCol{{"t3", timeType, strMax, internal.Time}, {"y1", yearType, strMax, internal.Time}},
			expected: []string{"Table 'events': Columns 't3' (time mapped to string(max)), 'y1' (year mapped to string(max)). Spanner does not support time/year types"},
		},
		{
			name:     "Time single column",
			cols:     []testCol{{"t3", timeType, strMax, internal.Time}},
			expected: []string{"Table 'events': Column 't3' of type time is mapped to string(max). Spanner does not support time/year types"},
		},
		{
			name:     "Widened same types",
			cols:     []testCol{{"a", intType, int64Type, internal.Widened}, {"b", intType, int64Type, internal.Widened}},
			expected: []string{"Table 'events': Some columns will consume more storage in Spanner: columns 'a', 'b' of type int are mapped to int64"},
		},
		{
			name:     "Widened mixed types",
			cols:     []testCol{{"a", intType, int64Type, internal.Widened}, {"b", smallintType, int64Type, internal.Widened}},
			expected: []string{"Table 'events': Some columns will consume more storage in Spanner: columns 'a' (int mapped to int64), 'b' (smallint mapped to int64)"},
		},
		{
			name:     "Widened source types containing TO (#1562)",
			cols:     []testCol{{"DS", schema.Type{Name: "INTERVAL DAY(3) TO SECOND(2)"}, strMax, internal.Widened}, {"YM", schema.Type{Name: "INTERVAL YEAR(4) TO MONTH"}, strMax, internal.Widened}},
			expected: []string{"Table 'events': Some columns will consume more storage in Spanner: columns 'DS' (INTERVAL DAY(3) TO SECOND(2) mapped to string(max)), 'YM' (INTERVAL YEAR(4) TO MONTH mapped to string(max))"},
		},
		{
			name:     "Datetime same types",
			cols:     []testCol{{"d1", datetime2, tsType, internal.Datetime}, {"d2", datetime2, tsType, internal.Datetime}},
			expected: []string{"Some columns have a source DB type without a time zone in table 'events': columns 'd1', 'd2' of type datetime2 are mapped to timestamp. " + IssueDB[internal.Datetime].Brief},
		},
		{
			name:     "Datetime mixed types",
			cols:     []testCol{{"d1", datetime2, tsType, internal.Datetime}, {"d2", smalldatetime, tsType, internal.Datetime}},
			expected: []string{"Some columns have a source DB type without a time zone in table 'events': columns 'd1' (datetime2 mapped to timestamp), 'd2' (smalldatetime mapped to timestamp). " + IssueDB[internal.Datetime].Brief},
		},
		{
			name:     "Timestamp",
			cols:     []testCol{{"a", schema.Type{Name: "timestamp"}, tsType, internal.Timestamp}, {"b", schema.Type{Name: "timestamp"}, tsType, internal.Timestamp}},
			expected: []string{"Some columns have source DB type 'timestamp without timezone' which is mapped to Spanner type timestamp in table 'events': columns 'a', 'b'. " + IssueDB[internal.Timestamp].Brief},
		},
		{
			name:     "DefaultValue",
			cols:     []testCol{{"a", intType, int64Type, internal.DefaultValue}, {"b", intType, int64Type, internal.DefaultValue}},
			expected: []string{IssueDB[internal.DefaultValue].Brief + " for table 'events': columns 'a', 'b'"},
		},
		{
			name:     "DefaultValueError",
			cols:     []testCol{{"a", intType, int64Type, internal.DefaultValueError}, {"b", intType, int64Type, internal.DefaultValueError}},
			expected: []string{IssueDB[internal.DefaultValueError].Brief + " for table 'events': columns 'a', 'b'"},
		},
		{
			name: "GeneratedColumnValueError stays per column",
			cols: []testCol{{"a", intType, int64Type, internal.GeneratedColumnValueError}, {"b", intType, int64Type, internal.GeneratedColumnValueError}},
			expected: []string{
				IssueDB[internal.GeneratedColumnValueError].Brief + " for table 'events' e.g. column 'a'",
				IssueDB[internal.GeneratedColumnValueError].Brief + " for table 'events' e.g. column 'b'",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, batchedIssueDescriptions(tc.cols))
		})
	}
}

func TestBatchedIssuesCapColumnList(t *testing.T) {
	var cols []testCol
	for idx := 0; idx < 12; idx++ {
		cols = append(cols, testCol{fmt.Sprintf("c%02d", idx), schema.Type{Name: "int"}, ddl.Type{Name: ddl.Int64}, internal.Widened})
	}
	expected := []string{"Table 'events': Some columns will consume more storage in Spanner: columns 'c00', 'c01', 'c02', 'c03', 'c04', 'c05', 'c06', 'c07', 'c08', 'c09' and 2 more of type int are mapped to int64"}
	assert.Equal(t, expected, batchedIssueDescriptions(cols))
}
