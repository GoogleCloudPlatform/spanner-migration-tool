// Copyright 2023 Google LLC
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

package summary

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestGetSummary(t *testing.T) {
	tc := []struct {
		conv            *internal.Conv
		expectedSummary map[string]ConversionSummary
	}{
		{
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "tn1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "cn1", T: ddl.Type{Name: "STRING", IsArray: false}, NotNull: true, Comment: "", Id: "c1"},
							"c2": {Name: "cn2", T: ddl.Type{Name: "STRING", IsArray: false}, NotNull: true, Comment: "", Id: "c2"},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
						Id:          "t1",
					}},
				SrcSchema: map[string]schema.Table{

					"t1": {
						Name:   "tn1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "cn1", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
							"c2": {Name: "cn2", Type: schema.Type{Name: "char"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
						Id:          "t1",
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Time},
						},
					},
				},
			},
			expectedSummary: map[string]ConversionSummary{},
		},
	}
	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		actualSummary := getSummary()

		assert.Equal(t, []reports.Issue([]reports.Issue{reports.Issue{Category: "TIME_YEAR_TYPE_USES", Description: "Table 'tn1': Column 'cn1', type varchar is mapped to string(0). Spanner does not support time/year types"}}), actualSummary["t1"].Warnings)
		assert.Equal(t, int(1), actualSummary["t1"].WarningsCount)

	}
}

func TestGetSummary_PartitionedTables(t *testing.T) {
	srcTable := func(id, name, parent string) schema.Table {
		return schema.Table{
			Name:            name,
			ColIds:          []string{"c1"},
			ColDefs:         map[string]schema.Column{"c1": {Name: "id", Type: schema.Type{Name: "bigint"}, NotNull: true, Id: "c1"}},
			PrimaryKeys:     []schema.Key{{ColId: "c1", Order: 1}},
			Id:              id,
			PartitionParent: parent,
		}
	}

	// t1 is the parent and converts; t2 and t3 are its partitions and are skipped.
	conv := &internal.Conv{
		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:        "orders",
				ColIds:      []string{"c1"},
				ColDefs:     map[string]ddl.ColumnDef{"c1": {Name: "id", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c1"}},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1}},
				Id:          "t1",
			},
		},
		SrcSchema: map[string]schema.Table{
			"t1": srcTable("t1", "orders", ""),
			"t2": srcTable("t2", "orders_2024", "orders"),
			"t3": srcTable("t3", "orders_2023", "orders"),
		},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
		SchemaIssues: map[string]internal.TableIssues{
			"t2": {
				TableLevelIssues:  []internal.SchemaIssue{internal.PartitionedTable},
				ColumnLevelIssues: map[string][]internal.SchemaIssue{},
			},
			"t3": {
				TableLevelIssues:  []internal.SchemaIssue{internal.PartitionedTable},
				ColumnLevelIssues: map[string][]internal.SchemaIssue{},
			},
		},
	}

	sessionState := session.GetSessionState()
	sessionState.Driver = constants.POSTGRES
	sessionState.Conv = conv

	actualSummary := getSummary()

	partitionWarnings := []reports.Issue{}
	for _, w := range actualSummary["t1"].Warnings {
		if w.Category == "PARTITIONED_TABLE_IGNORED" {
			partitionWarnings = append(partitionWarnings, w)
		}
	}
	assert.Equal(t, []reports.Issue{{
		Category:    "PARTITIONED_TABLE_IGNORED",
		Description: "Table 'orders': Partitioned tables are ignored",
	}}, partitionWarnings)

	assert.Equal(t, len(actualSummary["t1"].Warnings), actualSummary["t1"].WarningsCount)

	assert.Empty(t, actualSummary["t2"].Warnings)
	assert.Empty(t, actualSummary["t3"].Warnings)
}
