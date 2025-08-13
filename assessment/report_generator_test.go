// Copyright 2025 Google LLC
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

package assessment

import (
	// "os"
	// "path/filepath"

	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestGetElementTypeForColumn(t *testing.T) {
	testCases := []struct {
		name   string
		column utils.SrcColumnDetails
		want   string
	}{
		{
			name:   "Standard column",
			column: utils.SrcColumnDetails{GeneratedColumn: utils.GeneratedColumnInfo{IsPresent: false}},
			want:   "Column",
		},
		{
			name:   "Generated column",
			column: utils.SrcColumnDetails{GeneratedColumn: utils.GeneratedColumnInfo{IsPresent: true}},
			want:   "Generated Column",
		},
		{
			name:   "Zero-value column (edge case)",
			column: utils.SrcColumnDetails{},
			want:   "Column",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getElementTypeForColumn(tc.column)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTableDefinitionToString(t *testing.T) {
	testCases := []struct {
		name               string
		input              utils.SrcTableDetails
		expectedSubstrings []string
		expectedNumParts   int
	}{
		{
			name: "UTF charset and multiple properties",
			input: utils.SrcTableDetails{
				Charset:    "utf8mb4",
				Properties: map[string]string{"ENGINE": "InnoDB", "COMPRESSION": "ZLIB"},
			},
			expectedSubstrings: []string{"CHARSET=utf8mb4", "ENGINE=InnoDB", "COMPRESSION=ZLIB"},
			expectedNumParts:   3,
		},
		{
			name:               "Non-UTF charset with no properties",
			input:              utils.SrcTableDetails{Charset: "latin1"},
			expectedSubstrings: []string{},
			expectedNumParts:   0,
		},
		{
			name:               "Empty struct",
			input:              utils.SrcTableDetails{},
			expectedSubstrings: []string{},
			expectedNumParts:   0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tableDefinitionToString(tc.input)
			parts := strings.Fields(got)
			assert.Len(t, parts, tc.expectedNumParts, "The number of generated parts should match")

			for _, sub := range tc.expectedSubstrings {
				assert.Contains(t, got, sub)
			}
		})
	}
}

func TestSourceColumnDefinitionToString(t *testing.T) {
	testCases := []struct {
		name  string
		input utils.SrcColumnDetails
		want  string
	}{
		{
			name: "Everything is set",
			input: utils.SrcColumnDetails{
				Datatype:   "INT64",
				Mods:       []int64{10, 2},
				IsUnsigned: true,
				GeneratedColumn: utils.GeneratedColumnInfo{
					IsPresent: true,
					Statement: "(a + b)",
					IsVirtual: true,
				},
				DefaultValue: ddl.DefaultValue{
					IsPresent: true,
					Value:     ddl.Expression{Statement: "0"},
				},
				NotNull:                true,
				IsOnUpdateTimestampSet: true,
				AutoGen: ddl.AutoGenCol{
					Name:           "my_sequence",
					GenerationType: constants.AUTO_INCREMENT,
				},
			},
			want: "INT64(10,2) UNSIGNED GENERATED ALWAYS AS (a + b) VIRTUAL DEFAULT 0 NOT NULL ON UPDATE CURRENT_TIMESTAMP AUTO_INCREMENT",
		},
		{
			name: "Generated STORED with other flags off",
			input: utils.SrcColumnDetails{
				Datatype: "INT",
				GeneratedColumn: utils.GeneratedColumnInfo{
					IsPresent: true,
					Statement: "(c * d)",
					IsVirtual: false,
				},
				NotNull: false, // Test that NOT NULL is absent
				AutoGen: ddl.AutoGenCol{
					Name:           "my_sequence",
					GenerationType: "some_other_type",
				},
			},
			want: "INT GENERATED ALWAYS AS (c * d) STORED",
		},
		{
			name:  "Empty struct",
			input: utils.SrcColumnDetails{},
			want:  "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := sourceColumnDefinitionToString(tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCalculateTableDbChangesAndImpact(t *testing.T) {
	testCases := []struct {
		name        string
		input       utils.TableAssessment
		wantChanges string
		wantImpact  string
	}{
		{
			name: "No issues",
			input: utils.TableAssessment{
				CompatibleCharset:   true,
				SizeIncreaseInBytes: 0,
			},
			wantChanges: "None",
			wantImpact:  "None",
		},
		{
			name: "Incompatible charset and storage increase",
			input: utils.TableAssessment{
				CompatibleCharset:   false,
				SizeIncreaseInBytes: 1024,
			},
			wantChanges: "charset",
			wantImpact:  "storage increase",
		},
		{
			name: "Storage decrease only",
			input: utils.TableAssessment{
				CompatibleCharset:   true,
				SizeIncreaseInBytes: -512,
			},
			wantChanges: "None",
			wantImpact:  "storage decrease",
		},
		{
			name:  "Zero-value struct",
			input: utils.TableAssessment{},
			// CompatibleCharset defaults to false in a zero-value struct.
			wantChanges: "charset",
			wantImpact:  "None",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotChanges, gotImpact := calculateTableDbChangesAndImpact(tc.input)
			assert.Equal(t, tc.wantChanges, gotChanges)
			assert.Equal(t, tc.wantImpact, gotImpact)
		})
	}
}

func TestCalculateColumnDbChangesAndImpact(t *testing.T) {
	// Define common base structs to reduce repetition
	baseSrcCol := func() *utils.SrcColumnDetails {
		return &utils.SrcColumnDetails{}
	}
	baseSpCol := func() *utils.SpColumnDetails {
		return &utils.SpColumnDetails{}
	}

	testCases := []struct {
		name            string
		input           utils.ColumnAssessment
		wantChanges     string
		wantImpact      string
		wantEffort      string
		wantActionItems *[]string
	}{
		{
			name: "Fully compatible",
			input: utils.ColumnAssessment{
				SourceColDef:       baseSrcCol(),
				SpannerColDef:      baseSpCol(),
				CompatibleDataType: true,
			},
			wantChanges:     "None",
			wantImpact:      "None",
			wantEffort:      "Automatic",
			wantActionItems: &[]string{},
		},
		{
			name: "All issues present",
			input: utils.ColumnAssessment{
				SourceColDef: &utils.SrcColumnDetails{
					Datatype:               "bigint",
					IsUnsigned:             true,
					IsOnUpdateTimestampSet: true,
					DefaultValue:           ddl.DefaultValue{IsPresent: true, Value: ddl.Expression{Statement: "some_val"}},
					AutoGen:                ddl.AutoGenCol{Name: "some_name", GenerationType: constants.AUTO_INCREMENT},
					GeneratedColumn:        utils.GeneratedColumnInfo{IsPresent: true},
				},
				SpannerColDef:       &utils.SpColumnDetails{DefaultValue: ddl.DefaultValue{IsPresent: false}},
				CompatibleDataType:  false,
				SizeIncreaseInBytes: 128,
			},
			wantChanges:     "type,feature,feature,feature",
			wantImpact:      "storage increase,potential overflow",
			wantEffort:      "Small",
			wantActionItems: &[]string{"Update queries to include PENDING_COMMIT_TIMESTAMP", "Alter column to apply default value", "Update schema to add generated column"},
		},
		{
			name: "Storage decrease and defaultValue NULL is ignored",
			input: utils.ColumnAssessment{
				SourceColDef: &utils.SrcColumnDetails{
					DefaultValue: ddl.DefaultValue{IsPresent: true, Value: ddl.Expression{Statement: "NULL"}},
				},
				SpannerColDef:       &utils.SpColumnDetails{DefaultValue: ddl.DefaultValue{IsPresent: false}},
				SizeIncreaseInBytes: -128,
				CompatibleDataType:  true,
			},
			wantChanges:     "None",
			wantImpact:      "storage decrease",
			wantEffort:      "Automatic",
			wantActionItems: &[]string{},
		},
		{
			name:            "Zero-value struct",
			input:           utils.ColumnAssessment{},
			wantChanges:     "type", // CompatibleDataType defaults to false
			wantImpact:      "None",
			wantEffort:      "Automatic",
			wantActionItems: &[]string{},
		},
		{
			name: "Nil source column and Spanner column",
			input: utils.ColumnAssessment{
				SourceColDef:  nil,
				SpannerColDef: nil,
			},
			wantChanges:     "type", // CompatibleDataType defaults to false
			wantImpact:      "None",
			wantEffort:      "Automatic",
			wantActionItems: &[]string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotChanges, gotImpact, gotEffort, gotActionItems := calculateColumnDbChangesAndImpact(tc.input)
			assert.Equal(t, tc.wantChanges, gotChanges)
			assert.Equal(t, tc.wantImpact, gotImpact)
			assert.Equal(t, tc.wantEffort, gotEffort)
			assert.Equal(t, *tc.wantActionItems, *gotActionItems)
		})
	}
}

func TestPopulateChangesForUnsupportedElements(t *testing.T) {
	t.Run("Standard case on empty row", func(t *testing.T) {
		row := &SchemaReportRow{}
		populateChangesForUnsupportedElements(row)

		assert.Equal(t, "Not supported", row.targetName)
		assert.Equal(t, "N/A", row.targetDefinition)
		assert.Equal(t, "Not Supported", row.dbChangeEffort)
		assert.Equal(t, "Drop", row.dbChanges)
		assert.Equal(t, "Less Compute", row.dbImpact)
		assert.Equal(t, "Rewrite", row.codeChangeEffort)
		assert.Equal(t, "Manual", row.codeChangeType)
		assert.Equal(t, "Unknown", row.codeImpactedFiles)
		assert.Equal(t, "", row.codeSnippets)
		assert.Equal(t, &[]string{"Rewrite in application code"}, row.actionItems)
	})

	t.Run("Overwrites existing data", func(t *testing.T) {
		row := &SchemaReportRow{
			targetName:     "old_target",
			dbChanges:      "old_changes",
			codeChangeType: "old_type",
			actionItems:    &[]string{"old_action"},
		}
		populateChangesForUnsupportedElements(row)

		assert.Equal(t, "Not supported", row.targetName, "Should overwrite existing targetName")
		assert.Equal(t, "Drop", row.dbChanges, "Should overwrite existing dbChanges")
		assert.Equal(t, "Manual", row.codeChangeType, "Should overwrite existing codeChangeType")
		assert.Equal(t, &[]string{"Rewrite in application code"}, row.actionItems, "Should overwrite existing actionItems")
	})
}

// common test cases for populateStoredProcedureInfo, populateTriggerInfo, and populateFunctionInfo as they share similar logic
func TestPopulateUnsupportedObjects(t *testing.T) {
	testCases := []struct {
		name              string
		initialRows       []SchemaReportRow
		runFunc           func(rows *[]SchemaReportRow)
		expectedLen       int
		expectedLastRow   SchemaReportRow
		expectedFirstRows []SchemaReportRow
	}{
		{
			name:        "populateTriggerInfo - Single Trigger",
			initialRows: []SchemaReportRow{},
			runFunc: func(rows *[]SchemaReportRow) {
				triggers := map[string]utils.TriggerAssessment{
					"trg1": {Name: "my_trigger", TargetTable: "my_table", Operation: "INSERT"},
				}
				populateTriggerInfo(triggers, rows)
			},
			expectedLen: 1,
			expectedLastRow: SchemaReportRow{
				element:          "my_trigger",
				elementType:      "Trigger",
				sourceName:       "my_trigger",
				sourceTableName:  "my_table",
				sourceDefinition: "INSERT",
				targetName:       "Not supported",
			},
		},
		{
			name:        "populateStoredProcedureInfo - Multiple Procedures",
			initialRows: []SchemaReportRow{},
			runFunc: func(rows *[]SchemaReportRow) {
				sprocs := map[string]utils.StoredProcedureAssessment{
					"sp1": {Name: "proc1", Definition: "BEGIN...END"},
					"sp2": {Name: "proc2", Definition: "SELECT 1"},
				}
				populateStoredProcedureInfo(sprocs, rows)
			},
			expectedLen: 2,
			// We don't check the last row specifically because map iteration order is not guaranteed.
			// The length check is the most important assertion here.
		},
		{
			name:        "populateTriggerInfo - Nil Map",
			initialRows: []SchemaReportRow{},
			runFunc: func(rows *[]SchemaReportRow) {
				populateTriggerInfo(nil, rows)
			},
			expectedLen: 0,
		},
		{
			name: "populateFunctionInfo - Appends to existing rows",
			initialRows: []SchemaReportRow{
				{elementType: "Existing Row"},
			},
			runFunc: func(rows *[]SchemaReportRow) {
				funcs := map[string]utils.FunctionAssessment{
					"fn1": {Name: "another_func"},
				}
				populateFunctionInfo(funcs, rows)
			},
			expectedLen:       2,
			expectedFirstRows: []SchemaReportRow{{elementType: "Existing Row"}},
			expectedLastRow: SchemaReportRow{
				element:         "another_func",
				elementType:     "Function",
				sourceName:      "another_func",
				sourceTableName: "N/A",
				targetName:      "Not supported",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.initialRows
			tc.runFunc(&rows)

			assert.Len(t, rows, tc.expectedLen)

			// Verify the original rows are untouched if they existed.
			if tc.expectedFirstRows != nil {
				assert.Equal(t, tc.expectedFirstRows, rows[:len(tc.expectedFirstRows)])
			}

			// Verify the last added row's content where predictable.
			if tc.expectedLen > 0 && tc.expectedLastRow.elementType != "" {
				lastRow := rows[len(rows)-1]
				assert.Equal(t, tc.expectedLastRow.element, lastRow.element)
				assert.Equal(t, tc.expectedLastRow.elementType, lastRow.elementType)
				assert.Equal(t, tc.expectedLastRow.sourceName, lastRow.sourceName)
				assert.Equal(t, tc.expectedLastRow.sourceTableName, lastRow.sourceTableName)
				assert.Equal(t, tc.expectedLastRow.sourceDefinition, lastRow.sourceDefinition)
				assert.Equal(t, tc.expectedLastRow.targetName, lastRow.targetName) // From populateChangesForUnsupportedElements
			}
		})
	}
}

func TestPopulateViewInfo(t *testing.T) {
	testCases := []struct {
		name              string
		initialRows       []SchemaReportRow
		inputMap          map[string]utils.ViewAssessment
		expectedLen       int
		expectedLastRow   SchemaReportRow
		expectedFirstRows []SchemaReportRow
	}{
		{
			name:        "Standard case with a single view",
			initialRows: []SchemaReportRow{},
			inputMap: map[string]utils.ViewAssessment{
				"v1": {SrcName: "active_users", SpName: "active_users_sp", SrcViewType: "SELECT * FROM users WHERE active = 1"},
			},
			expectedLen: 1,
			expectedLastRow: SchemaReportRow{
				element:           "active_users",
				elementType:       "View",
				sourceName:        "active_users",
				sourceDefinition:  "SELECT * FROM users WHERE active = 1",
				sourceTableName:   "N/A",
				targetName:        "active_users_sp",
				targetDefinition:  "Unknown",
				dbChangeEffort:    "Small",
				dbChanges:         "Unknown",
				dbImpact:          "None",
				codeChangeType:    "Manual",
				codeChangeEffort:  "Unknown",
				codeImpactedFiles: "Unknown",
				codeSnippets:      "",
				actionItems:       &[]string{"Create view manually"},
			},
		},
		{
			name:        "Multiple views",
			initialRows: []SchemaReportRow{},
			inputMap: map[string]utils.ViewAssessment{
				"v1": {SrcName: "view1"},
				"v2": {SrcName: "view2"},
			},
			expectedLen: 2,
		},
		{
			name:        "Nil map",
			initialRows: []SchemaReportRow{},
			inputMap:    nil,
			expectedLen: 0,
		},
		{
			name: "Appends to existing rows",
			initialRows: []SchemaReportRow{
				{elementType: "Existing Row 1"},
			},
			inputMap: map[string]utils.ViewAssessment{
				"v1": {SrcName: "appended_view"},
			},
			expectedLen:       2,
			expectedFirstRows: []SchemaReportRow{{elementType: "Existing Row 1"}},
			expectedLastRow:   SchemaReportRow{elementType: "View", sourceName: "appended_view"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.initialRows
			populateViewInfo(tc.inputMap, &rows)

			assert.Len(t, rows, tc.expectedLen)

			if tc.expectedFirstRows != nil {
				assert.Equal(t, tc.expectedFirstRows, rows[:len(tc.expectedFirstRows)])
			}

			if tc.expectedLen > 0 && tc.expectedLastRow.elementType != "" {
				lastRow := rows[len(rows)-1]
				// Assert on all the fields for the standard case.
				if tc.name == "Standard case with a single view" {
					assert.Equal(t, tc.expectedLastRow, lastRow)
				} else {
					// For other cases, just check a key field to confirm the right row was added.
					assert.Equal(t, tc.expectedLastRow.elementType, lastRow.elementType)
					assert.Equal(t, tc.expectedLastRow.sourceName, lastRow.sourceName)
				}
			}
		})
	}
}

func TestPopulateIndexes(t *testing.T) {
	testCases := []struct {
		name              string
		initialRows       []SchemaReportRow
		tableAssessment   utils.TableAssessment
		spTableName       string
		expectedLen       int
		expectedLastRow   SchemaReportRow
		expectedFirstRows []SchemaReportRow
	}{
		{
			name:        "Standard case with a single index",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{Name: "users"},
				SourceIndexDef: []utils.SrcIndexDetails{
					{Name: "idx_email", Ddl: "CREATE INDEX idx_email ON users(email)"},
				},
				SpannerIndexDef: []utils.SpIndexDetails{
					{Name: "idx_email_sp", Ddl: "CREATE INDEX idx_email_sp ON users(email)"},
				},
			},
			spTableName: "users_sp",
			expectedLen: 1,
			expectedLastRow: SchemaReportRow{
				element:           "users.idx_email",
				elementType:       "Index",
				sourceTableName:   "users",
				sourceName:        "idx_email",
				sourceDefinition:  "CREATE INDEX idx_email ON users(email)",
				targetName:        "users_sp.idx_email_sp",
				targetDefinition:  "CREATE INDEX idx_email_sp ON users(email)",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "None",
				codeChangeEffort:  "None",
				codeChangeType:    "None",
				codeImpactedFiles: "None",
				codeSnippets:      "None",
			},
		},
		{
			name:        "Multiple indexes",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef:  &utils.SrcTableDetails{Name: "users"},
				SourceIndexDef:  []utils.SrcIndexDetails{{Id: "idx1"}, {Id: "idx2"}},
				SpannerIndexDef: []utils.SpIndexDetails{{Id: "idx1"}, {Id: "idx2"}},
			},
			spTableName: "users_sp",
			expectedLen: 2,
		},
		{
			name: "Appends to existing rows",
			initialRows: []SchemaReportRow{
				{elementType: "Existing Row"},
			},
			tableAssessment: utils.TableAssessment{
				SourceTableDef:  &utils.SrcTableDetails{Name: "users"},
				SourceIndexDef:  []utils.SrcIndexDetails{{Id: "idx1", Name: "appended_idx"}},
				SpannerIndexDef: []utils.SpIndexDetails{{Id: "idx1"}},
			},
			spTableName:       "users_sp",
			expectedLen:       2,
			expectedFirstRows: []SchemaReportRow{{elementType: "Existing Row"}},
			expectedLastRow:   SchemaReportRow{elementType: "Index", sourceName: "appended_idx"},
		},
		{
			name:        "No indexes - empty slice",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceIndexDef:  []utils.SrcIndexDetails{},
				SpannerIndexDef: []utils.SpIndexDetails{},
			},
			spTableName: "users_sp",
			expectedLen: 0,
		},
		{
			name:        "Nil SourceTableDef",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef:  nil,
				SpannerTableDef: nil,
			},
			spTableName: "",
			expectedLen: 0, // No rows should be added
		},
		{
			name:        "Mismatched index count skips row and does not panic",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{Name: "users"},
				// Source has one index.
				SourceIndexDef: []utils.SrcIndexDetails{
					{Name: "idx_email", Ddl: "CREATE INDEX idx_email ON users(email)"},
				},
				// Spanner has zero indexes. The bounds check should skip thisiteration, prevent ing a panic.
				SpannerIndexDef: []utils.SpIndexDetails{},
			},
			spTableName: "users_sp",
			expectedLen: 0, // No row should be added because the iteration is skipped.
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.initialRows
			populateIndexes(tc.tableAssessment, tc.spTableName, &rows)

			assert.Len(t, rows, tc.expectedLen)

			if tc.expectedFirstRows != nil {
				assert.Equal(t, tc.expectedFirstRows, rows[:len(tc.expectedFirstRows)])
			}

			if tc.expectedLen > 0 && tc.expectedLastRow.elementType != "" {
				lastRow := rows[len(rows)-1]
				if tc.name == "Standard case with a single index" {
					assert.Equal(t, tc.expectedLastRow, lastRow)
				} else {
					assert.Equal(t, tc.expectedLastRow.elementType, lastRow.elementType)
					assert.Equal(t, tc.expectedLastRow.sourceName, lastRow.sourceName)
				}
			}
		})
	}
}

func TestPopulateForeignKeys(t *testing.T) {
	testCases := []struct {
		name              string
		initialRows       []SchemaReportRow
		tableAssessment   utils.TableAssessment
		spTableName       string
		expectedLen       int
		expectedLastRow   SchemaReportRow
		expectedFirstRows []SchemaReportRow
	}{
		{
			name:        "Compatible foreign key",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{
					Name: "orders",
					SourceForeignKey: map[string]utils.SourceForeignKey{
						"fk1": {Definition: schema.ForeignKey{Name: "fk_customer", OnUpdate: "NO ACTION", OnDelete: "NO ACTION"}, Ddl: "CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id)"},
					},
				},
				SpannerTableDef: &utils.SpTableDetails{
					SpannerForeignKey: map[string]utils.SpannerForeignKey{
						"fk1": {Definition: ddl.Foreignkey{Name: "fk_customer", OnUpdate: "NO ACTION", OnDelete: "NO ACTION"}, Ddl: "CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id)"},
					},
				},
			},
			spTableName: "orders_sp",
			expectedLen: 1,
			expectedLastRow: SchemaReportRow{
				element:           "orders.fk_customer",
				elementType:       "Foreign Key",
				sourceTableName:   "orders",
				sourceName:        "fk_customer",
				sourceDefinition:  "CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id)",
				targetName:        "orders_sp.fk_customer",
				targetDefinition:  "CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id)",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "None",
				codeChangeType:    "None",
				codeChangeEffort:  "None",
				codeImpactedFiles: "None",
				codeSnippets:      "None",
			},
		},
		{
			name:        "Incompatible foreign key (ON UPDATE SET NULL)",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{
					Name: "orders",
					SourceForeignKey: map[string]utils.SourceForeignKey{
						"fk2": {Definition: schema.ForeignKey{Name: "fk_product", OnUpdate: "SET NULL"}, Ddl: "CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id) ON UPDATE SET NULL"},
					},
				},
				SpannerTableDef: &utils.SpTableDetails{
					SpannerForeignKey: map[string]utils.SpannerForeignKey{
						"fk2": {Definition: ddl.Foreignkey{Name: "fk_product", OnUpdate: "NO ACTION"}, Ddl: "CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)"},
					},
				},
			},
			spTableName: "orders_sp",
			expectedLen: 1,
			expectedLastRow: SchemaReportRow{
				element:           "orders.fk_product",
				elementType:       "Foreign Key",
				sourceName:        "fk_product",
				sourceDefinition:  "CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id) ON UPDATE SET NULL",
				sourceTableName:   "orders",
				targetName:        "orders_sp.fk_product",
				targetDefinition:  "CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)",
				dbChangeEffort:    "Automatic",
				dbChanges:         "reference_option",
				dbImpact:          "None",
				codeChangeEffort:  "Modify",
				codeChangeType:    "Manual",
				codeImpactedFiles: "Unknown",
				codeSnippets:      "None",
			},
		},
		{
			name:        "Spanner foreign key not found",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{
					Name: "orders",
					SourceForeignKey: map[string]utils.SourceForeignKey{
						"fk_missing": {Definition: schema.ForeignKey{Name: "fk_missing"}, Ddl: "CONSTRAINT fk_missing FOREIGN KEY (customer_id) REFERENCES customers(id)"},
					},
				},
				SpannerTableDef: &utils.SpTableDetails{
					Name: "orders_sp",
					// SpannerForeignKey is empty, so fk_missing won't be found.
					SpannerForeignKey: map[string]utils.SpannerForeignKey{},
				},
			},
			spTableName: "orders_sp",
			expectedLen: 0, // No row should be added.
		},
		{
			name: "No foreign keys (empty map)",
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{SourceForeignKey: map[string]utils.SourceForeignKey{}},
			},
			expectedLen: 0,
		},
		{
			name: "Appends to existing rows",
			initialRows: []SchemaReportRow{
				{elementType: "Existing Row"},
			},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{
					Name: "orders",
					SourceForeignKey: map[string]utils.SourceForeignKey{
						"fk3": {Definition: schema.ForeignKey{Name: "fk_append"}, Ddl: "CONSTRAINT fk_append FOREIGN KEY (a) REFERENCES b(c)"},
					},
				},
				SpannerTableDef: &utils.SpTableDetails{
					SpannerForeignKey: map[string]utils.SpannerForeignKey{
						"fk3": {Definition: ddl.Foreignkey{Name: "fk_append"}, Ddl: "CONSTRAINT fk_append FOREIGN KEY (a) REFERENCES b(c)"},
					},
				},
			},
			expectedLen:       2,
			expectedFirstRows: []SchemaReportRow{{elementType: "Existing Row"}},
			expectedLastRow: SchemaReportRow{
				element:           "orders.fk_append",
				elementType:       "Foreign Key",
				sourceName:        "fk_append",
				sourceDefinition:  "CONSTRAINT fk_append FOREIGN KEY (a) REFERENCES b(c)",
				sourceTableName:   "orders",
				targetName:        ".fk_append",
				targetDefinition:  "CONSTRAINT fk_append FOREIGN KEY (a) REFERENCES b(c)",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "None",
				codeChangeEffort:  "None",
				codeChangeType:    "None",
				codeImpactedFiles: "None",
				codeSnippets:      "None",
			},
		},
		{
			name:        "Nil SourceTableDef does not panic",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef:  nil,
				SpannerTableDef: &utils.SpTableDetails{},
			},
			spTableName: "orders_sp",
			expectedLen: 0, // Expect no rows to be added.
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.initialRows
			populateForeignKeys(tc.tableAssessment, tc.spTableName, &rows)

			assert.Len(t, rows, tc.expectedLen)

			if tc.expectedFirstRows != nil {
				assert.Equal(t, tc.expectedFirstRows, rows[:len(tc.expectedFirstRows)])
			}

			if tc.expectedLen > 0 && tc.expectedLastRow.elementType != "" {
				lastRow := rows[len(rows)-1]
				assert.Equal(t, tc.expectedLastRow, lastRow)
			}
		})
	}
}

func TestPopulateCheckConstraints(t *testing.T) {
	testCases := []struct {
		name              string
		initialRows       []SchemaReportRow
		tableAssessment   utils.TableAssessment
		spTableName       string
		expectedLen       int
		expectedLastRow   SchemaReportRow
		expectedFirstRows []SchemaReportRow
	}{
		{
			name:        "Successfully migrated check constraint",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{
					Name: "products",
					CheckConstraints: map[string]schema.CheckConstraint{
						"cc1": {Name: "price_check", Expr: "price > 0"},
					},
				},
				SpannerTableDef: &utils.SpTableDetails{
					Name: "products_sp",
					CheckConstraints: map[string]ddl.CheckConstraint{
						"cc1": {Name: "price_check", Expr: "price > 0"},
					},
				},
			},
			spTableName: "products_sp",
			expectedLen: 1,
			expectedLastRow: SchemaReportRow{
				element:           "products.price_check",
				elementType:       "Check Constraint",
				sourceTableName:   "products",
				sourceName:        "price_check",
				sourceDefinition:  "price > 0",
				targetName:        "products_sp.price_check",
				targetDefinition:  "price > 0",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "None",
				codeChangeType:    "None",
				codeChangeEffort:  "None",
				codeImpactedFiles: "None",
				codeSnippets:      "None",
			},
		},
		{
			name:        "Dropped check constraint",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{
					Name: "products",
					CheckConstraints: map[string]schema.CheckConstraint{
						"cc2": {Name: "stock_check", Expr: "stock >= 0"},
					},
				},
				SpannerTableDef: &utils.SpTableDetails{ // No corresponding constraint in Spanner
					Name:             "products_sp",
					CheckConstraints: map[string]ddl.CheckConstraint{},
				},
			},
			spTableName: "products_sp",
			expectedLen: 1,
			expectedLastRow: SchemaReportRow{
				element:           "products.stock_check",
				elementType:       "Check Constraint",
				sourceTableName:   "products",
				sourceName:        "stock_check",
				sourceDefinition:  "stock >= 0",
				targetName:        "N/A",
				targetDefinition:  "N/A",
				dbChangeEffort:    "Small",
				dbChanges:         "Unknown",
				dbImpact:          "",
				codeChangeType:    "None",
				codeChangeEffort:  "None",
				codeImpactedFiles: "None",
				codeSnippets:      "None",
				actionItems:       &[]string{"Alter column to apply check constraint"},
			},
		},
		{
			name: "No check constraints",
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{
					Name:             "products",
					CheckConstraints: map[string]schema.CheckConstraint{},
				},
			},
			expectedLen: 0,
		},
		{
			name: "Appends to existing rows",
			initialRows: []SchemaReportRow{
				{elementType: "Existing Row"},
			},
			tableAssessment: utils.TableAssessment{
				SourceTableDef: &utils.SrcTableDetails{
					Name: "products",
					CheckConstraints: map[string]schema.CheckConstraint{
						"cc1": {Name: "append_check", Expr: "val > 0"},
					},
				},
				SpannerTableDef: &utils.SpTableDetails{
					Name: "products_sp",
					CheckConstraints: map[string]ddl.CheckConstraint{
						"cc1": {Name: "append_check", Expr: "val > 0"},
					},
				},
			},
			expectedLen:       2,
			expectedFirstRows: []SchemaReportRow{{elementType: "Existing Row"}},
			expectedLastRow: SchemaReportRow{
				element:           "products.append_check",
				elementType:       "Check Constraint",
				sourceTableName:   "products",
				sourceName:        "append_check",
				sourceDefinition:  "val > 0",
				targetName:        ".append_check",
				targetDefinition:  "val > 0",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "None",
				codeChangeType:    "None",
				codeChangeEffort:  "None",
				codeImpactedFiles: "None",
				codeSnippets:      "None",
			},
		},
		{
			name:        "Nil source table definition",
			initialRows: []SchemaReportRow{},
			tableAssessment: utils.TableAssessment{
				SourceTableDef:  nil,
				SpannerTableDef: nil,
			},
			spTableName: "products_sp",
			expectedLen: 0, // No rows should be added
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.initialRows
			populateCheckConstraints(tc.tableAssessment, tc.spTableName, &rows)

			assert.Len(t, rows, tc.expectedLen)

			if tc.expectedFirstRows != nil {
				assert.Equal(t, tc.expectedFirstRows, rows[:len(tc.expectedFirstRows)])
			}

			if tc.expectedLen > 0 && len(rows) > len(tc.initialRows) {
				lastRow := rows[len(rows)-1]
				assert.Equal(t, tc.expectedLastRow, lastRow)
			}
		})
	}
}

func TestSpannerColumnDefinitionToString(t *testing.T) {
	testCases := []struct {
		name  string
		input utils.SpColumnDetails
		want  string
	}{
		{
			name:  "Simple INT64 NOT NULL",
			input: utils.SpColumnDetails{Name: "id", Datatype: "INT64", NotNull: true},
			want:  "id INT64 NOT NULL ",
		},
		{
			name:  "STRING with length",
			input: utils.SpColumnDetails{Name: "name", Datatype: "STRING", Len: 255, NotNull: false},
			want:  "name STRING(255)",
		},
		{
			name:  "BYTES with MAX length",
			input: utils.SpColumnDetails{Name: "data", Datatype: "BYTES", Len: ddl.MaxLength, NotNull: false},
			want:  "data BYTES(MAX)",
		},
		{
			name:  "ARRAY of INT64",
			input: utils.SpColumnDetails{Name: "user_ids", Datatype: "INT64", IsArray: true, NotNull: false},
			want:  "user_ids ARRAY<INT64>",
		},
		{
			name: "DEFAULT value",
			input: utils.SpColumnDetails{Name: "status", Datatype: "STRING", Len: 50, NotNull: true, DefaultValue: ddl.DefaultValue{
				IsPresent: true, Value: ddl.Expression{Statement: "'active'"}},
			},
			want: "status STRING(50) NOT NULL  DEFAULT ('active')",
		},
		{
			name: "AutoGen sequence",
			input: utils.SpColumnDetails{Name: "seq_val", Datatype: "INT64", NotNull: true, AutoGen: ddl.AutoGenCol{
				Name: "my_sequence", GenerationType: constants.SEQUENCE},
			},
			want: "seq_val INT64 NOT NULL  DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE my_sequence))",
		},
		{
			name:  "ARRAY of STRING(MAX) NOT NULL",
			input: utils.SpColumnDetails{Name: "photo_urls", Datatype: "STRING", Len: ddl.MaxLength, IsArray: true, NotNull: true},
			want:  "photo_urls ARRAY<STRING(MAX)> NOT NULL ",
		},
		{
			name:  "Zero-value struct",
			input: utils.SpColumnDetails{},
			want:  " ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := spannerColumnDefinitionToString(tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPopulateTableCodeImpact(t *testing.T) {
	testCases := []struct {
		name        string
		srcTable    utils.SrcTableDetails
		spTable     utils.SpTableDetails
		snippets    *[]utils.Snippet
		expectedRow SchemaReportRow
	}{
		{
			name:     "Table not renamed",
			srcTable: utils.SrcTableDetails{Name: "users"},
			spTable:  utils.SpTableDetails{Name: "users"},
			snippets: &[]utils.Snippet{
				{TableName: "users", Id: "s1", RelativeFilePath: "file1.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "None",
				codeChangeEffort:  "None",
				codeImpactedFiles: "None",
				codeSnippets:      "None",
			},
		},
		{
			name:     "Table renamed with related snippets",
			srcTable: utils.SrcTableDetails{Name: "users"},
			spTable:  utils.SpTableDetails{Name: "Users"},
			snippets: &[]utils.Snippet{
				{TableName: "users", Id: "s1", RelativeFilePath: "file1.java"},
				{TableName: "orders", Id: "s2", RelativeFilePath: "file2.java"},
				{TableName: "users", Id: "s3", RelativeFilePath: "file3.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "Suggested",
				codeChangeEffort:  "TBD",
				codeImpactedFiles: "file1.java,file3.java",
				codeSnippets:      "s1,s3",
			},
		},
		{
			name:     "Table renamed with no related snippets",
			srcTable: utils.SrcTableDetails{Name: "users"},
			spTable:  utils.SpTableDetails{Name: "Users"},
			snippets: &[]utils.Snippet{
				{TableName: "orders", Id: "s2", RelativeFilePath: "file2.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "None",
				codeChangeEffort:  "None",
				codeImpactedFiles: "None",
				codeSnippets:      "",
			},
		},
		{
			name:     "Table renamed, no code analysis performed (nil snippets)",
			srcTable: utils.SrcTableDetails{Name: "users"},
			spTable:  utils.SpTableDetails{Name: "Users"},
			snippets: nil,
			expectedRow: SchemaReportRow{
				codeChangeType:    "Unavailable",
				codeChangeEffort:  "Unavailable",
				codeImpactedFiles: "Unavailable",
				codeSnippets:      "Unavailable",
			},
		},
		{
			name:     "Table renamed, handles duplicate file paths",
			srcTable: utils.SrcTableDetails{Name: "users"},
			spTable:  utils.SpTableDetails{Name: "Users"},
			snippets: &[]utils.Snippet{
				{TableName: "users", Id: "s1", RelativeFilePath: "file1.java"},
				{TableName: "users", Id: "s4", RelativeFilePath: "file1.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "Suggested",
				codeChangeEffort:  "TBD",
				codeImpactedFiles: "file1.java",
				codeSnippets:      "s1,s4",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row := &SchemaReportRow{}
			populateTableCodeImpact(tc.srcTable, tc.spTable, tc.snippets, row)

			assert.Equal(t, tc.expectedRow.codeChangeType, row.codeChangeType)
			assert.Equal(t, tc.expectedRow.codeChangeEffort, row.codeChangeEffort)
			assert.Equal(t, tc.expectedRow.codeImpactedFiles, row.codeImpactedFiles)
			assert.Equal(t, tc.expectedRow.codeSnippets, row.codeSnippets)
		})
	}
}

func TestPopulateColumnCodeImpact(t *testing.T) {
	testCases := []struct {
		name        string
		srcColumn   utils.SrcColumnDetails
		assessment  utils.ColumnAssessment
		snippets    *[]utils.Snippet
		expectedRow SchemaReportRow
	}{
		{
			name:       "Compatible data type",
			assessment: utils.ColumnAssessment{CompatibleDataType: true},
			expectedRow: SchemaReportRow{
				codeChangeType:    "None",
				codeChangeEffort:  "None",
				codeImpactedFiles: "None",
				codeSnippets:      "None",
			},
		},
		{
			name:       "Incompatible type, no code analysis performed (nil snippets)",
			srcColumn:  utils.SrcColumnDetails{TableName: "users", Name: "email"},
			assessment: utils.ColumnAssessment{CompatibleDataType: false},
			snippets:   nil,
			expectedRow: SchemaReportRow{
				codeChangeType:    "Unavailable",
				codeChangeEffort:  "Unavailable",
				codeImpactedFiles: "Unavailable",
				codeSnippets:      "Unavailable",
			},
		},
		{
			name:       "ON UPDATE timestamp",
			srcColumn:  utils.SrcColumnDetails{IsOnUpdateTimestampSet: true},
			assessment: utils.ColumnAssessment{CompatibleDataType: false},
			snippets: &[]utils.Snippet{
				{TableName: "users", ColumnName: "email", Id: "s1", RelativeFilePath: "file1.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "Manual",
				codeChangeEffort:  "Large",
				codeImpactedFiles: "TBD",
				codeSnippets:      "",
			},
		},
		{
			name:       "Incompatible type with related snippets",
			srcColumn:  utils.SrcColumnDetails{TableName: "users", Name: "email"},
			assessment: utils.ColumnAssessment{CompatibleDataType: false},
			snippets: &[]utils.Snippet{
				{TableName: "users", ColumnName: "email", Id: "s1", RelativeFilePath: "file1.java"},
				{TableName: "users", ColumnName: "id", Id: "s2", RelativeFilePath: "file2.java"},
				{TableName: "users", ColumnName: "email", Id: "s3", RelativeFilePath: "file3.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "Suggested",
				codeChangeEffort:  "Small",
				codeImpactedFiles: "file1.java,file3.java",
				codeSnippets:      "s1,s3",
			},
		},
		{
			name:       "Incompatible type with no related snippets",
			srcColumn:  utils.SrcColumnDetails{TableName: "users", Name: "email"},
			assessment: utils.ColumnAssessment{CompatibleDataType: false},
			snippets: &[]utils.Snippet{
				{TableName: "orders", ColumnName: "price", Id: "s4", RelativeFilePath: "file4.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "None",
				codeChangeEffort:  "None",
				codeImpactedFiles: "None",
				codeSnippets:      "",
			},
		},
		{
			name:       "Incompatible type, handles duplicate file paths",
			srcColumn:  utils.SrcColumnDetails{TableName: "users", Name: "email"},
			assessment: utils.ColumnAssessment{CompatibleDataType: false},
			snippets: &[]utils.Snippet{
				{TableName: "users", ColumnName: "email", Id: "s1", RelativeFilePath: "file1.java"},
				{TableName: "users", ColumnName: "email", Id: "s5", RelativeFilePath: "file1.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "Suggested",
				codeChangeEffort:  "Small",
				codeImpactedFiles: "file1.java",
				codeSnippets:      "s1,s5",
			},
		},
		{
			name:       "ON UPDATE takes precedence over snippet search",
			srcColumn:  utils.SrcColumnDetails{TableName: "users", Name: "last_updated", IsOnUpdateTimestampSet: true},
			assessment: utils.ColumnAssessment{CompatibleDataType: false},
			snippets: &[]utils.Snippet{
				{TableName: "users", ColumnName: "last_updated", Id: "s1", RelativeFilePath: "file1.java"},
			},
			expectedRow: SchemaReportRow{
				codeChangeType:    "Manual",
				codeChangeEffort:  "Large",
				codeImpactedFiles: "TBD",
				codeSnippets:      "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row := &SchemaReportRow{}
			// spColumnDef is not used by the function, so an empty struct is sufficient.
			populateColumnCodeImpact(tc.srcColumn, utils.SpColumnDetails{}, tc.snippets, row, tc.assessment)

			assert.Equal(t, tc.expectedRow.codeChangeType, row.codeChangeType)
			assert.Equal(t, tc.expectedRow.codeChangeEffort, row.codeChangeEffort)
			assert.Equal(t, tc.expectedRow.codeImpactedFiles, row.codeImpactedFiles)
			assert.Equal(t, tc.expectedRow.codeSnippets, row.codeSnippets)
		})
	}
}

func TestPopulateSequenceInfo(t *testing.T) {
	testCases := []struct {
		name              string
		initialRows       []SchemaReportRow
		sequences         map[string]ddl.Sequence
		tableAssessments  []utils.TableAssessment
		codeSnippets      *[]utils.Snippet
		expectedLen       int
		expectedLastRow   SchemaReportRow
		expectedFirstRows []SchemaReportRow
	}{
		{
			name:        "Sequence linked to a single known table",
			initialRows: []SchemaReportRow{},
			sequences: map[string]ddl.Sequence{
				"seq1": {Name: "my_seq", ColumnsUsingSeq: map[string][]string{"t1": {"c1"}}},
			},
			tableAssessments: []utils.TableAssessment{
				{SourceTableDef: &utils.SrcTableDetails{Id: "t1", Name: "my_table"}},
			},
			codeSnippets: &[]utils.Snippet{},
			expectedLen:  1,
			expectedLastRow: SchemaReportRow{
				element:           "N/A",
				elementType:       "Sequence",
				sourceTableName:   "my_table",
				sourceName:        "my_seq",
				sourceDefinition:  "N/A",
				targetName:        "my_seq",
				targetDefinition:  "CREATE SEQUENCE my_seq",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "N/A",
				codeChangeEffort:  "Modify",
				codeChangeType:    "Manual",
				codeImpactedFiles: "Unknown",
				codeSnippets:      "Unknown",
			},
		},
		{
			name:        "Sequence linked to multiple tables",
			initialRows: []SchemaReportRow{},
			sequences: map[string]ddl.Sequence{
				"seq1": {Name: "shared_seq", ColumnsUsingSeq: map[string][]string{"t1": {"c1"}, "t2": {"c2"}}},
			},
			tableAssessments: []utils.TableAssessment{
				{SourceTableDef: &utils.SrcTableDetails{Id: "t1", Name: "table1"}},
				{SourceTableDef: &utils.SrcTableDetails{Id: "t2", Name: "table2"}},
			},
			codeSnippets: &[]utils.Snippet{},
			expectedLen:  1,
			expectedLastRow: SchemaReportRow{
				element:           "N/A",
				elementType:       "Sequence",
				sourceTableName:   "N/A", // Falls back to N/A
				sourceName:        "shared_seq",
				sourceDefinition:  "N/A",
				targetName:        "shared_seq",
				targetDefinition:  "CREATE SEQUENCE shared_seq",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "N/A",
				codeChangeEffort:  "Modify",
				codeChangeType:    "Manual",
				codeImpactedFiles: "Unknown",
				codeSnippets:      "Unknown",
			},
		},
		{
			name:        "Sequence linked to an unknown table",
			initialRows: []SchemaReportRow{},
			sequences: map[string]ddl.Sequence{
				"seq1": {Name: "orphan_seq", ColumnsUsingSeq: map[string][]string{"t_unknown": {"c1"}}},
			},
			tableAssessments: []utils.TableAssessment{
				{SourceTableDef: &utils.SrcTableDetails{Id: "t1", Name: "my_table"}},
			},
			codeSnippets: &[]utils.Snippet{},
			expectedLen:  1,
			expectedLastRow: SchemaReportRow{
				elementType:       "Sequence",
				sourceName:        "orphan_seq",
				sourceTableName:   "N/A", // Falls back to N/A
				element:           "N/A",
				sourceDefinition:  "N/A",
				targetName:        "orphan_seq",
				targetDefinition:  "CREATE SEQUENCE orphan_seq",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "N/A",
				codeChangeEffort:  "Modify",
				codeChangeType:    "Manual",
				codeImpactedFiles: "Unknown",
				codeSnippets:      "Unknown",
			},
		},
		{
			name:        "Multiple sequences",
			initialRows: []SchemaReportRow{},
			sequences: map[string]ddl.Sequence{
				"seq1": {Name: "seqA", ColumnsUsingSeq: map[string][]string{"t1": {"c1"}}},
				"seq2": {Name: "seqB", ColumnsUsingSeq: map[string][]string{"t2": {"c2"}}},
			},
			tableAssessments: []utils.TableAssessment{
				{SourceTableDef: &utils.SrcTableDetails{Id: "t1", Name: "tableA"}},
				{SourceTableDef: &utils.SrcTableDetails{Id: "t2", Name: "tableB"}},
			},
			codeSnippets: &[]utils.Snippet{},
			expectedLen:  2,
			expectedLastRow: SchemaReportRow{
				elementType:       "Sequence",
				sourceName:        "seqB",
				sourceTableName:   "tableB",
				element:           "N/A",
				sourceDefinition:  "N/A",
				targetName:        "seqB",
				targetDefinition:  "CREATE SEQUENCE seqB",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "N/A",
				codeChangeEffort:  "Modify",
				codeChangeType:    "Manual",
				codeImpactedFiles: "Unknown",
				codeSnippets:      "Unknown",
			},
		},
		{
			name:        "Code snippets are nil",
			initialRows: []SchemaReportRow{},
			sequences: map[string]ddl.Sequence{
				"seq1": {Name: "my_seq", ColumnsUsingSeq: map[string][]string{"t1": {"c1"}}},
			},
			tableAssessments: []utils.TableAssessment{
				{SourceTableDef: &utils.SrcTableDetails{Id: "t1", Name: "my_table"}},
			},
			codeSnippets: nil,
			expectedLen:  1,
			expectedLastRow: SchemaReportRow{
				elementType:       "Sequence",
				sourceName:        "my_seq",
				sourceTableName:   "my_table",
				codeImpactedFiles: "Unavailable",
				codeSnippets:      "Unavailable",
				element:           "N/A",
				sourceDefinition:  "N/A",
				targetName:        "my_seq",
				targetDefinition:  "CREATE SEQUENCE my_seq",
				dbChangeEffort:    "Automatic",
				dbChanges:         "None",
				dbImpact:          "N/A",
				codeChangeEffort:  "Modify",
				codeChangeType:    "Manual",
			},
		},
		{
			name:             "Empty sequence map",
			initialRows:      []SchemaReportRow{},
			sequences:        map[string]ddl.Sequence{},
			tableAssessments: []utils.TableAssessment{},
			codeSnippets:     nil,
			expectedLen:      0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.initialRows
			populateSequenceInfo(tc.sequences, tc.tableAssessments, tc.codeSnippets, &rows)

			sort.Slice(rows, func(i, j int) bool {
				return rows[i].sourceName < rows[j].sourceName
			})

			assert.Len(t, rows, tc.expectedLen)

			if tc.expectedLen > 0 {
				lastRow := rows[len(rows)-1]
				assert.Equal(t, tc.expectedLastRow, lastRow)
			}
		})
	}
}

func TestConvertToCodeReportRows(t *testing.T) {
	testCases := []struct {
		name        string
		input       *[]utils.Snippet
		expectedLen int
		expectedRow CodeReportRow
	}{
		{
			name: "Snippet with method signature, affected lines, schema change and custom explanation",
			input: &[]utils.Snippet{
				{Id: "s1", SourceMethodSignature: "void oldMethod()", SuggestedMethodSignature: "void newMethod()", NumberOfAffectedLines: 5, SchemaChange: "ALTER TABLE users ADD COLUMN age INT", Explanation: "Adds age column to users"},
			},
			expectedLen: 1,
			expectedRow: CodeReportRow{
				snippetId:           "s1",
				sourceDefinition:    "void oldMethod()",
				suggestedDefinition: "void newMethod()",
				loc:                 5,
				explanation:         "Adds age column to users",
				schemaRelated:       "Yes",
			},
		},
		{
			name: "Snippet with multi-line code and no source method signature/affected lines/schema change/explanation",
			input: &[]utils.Snippet{
				{Id: "s2", SourceCodeSnippet: []string{"line1", "line2"}, SuggestedCodeSnippet: []string{"newLine1", "newLine2"}},
			},
			expectedLen: 1,
			expectedRow: CodeReportRow{
				snippetId:           "s2",
				sourceDefinition:    "line1\nline2",
				suggestedDefinition: "newLine1\nnewLine2",
				loc:                 2, // Falls back to length of snippet
				explanation:         "",
				schemaRelated:       "No",
			},
		},
		{
			name: "Explanation generation from TableName",
			input: &[]utils.Snippet{
				{Id: "s4", TableName: "users", NumberOfAffectedLines: 1},
			},
			expectedLen: 1,
			expectedRow: CodeReportRow{
				snippetId:     "s4",
				explanation:   "changes to users",
				loc:           1,
				schemaRelated: "No",
			},
		},
		{
			name: "Snippet is filtered out due to zero LOC",
			input: &[]utils.Snippet{
				{Id: "s6", NumberOfAffectedLines: 0, SourceCodeSnippet: []string{}},
			},
			expectedLen: 0,
		},
		{
			name:        "Nil input slice",
			input:       nil,
			expectedLen: 0,
		},
		{
			name:        "Empty input slice",
			input:       &[]utils.Snippet{},
			expectedLen: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := convertToCodeReportRows(tc.input)
			assert.Len(t, got, tc.expectedLen)

			if tc.expectedLen > 0 {
				// For simplicity, we check the first row's content.
				// For more complex cases, a loop or more specific checks would be needed.
				firstRow := got[0]
				assert.Equal(t, tc.expectedRow, firstRow)
			}
		})
	}
}

func TestGenerateCodeSummary(t *testing.T) {
	testCases := []struct {
		name             string
		input            *utils.AppCodeAssessmentOutput
		expectedNumRows  int
		expectedSummary  map[string]string
		expectedDataRows [][]string
	}{
		{
			name: "Standard code assessment with multiple snippets",
			input: &utils.AppCodeAssessmentOutput{
				Language:   "Java",
				Framework:  "Spring",
				TotalFiles: 50,
				TotalLoc:   5000,
				CodeSnippets: &[]utils.Snippet{
					{Id: "s1", RelativeFilePath: "file.java", SourceMethodSignature: "old()", SuggestedMethodSignature: "new()", NumberOfAffectedLines: 1, SchemaChange: "Y", Explanation: "test"},
					{Id: "s2", RelativeFilePath: "file2.java", SourceMethodSignature: "old2()", SuggestedMethodSignature: "new2()", NumberOfAffectedLines: 2, SchemaChange: "Y", Explanation: "test2"},
				},
			},
			expectedNumRows: 7, // 4 summary rows + 1 header + 2 data row
			expectedSummary: map[string]string{
				"Language":       "Java",
				"Framework":      "Spring",
				"App Code Files": "50",
				"Lines of code":  "5000",
			},
			expectedDataRows: [][]string{
				{"s1", "file.java", "old()", "new()", "1", "Yes", "test"},
				{"s2", "file2.java", "old2()", "new2()", "2", "Yes", "test2"},
			},
		},
		{
			name: "Code assessment with no snippets",
			input: &utils.AppCodeAssessmentOutput{
				Language:     "Python",
				Framework:    "Django",
				TotalFiles:   20,
				TotalLoc:     2000,
				CodeSnippets: nil,
			},
			expectedNumRows: 5, // 4 summary rows + 1 header
			expectedSummary: map[string]string{
				"Language":       "Python",
				"Framework":      "Django",
				"App Code Files": "20",
				"Lines of code":  "2000",
			},
		},
		{
			name: "Handles special characters that need sanitization",
			input: &utils.AppCodeAssessmentOutput{
				Language:   "Java",
				Framework:  "Spring",
				TotalFiles: 1,
				TotalLoc:   100,
				CodeSnippets: &[]utils.Snippet{
					{Id: "s3", RelativeFilePath: "path/to/file.java", SourceMethodSignature: "old\nmethod()", SuggestedMethodSignature: "new\tmethod()", NumberOfAffectedLines: 1, SchemaChange: "Y", Explanation: "An explanation\nwith a newline"},
				},
			},
			expectedNumRows: 6, // 4 summary rows + 1 header + 1 data row
			expectedSummary: map[string]string{
				"Language":       "Java",
				"Framework":      "Spring",
				"App Code Files": "1",
				"Lines of code":  "100",
			},
			expectedDataRows: [][]string{
				// Note how the newlines and tabs from the input are replaced with spaces.
				{"s3", "path/to/file.java", "old method()", "new method()", "1", "Yes", "An explanation with a newline"},
			},
		},
		{
			name:            "Nil input struct",
			input:           nil,
			expectedNumRows: 0, // or whatever your function returns for nil input
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := generateCodeSummary(tc.input)
			assert.Len(t, got, tc.expectedNumRows)

			if tc.expectedNumRows > 0 {
				expected := [][]string{
					{"Language", tc.expectedSummary["Language"]},
					{"Framework", tc.expectedSummary["Framework"]},
					{"App Code Files", tc.expectedSummary["App Code Files"]},
					{"Lines of code", tc.expectedSummary["Lines of code"]},
					getNonSchemaChangeHeaders(),
				}
				expected = append(expected, tc.expectedDataRows...)
				assert.Equal(t, expected, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

func TestConvertToSchemaReportRows(t *testing.T) {
	tests := []struct {
		name   string
		input  utils.AssessmentOutput
		expect []SchemaReportRow
	}{
		{
			name:   "Empty AssessmentOutput",
			input:  utils.AssessmentOutput{},
			expect: []SchemaReportRow{},
		},
		{
			name: "AppCodeAssessment nil",
			input: utils.AssessmentOutput{
				SchemaAssessment: &utils.SchemaAssessmentOutput{
					TableAssessmentOutput: []utils.TableAssessment{
						{
							SourceTableDef:    &utils.SrcTableDetails{Name: "t1"},
							SpannerTableDef:   &utils.SpTableDetails{Name: "t1"},
							CompatibleCharset: true,
						},
					},
				},
				AppCodeAssessment: nil,
			},
			expect: []SchemaReportRow{
				{
					element:           "t1",
					elementType:       "Table",
					sourceTableName:   "t1",
					sourceName:        "t1",
					sourceDefinition:  "",
					targetName:        "t1",
					targetDefinition:  "N/A",
					dbChangeEffort:    "Automatic",
					dbChanges:         "None",
					dbImpact:          "None",
					codeChangeType:    "None",
					codeChangeEffort:  "None",
					codeImpactedFiles: "None",
					codeSnippets:      "None",
				},
			},
		},
		// Below test checks for non-nil AppCodeAssessment and various elements present
		{
			name: "All element types present",
			input: utils.AssessmentOutput{
				SchemaAssessment: &utils.SchemaAssessmentOutput{
					TableAssessmentOutput: []utils.TableAssessment{
						{
							SourceTableDef: &utils.SrcTableDetails{
								Name: "my_table",
								CheckConstraints: map[string]schema.CheckConstraint{
									"cc1": {Name: "check1", Expr: "val > 0"},
								},
								SourceForeignKey: map[string]utils.SourceForeignKey{
									"fk1": {Definition: schema.ForeignKey{Id: "fk1", Name: "fk1"}, Ddl: "CONSTRAINT fk1 FOREIGN KEY (col) REFERENCES other(id)"},
								},
							},
							SpannerTableDef: &utils.SpTableDetails{
								Name: "my_table",
								CheckConstraints: map[string]ddl.CheckConstraint{
									"cc1": {Name: "check1", Expr: "val > 0"},
								},
								SpannerForeignKey: map[string]utils.SpannerForeignKey{
									"fk1": {Definition: ddl.Foreignkey{Id: "fk1", Name: "fk1"}, Ddl: "CONSTRAINT fk1 FOREIGN KEY (col) REFERENCES other(id)"},
								},
							},
							Columns: []utils.ColumnAssessment{
								{
									SourceColDef:  &utils.SrcColumnDetails{Name: "col1", TableName: "my_table", Datatype: "INT"},
									SpannerColDef: &utils.SpColumnDetails{Name: "col1", TableName: "my_table", Datatype: "INT64"},
								},
								{
									SourceColDef:  &utils.SrcColumnDetails{Name: "col2", TableName: "my_table", Datatype: "VARCHAR"},
									SpannerColDef: &utils.SpColumnDetails{Name: "col2", TableName: "my_table", Datatype: "STRING"},
								},
							},
							SourceIndexDef: []utils.SrcIndexDetails{
								{Id: "idx1", Name: "idx1", Ddl: "CREATE INDEX idx1 ON my_table(col1)"},
							},
							SpannerIndexDef: []utils.SpIndexDetails{
								{Id: "idx1", Name: "idx1", Ddl: "CREATE INDEX idx1 ON my_table(col1)"},
							},
							CompatibleCharset: true,
						},
					},
					StoredProcedureAssessmentOutput: map[string]utils.StoredProcedureAssessment{
						"sp1": {Name: "proc1", Definition: "BEGIN...END"},
					},
					TriggerAssessmentOutput: map[string]utils.TriggerAssessment{
						"tr1": {Name: "trg1", TargetTable: "my_table", Operation: "INSERT"},
					},
					FunctionAssessmentOutput: map[string]utils.FunctionAssessment{
						"fn1": {Name: "func1", Definition: "RETURN 1"},
					},
					ViewAssessmentOutput: map[string]utils.ViewAssessment{
						"v1": {SrcName: "my_view", SpName: "my_view_sp", SrcViewType: "SELECT * FROM my_table"},
					},
					SpSequences: map[string]ddl.Sequence{
						"seq1": {Name: "my_seq", ColumnsUsingSeq: map[string][]string{"my_table": {"col1"}}},
					},
				},
				AppCodeAssessment: &utils.AppCodeAssessmentOutput{
					CodeSnippets: &[]utils.Snippet{
						{Id: "s1", TableName: "my_table", ColumnName: "col1", RelativeFilePath: "file1.go"},
					},
				},
			},
			expect: []SchemaReportRow{
				// Table
				{
					element:           "my_table",
					elementType:       "Table",
					sourceTableName:   "my_table",
					sourceName:        "my_table",
					sourceDefinition:  "",
					targetName:        "my_table",
					targetDefinition:  "N/A",
					dbChangeEffort:    "Automatic",
					dbChanges:         "None",
					dbImpact:          "None",
					codeChangeType:    "None",
					codeChangeEffort:  "None",
					codeImpactedFiles: "None",
					codeSnippets:      "None",
				},
				// Column 1
				{
					element:           "my_table.col1",
					elementType:       "Column",
					sourceTableName:   "my_table",
					sourceName:        "col1",
					sourceDefinition:  "INT",
					targetName:        "my_table.col1",
					targetDefinition:  "col1 INT64",
					dbChangeEffort:    "Automatic",
					dbChanges:         "type",
					dbImpact:          "None",
					codeChangeType:    "Suggested",
					codeChangeEffort:  "Small",
					codeImpactedFiles: "file1.go",
					codeSnippets:      "s1",
					actionItems:       &[]string{},
				},
				// Column 2
				{
					element:           "my_table.col2",
					elementType:       "Column",
					sourceTableName:   "my_table",
					sourceName:        "col2",
					sourceDefinition:  "VARCHAR",
					targetName:        "my_table.col2",
					targetDefinition:  "col2 STRING(0)",
					dbChangeEffort:    "Automatic",
					dbChanges:         "type",
					dbImpact:          "None",
					codeChangeType:    "None",
					codeChangeEffort:  "None",
					codeImpactedFiles: "None",
					codeSnippets:      "",
					actionItems:       &[]string{},
				},
				// Check Constraint
				{
					element:           "my_table.check1",
					elementType:       "Check Constraint",
					sourceTableName:   "my_table",
					sourceName:        "check1",
					sourceDefinition:  "val > 0",
					targetName:        "my_table.check1",
					targetDefinition:  "val > 0",
					dbChangeEffort:    "Automatic",
					dbChanges:         "None",
					dbImpact:          "None",
					codeChangeType:    "None",
					codeChangeEffort:  "None",
					codeImpactedFiles: "None",
					codeSnippets:      "None",
				},
				// Foreign Key
				{
					element:           "my_table.fk1",
					elementType:       "Foreign Key",
					sourceTableName:   "my_table",
					sourceName:        "fk1",
					sourceDefinition:  "CONSTRAINT fk1 FOREIGN KEY (col) REFERENCES other(id)",
					targetName:        "my_table.fk1",
					targetDefinition:  "CONSTRAINT fk1 FOREIGN KEY (col) REFERENCES other(id)",
					dbChangeEffort:    "Automatic",
					dbChanges:         "None",
					dbImpact:          "None",
					codeChangeType:    "None",
					codeChangeEffort:  "None",
					codeImpactedFiles: "None",
					codeSnippets:      "None",
				},
				// Index
				{
					element:           "my_table.idx1",
					elementType:       "Index",
					sourceTableName:   "my_table",
					sourceName:        "idx1",
					sourceDefinition:  "CREATE INDEX idx1 ON my_table(col1)",
					targetName:        "my_table.idx1",
					targetDefinition:  "CREATE INDEX idx1 ON my_table(col1)",
					dbChangeEffort:    "Automatic",
					dbChanges:         "None",
					dbImpact:          "None",
					codeChangeType:    "None",
					codeChangeEffort:  "None",
					codeImpactedFiles: "None",
					codeSnippets:      "None",
				},
				// Stored Procedure
				{
					element:           "proc1",
					elementType:       "Stored Procedure",
					sourceName:        "proc1",
					sourceTableName:   "N/A",
					sourceDefinition:  "BEGIN...END",
					targetName:        "Not supported",
					targetDefinition:  "N/A",
					dbChangeEffort:    "Not Supported",
					dbChanges:         "Drop",
					dbImpact:          "Less Compute",
					codeChangeEffort:  "Rewrite",
					codeChangeType:    "Manual",
					codeImpactedFiles: "Unknown",
					codeSnippets:      "",
					actionItems:       &[]string{"Rewrite in application code"},
				},
				// Trigger
				{
					element:           "trg1",
					elementType:       "Trigger",
					sourceName:        "trg1",
					sourceTableName:   "my_table",
					sourceDefinition:  "INSERT",
					targetName:        "Not supported",
					targetDefinition:  "N/A",
					dbChangeEffort:    "Not Supported",
					dbChanges:         "Drop",
					dbImpact:          "Less Compute",
					codeChangeEffort:  "Rewrite",
					codeChangeType:    "Manual",
					codeImpactedFiles: "Unknown",
					codeSnippets:      "",
					actionItems:       &[]string{"Rewrite in application code"},
				},
				// Function
				{
					element:           "func1",
					elementType:       "Function",
					sourceName:        "func1",
					sourceTableName:   "N/A",
					sourceDefinition:  "RETURN 1",
					targetName:        "Not supported",
					targetDefinition:  "N/A",
					dbChangeEffort:    "Not Supported",
					dbChanges:         "Drop",
					dbImpact:          "Less Compute",
					codeChangeEffort:  "Rewrite",
					codeChangeType:    "Manual",
					codeImpactedFiles: "Unknown",
					codeSnippets:      "",
					actionItems:       &[]string{"Rewrite in application code"},
				},
				// View
				{
					element:           "my_view",
					elementType:       "View",
					sourceName:        "my_view",
					sourceTableName:   "N/A",
					sourceDefinition:  "SELECT * FROM my_table",
					targetName:        "my_view_sp",
					targetDefinition:  "Unknown",
					dbChangeEffort:    "Small",
					dbChanges:         "Unknown",
					dbImpact:          "None",
					codeChangeType:    "Manual",
					codeChangeEffort:  "Unknown",
					codeImpactedFiles: "Unknown",
					codeSnippets:      "",
					actionItems:       &[]string{"Create view manually"},
				},
				// Sequence
				{
					element:           "N/A",
					elementType:       "Sequence",
					sourceTableName:   "N/A", // Falls back to N/A
					sourceName:        "my_seq",
					sourceDefinition:  "N/A",
					targetName:        "my_seq",
					targetDefinition:  "CREATE SEQUENCE my_seq",
					dbChangeEffort:    "Automatic",
					dbChanges:         "None",
					dbImpact:          "N/A",
					codeChangeEffort:  "Modify",
					codeChangeType:    "Manual",
					codeImpactedFiles: "Unknown",
					codeSnippets:      "Unknown",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := convertToSchemaReportRows(tc.input)

			sort.Slice(got, func(i, j int) bool {
				if got[i].elementType != got[j].elementType {
					return got[i].elementType < got[j].elementType
				}
				return got[i].element < got[j].element
			})

			sort.Slice(tc.expect, func(i, j int) bool {
				if tc.expect[i].elementType != tc.expect[j].elementType {
					return tc.expect[i].elementType < tc.expect[j].elementType
				}
				return tc.expect[i].element < tc.expect[j].element
			})

			assert.Equal(t, tc.expect, got)
			assert.Equal(t, len(tc.expect), len(got))
		})
	}
}

func TestGenerateSchemaReport(t *testing.T) {
	header := getSchemaHeaders()
	testCases := []struct {
		name            string
		input           utils.AssessmentOutput
		expectedRecords [][]string
	}{
		{
			name:            "Nil schema assessment",
			input:           utils.AssessmentOutput{SchemaAssessment: nil},
			expectedRecords: [][]string{header},
		},
		{
			name: "Empty schema assessment",
			input: utils.AssessmentOutput{
				SchemaAssessment: &utils.SchemaAssessmentOutput{TableAssessmentOutput: []utils.TableAssessment{}},
			},
			expectedRecords: [][]string{header},
		},
		{
			name: "Standard assessment with one table",
			input: utils.AssessmentOutput{
				SchemaAssessment: &utils.SchemaAssessmentOutput{
					TableAssessmentOutput: []utils.TableAssessment{
						{
							SourceTableDef:  &utils.SrcTableDetails{Name: "users"},
							SpannerTableDef: &utils.SpTableDetails{Name: "users_sp"},
						},
					},
				},
				AppCodeAssessment: nil,
			},
			expectedRecords: [][]string{
				header,
				{"Table", "users", "users", "", "users_sp", "N/A", "Automatic", "charset", "None", "Unavailable", "Unavailable", "Unavailable", "None"},
			},
		},
		{
			name: "Assessment with column, action items, and special characters",
			input: utils.AssessmentOutput{
				SchemaAssessment: &utils.SchemaAssessmentOutput{
					TableAssessmentOutput: []utils.TableAssessment{
						{
							SourceTableDef:  &utils.SrcTableDetails{Name: "table\twith\ttabs"},
							SpannerTableDef: &utils.SpTableDetails{Name: "table_with_tabs"},
							Columns: []utils.ColumnAssessment{
								{
									SourceColDef: &utils.SrcColumnDetails{
										Name:                   "col\n1",
										TableName:              "table\twith\ttabs",
										IsOnUpdateTimestampSet: true, // This triggers an action item.
									},
									SpannerColDef: &utils.SpColumnDetails{
										Name:      "col_1",
										TableName: "table_with_tabs",
									},
								},
							},
						},
					},
				},
			},
			expectedRecords: [][]string{
				header,
				{"Table", "table with tabs", "table with tabs", "", "table_with_tabs", "N/A", "Automatic", "charset", "None", "Unavailable", "Unavailable", "Unavailable", "None"},
				{"Column", "table with tabs", "col 1", " ON UPDATE CURRENT_TIMESTAMP", "table_with_tabs.col_1", "col_1 ", "None", "type,feature", "None", "Unavailable", "Unavailable", "Unavailable", "Update queries to include PENDING_COMMIT_TIMESTAMP"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := generateSchemaReport(tc.input)
			// Assert the header is correct.
			assert.Equal(t, tc.expectedRecords[0], got[0], "Header should match")

			// Sort the data rows (all rows after the header) to avoid flakiness due to order.
			gotDataRows := got[1:]
			expectedDataRows := tc.expectedRecords[1:]

			sort.Slice(gotDataRows, func(i, j int) bool {
				// Sort by Element Type, then Source Name
				if gotDataRows[i][0] != gotDataRows[j][0] {
					return gotDataRows[i][0] < gotDataRows[j][0]
				}
				return gotDataRows[i][2] < gotDataRows[j][2]
			})

			sort.Slice(expectedDataRows, func(i, j int) bool {
				if expectedDataRows[i][0] != expectedDataRows[j][0] {
					return expectedDataRows[i][0] < expectedDataRows[j][0]
				}
				return expectedDataRows[i][2] < expectedDataRows[j][2]
			})

			// Assert that the sorted data rows are equal.
			assert.Equal(t, expectedDataRows, gotDataRows, "Data rows should match after sorting")
		})
	}
}

func TestWriteRawSnippets(t *testing.T) {
	snippet1 := utils.Snippet{
		Id:                    "s1",
		TableName:             "users",
		RelativeFilePath:      "path/to/file.java",
		NumberOfAffectedLines: 5,
	}

	testCases := []struct {
		name            string
		snippets        []utils.Snippet
		expectFile      bool
		expectedContent string
		setupFailure    func(dir string) // Optional function to set up a failure condition
	}{
		{
			name:            "Standard case with a single snippet",
			snippets:        []utils.Snippet{snippet1},
			expectFile:      true,
			expectedContent: `[{"Id":"s1","TableName":"users","ColumnName":"","SchemaChange":"","NumberOfAffectedLines":5,"Complexity":"","SourceCodeSnippet":null,"SuggestedCodeSnippet":null,"SourceMethodSignature":"","SuggestedMethodSignature":"","Explanation":"","RelativeFilePath":"path/to/file.java","FilePath":"","IsDao":false}]` + "\n",
		},
		{
			name:       "Empty snippets slice writes an empty JSON array",
			snippets:   []utils.Snippet{},
			expectFile: true,
			// Note: json.Encoder adds a newline character at the end.
			expectedContent: "[]\n",
		},
		{
			name:            "Nil snippets slice writes JSON null",
			snippets:        nil,
			expectFile:      true,
			expectedContent: "null\n",
		},
		{
			name:       "File creation failure due to permissions",
			snippets:   []utils.Snippet{snippet1},
			expectFile: false,
			setupFailure: func(dir string) {
				// Make the directory read-only to cause os.Create to fail.
				err := os.Chmod(dir, 0555)
				assert.NoError(t, err)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()

			if tc.setupFailure != nil {
				tc.setupFailure(tempDir)
			}

			filePath := filepath.Join(tempDir, "raw_snippets.txt")
			writeRawSnippets(tempDir+"/", tc.snippets)

			if !tc.expectFile {
				assert.NoFileExists(t, filePath)
				return
			}

			assert.FileExists(t, filePath)
			content, err := os.ReadFile(filePath)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedContent, string(content))
		})
	}
}

func TestDumpCsvReport(t *testing.T) {
	testCases := []struct {
		name            string
		records         [][]string
		expectFile      bool
		expectedContent string
		setupFailure    func(filePath string) // Optional function to set up a failure condition
	}{
		{
			name: "Standard case with multiple rows",
			records: [][]string{
				{"Header1", "Header2"},
				{"Value1", "Value2"},
				{"Value3", "Value4"},
			},
			expectFile: true,
			// Note: The csv writer adds a CRLF (\r\n) after each line.
			expectedContent: "Header1\tHeader2\r\nValue1\tValue2\r\nValue3\tValue4\r\n",
		},
		{
			name: "Handles records with special characters",
			records: [][]string{
				{"Field A", "Field with\na newline"},
				{"Field with a\ttab", "Field, with, commas and \"quotes\""},
			},
			expectFile:      true,
			expectedContent: "Field A\t\"Field with\r\na newline\"\r\n\"Field with a\ttab\"\t\"Field, with, commas and \"\"quotes\"\"\"\r\n",
		},
		{
			name:            "Empty records slice creates an empty file",
			records:         [][]string{},
			expectFile:      true,
			expectedContent: "",
		},
		{
			name:            "Nil records slice creates an empty file",
			records:         nil,
			expectFile:      true,
			expectedContent: "",
		},
		{
			name:       "File creation failure due to permissions",
			records:    [][]string{{"data"}},
			expectFile: false,
			setupFailure: func(filePath string) {
				// Make the parent directory read-only to cause os.Create to fail.
				dir := filepath.Dir(filePath)
				err := os.Chmod(dir, 0555) // Read and execute permissions only
				assert.NoError(t, err)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			filePath := filepath.Join(tempDir, "report.csv")

			if tc.setupFailure != nil {
				tc.setupFailure(filePath)
			}

			dumpCsvReport(filePath, tc.records)
			if !tc.expectFile {
				assert.NoFileExists(t, filePath)
				// Restore permissions so the temp directory can be cleaned up.
				if tc.setupFailure != nil {
					os.Chmod(filepath.Dir(filePath), 0755)
				}
				return
			}

			assert.FileExists(t, filePath)
			content, err := os.ReadFile(filePath)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedContent, string(content))
		})
	}
}

func TestGenerateReport(t *testing.T) {
	// Helper function to set up a temporary directory for each test case.
	setup := func(t *testing.T) (string, func()) {
		t.Helper()
		tempDir := t.TempDir()
		originalWd, err := os.Getwd()
		assert.NoError(t, err)
		err = os.Chdir(tempDir)
		assert.NoError(t, err)
		return tempDir, func() {
			os.Chdir(originalWd)
		}
	}
	t.Run("Full report with schema and code", func(t *testing.T) {
		_, cleanup := setup(t)
		defer cleanup()

		dbName := "full_report_db"
		snippets := []utils.Snippet{
			{Id: "s1", RelativeFilePath: "file.java", NumberOfAffectedLines: 1},
		}
		assessmentOutput := utils.AssessmentOutput{
			SchemaAssessment: &utils.SchemaAssessmentOutput{
				TableAssessmentOutput: []utils.TableAssessment{
					{SourceTableDef: &utils.SrcTableDetails{Name: "t1"}, SpannerTableDef: &utils.SpTableDetails{Name: "t1"}},
				},
			},
			AppCodeAssessment: &utils.AppCodeAssessmentOutput{
				TotalFiles: 1, CodeSnippets: &snippets,
			},
		}

		GenerateReport(dbName, assessmentOutput)

		// Assert directory and files were created
		reportDir := "assessment_" + dbName
		assert.DirExists(t, reportDir)

		schemaFile := filepath.Join(reportDir, "schema.csv")
		assert.FileExists(t, schemaFile)

		codeFile := filepath.Join(reportDir, "code_changes.csv")
		assert.FileExists(t, codeFile)

		rawFile := filepath.Join(reportDir, "raw_snippets.txt")
		assert.FileExists(t, rawFile)

		schemaContent, err := os.ReadFile(schemaFile)
		assert.NoError(t, err)
		goldenSchema := "Element Type\tSource Table Name\tSource Name\tSource Definition\tTarget Name\tTarget Definition\tDB Change Effort\tDB Changes\tDB Impact\tCode Change Type\tImpacted Files\tCode Snippet References\tAction Items\r\n" +
			"Table\tt1\tt1\t\tt1\tN/A\tAutomatic\tcharset\tNone\tNone\tNone\tNone\tNone\r\n"
		assert.Equal(t, goldenSchema, string(schemaContent))
	})

	t.Run("Schema-only report (nil AppCodeAssessment)", func(t *testing.T) {
		_, cleanup := setup(t)
		defer cleanup()

		dbName := "schema_only_db"
		assessmentOutput := utils.AssessmentOutput{
			SchemaAssessment: &utils.SchemaAssessmentOutput{
				TableAssessmentOutput: []utils.TableAssessment{},
			},
			AppCodeAssessment: nil, // No code assessment
		}

		GenerateReport(dbName, assessmentOutput)

		reportDir := "assessment_" + dbName
		assert.DirExists(t, reportDir)

		schemaFile := filepath.Join(reportDir, "schema.csv")
		assert.FileExists(t, schemaFile)

		codeFile := filepath.Join(reportDir, "code_changes.csv")
		assert.NoFileExists(t, codeFile)

		rawFile := filepath.Join(reportDir, "raw_snippets.txt")
		assert.NoFileExists(t, rawFile)
	})

	t.Run("Schema report with code assessment but zero files", func(t *testing.T) {
		_, cleanup := setup(t)
		defer cleanup()

		dbName := "zero_files_db"
		assessmentOutput := utils.AssessmentOutput{
			SchemaAssessment: &utils.SchemaAssessmentOutput{
				TableAssessmentOutput: []utils.TableAssessment{},
			},
			AppCodeAssessment: &utils.AppCodeAssessmentOutput{TotalFiles: 0}, // Zero files
		}

		GenerateReport(dbName, assessmentOutput)

		reportDir := "assessment_" + dbName
		assert.DirExists(t, reportDir)

		schemaFile := filepath.Join(reportDir, "schema.csv")
		assert.FileExists(t, schemaFile)

		codeFile := filepath.Join(reportDir, "code_changes.csv")
		assert.NoFileExists(t, codeFile)

		rawFile := filepath.Join(reportDir, "raw_snippets.txt")
		assert.NoFileExists(t, rawFile)
	})

	t.Run("Schema report with code assessment but nil snippets", func(t *testing.T) {
		_, cleanup := setup(t)
		defer cleanup()

		dbName := "nil_snippets_db"
		assessmentOutput := utils.AssessmentOutput{
			SchemaAssessment: &utils.SchemaAssessmentOutput{
				TableAssessmentOutput: []utils.TableAssessment{},
			},
			AppCodeAssessment: &utils.AppCodeAssessmentOutput{
				TotalFiles:   1,
				CodeSnippets: nil, // nil snippets
			},
		}

		GenerateReport(dbName, assessmentOutput)

		reportDir := "assessment_" + dbName
		assert.DirExists(t, reportDir)

		schemaFile := filepath.Join(reportDir, "schema.csv")
		assert.FileExists(t, schemaFile)

		codeFile := filepath.Join(reportDir, "code_changes.csv")
		assert.FileExists(t, codeFile)

		rawFile := filepath.Join(reportDir, "raw_snippets.txt")
		assert.NoFileExists(t, rawFile) // raw snippets file should not be created
	})

	t.Run("Schema-only report (nil SchemaAssessment)", func(t *testing.T) {
		_, cleanup := setup(t)
		defer cleanup()

		dbName := "nil_schema_db"
		assessmentOutput := utils.AssessmentOutput{
			SchemaAssessment:  nil,
			AppCodeAssessment: nil,
		}

		GenerateReport(dbName, assessmentOutput)

		reportDir := "assessment_" + dbName
		assert.DirExists(t, reportDir)

		schemaFile := filepath.Join(reportDir, "schema.csv")
		assert.FileExists(t, schemaFile)

		// Check that schema file only contains headers
		content, err := os.ReadFile(schemaFile)
		assert.NoError(t, err)
		expectedHeader := "Element Type\tSource Table Name\tSource Name\tSource Definition\tTarget Name\tTarget Definition\tDB Change Effort\tDB Changes\tDB Impact\tCode Change Type\tImpacted Files\tCode Snippet References\tAction Items\r\n"
		assert.Equal(t, expectedHeader, string(content))
	})

	t.Run("Directory creation fails", func(t *testing.T) {
		_, cleanup := setup(t)
		defer cleanup()

		dbName := "dir_fail_db"
		reportDir := "assessment_" + dbName

		// Create a file with the same name as the directory to cause Mkdir to fail
		f, err := os.Create(reportDir)
		assert.NoError(t, err)
		f.Close()

		assessmentOutput := utils.AssessmentOutput{SchemaAssessment: &utils.SchemaAssessmentOutput{}}

		// This should log a warning and return, not panic.
		GenerateReport(dbName, assessmentOutput)

		// Verify no other files were created inside a non-existent directory
		schemaFile := filepath.Join(reportDir, "schema.csv")
		assert.NoFileExists(t, schemaFile)
	})
}
