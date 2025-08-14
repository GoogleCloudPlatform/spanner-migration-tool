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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestReadSessionFile(t *testing.T) {
	createdExpectedConv := func() *internal.Conv {
		expectedConv := internal.MakeConv()
		expectedConv.SpSchema = map[string]ddl.CreateTable{
			"t1": {
				Name:          "numbers",
				ColIds:        []string{"c1", "c2"},
				ShardIdColumn: "c1",
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {
						Name:    "id",
						NotNull: true,
						Comment: "From: id int(10)",
						Id:      "c2",
					},
					"c2": {
						Name:    "value",
						NotNull: false,
						Id:      "c2",
					},
				},
				PrimaryKeys: []ddl.IndexKey{
					{
						ColId: "c1",
						Order: 1,
					},
				},
				Comment: "Spanner schema for source table numbers",
				Id:      "t1",
			},
		}
		expectedConv.SrcSchema = map[string]schema.Table{
			"t1": {
				Name:   "numbers",
				Schema: "default",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]schema.Column{
					"c1": {
						Name: "id",
						Type: schema.Type{
							Name: "int",
							Mods: []int64{10},
						},
						NotNull: true,
						Id:      "c1",
					},
					"c2": {
						Name: "value",
						Type: schema.Type{
							Name: "int",
							Mods: []int64{10},
						},
						NotNull: true,
						Id:      "c2",
					},
				},
				PrimaryKeys: []schema.Key{
					{
						ColId: "c1",
						Desc:  false,
						Order: 1,
					},
				},
				Id: "t1",
			},
		}
		expectedConv.SchemaIssues = map[string]internal.TableIssues{
			"t1": {
				ColumnLevelIssues: map[string][]internal.SchemaIssue{
					"c1": {14},
				},
			},
		}
		return expectedConv
	}
	expectedConvWithSequences := createdExpectedConv()
	expectedConvWithSequences.SpSequences = map[string]ddl.Sequence{
		"s1": {
			Name:         "Seq",
			Id:           "s1",
			SequenceKind: "BIT REVERSED POSITIVE",
		},
	}
	testCases := []struct {
		name         string
		filePath     string
		expectedConv *internal.Conv
		expectError  bool
	}{
		{
			name:         "test basic session file",
			filePath:     filepath.Join("..", "test_data", "basic_session_file_test.json"),
			expectedConv: expectedConvWithSequences,
			expectError:  false,
		},
		{
			name:         "test session file without sequences",
			filePath:     filepath.Join("..", "test_data", "basic_sessions_file_wo_sequences_test.json"),
			expectedConv: createdExpectedConv(),
			expectError:  false,
		},
	}
	for _, tc := range testCases {
		conv := internal.MakeConv()
		err := ReadSessionFile(conv, tc.filePath)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		assert.Equal(t, &tc.expectedConv, &conv, tc.name)
	}
}

func TestWriteOverridesFile(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	tests := []struct {
		name     string
		conv     *internal.Conv
		fileName string
		expected string
	}{
		{
			name: "empty overrides",
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {Name: "users", ColDefs: map[string]schema.Column{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {Name: "users", ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"users": {
						Name: "users",
						Cols: map[string]string{
							"id":   "id",
							"name": "name",
						},
					},
				},
			},
			fileName: tempDir + "/test_overrides.json",
			expected: `{
  "renamedTables": {},
  "renamedColumns": {}
}`,
		},
		{
			name: "with table and column renames",
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {Name: "user_table", ColDefs: map[string]schema.Column{
						"c1": {Name: "user_id"},
						"c2": {Name: "user_name"},
					}},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {Name: "Users", ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"user_table": {
						Name: "Users",
						Cols: map[string]string{
							"user_id":   "id",
							"user_name": "name",
						},
					},
				},
			},
			fileName: tempDir + "/test_overrides_with_renames.json",
			expected: `{
  "renamedTables": {
    "user_table": "Users"
  },
  "renamedColumns": {
    "user_table": {
      "user_id": "id",
      "user_name": "name"
    }
  }
}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary file for output
			outFile, err := os.CreateTemp(tempDir, "test_output")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(outFile.Name())
			outFile.Close()

			// Call WriteOverridesFile
			WriteOverridesFile(tt.conv, tt.fileName, outFile)

			// Read the generated file
			content, err := os.ReadFile(tt.fileName)
			if err != nil {
				t.Fatalf("Failed to read generated file: %v", err)
			}

			// Unmarshal both the generated file and the expected string to OverridesFile and compare
			var gotOverrides, expectedOverrides internal.OverridesFile
			if err := json.Unmarshal(content, &gotOverrides); err != nil {
				t.Fatalf("Failed to unmarshal generated file: %v", err)
			}
			if err := json.Unmarshal([]byte(tt.expected), &expectedOverrides); err != nil {
				t.Fatalf("Failed to unmarshal expected JSON: %v", err)
			}
			if !assert.Equal(t, expectedOverrides, gotOverrides, "WriteOverridesFile() output mismatch") {
				t.Errorf("WriteOverridesFile() output = %+v, want %+v", gotOverrides, expectedOverrides)
			}

			// Verify file exists
			if _, err := os.Stat(tt.fileName); os.IsNotExist(err) {
				t.Errorf("WriteOverridesFile() did not create file %s", tt.fileName)
			}
		})
	}
}
