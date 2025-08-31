/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package assessment

import (
	"reflect"
	"testing"

	. "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop() // Set to a no-op logger during tests
}
func TestParseStringArrayInterface(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  []string
	}{
		{
			name:  "string array",
			input: []string{"a", "b", "c"},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "single string",
			input: "abc",
			want:  []string{"abc"},
		},
		{
			name:  "interface array of strings",
			input: []any{"a", "b", "c"},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "interface array of mixed types",
			input: []any{"a", 1, "c", 2.0},
			want:  []string{"a", "c"},
		},
		{
			name:  "empty interface array",
			input: []any{},
			want:  []string{},
		},
		{
			name:  "nil input",
			input: nil,
			want:  []string{},
		},
		{
			name:  "interface array with nil",
			input: []any{"a", nil, "c"},
			want:  []string{"a", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseStringArrayInterface(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseStringArrayInterface() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseAnyToString(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  string
	}{
		{
			name:  "string",
			input: "abc",
			want:  "abc",
		},
		{
			name:  "integer",
			input: 123,
			want:  "123",
		},
		{
			name:  "float",
			input: 3.14,
			want:  "3.14",
		},
		{
			name:  "boolean",
			input: true,
			want:  "true",
		},
		{
			name:  "nil",
			input: nil,
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseAnyToString(tt.input)
			if got != tt.want {
				t.Errorf("parseAnyToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseAnyToInteger(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  int
	}{
		{
			name:  "valid integer string",
			input: "123",
			want:  123,
		},
		{
			name:  "integer",
			input: 123,
			want:  123,
		},
		{
			name:  "float string",
			input: "3.14",
			want:  0,
		},
		{
			name:  "string",
			input: "abc",
			want:  0,
		},
		{
			name:  "float",
			input: 3.14,
			want:  0,
		},
		{
			name:  "nil",
			input: nil,
			want:  0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseAnyToInteger(tt.input)
			if got != tt.want {
				t.Errorf("parseAnyToInteger() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetRelativeFilePath(t *testing.T) {
	tests := []struct {
		name        string
		projectPath string
		filePath    string
		want        string
	}{
		{
			name:        "file in project path",
			projectPath: "/home/user/project",
			filePath:    "/home/user/project/src/main.go",
			want:        "/src/main.go",
		},
		{
			name:        "file not in project path",
			projectPath: "/home/user/project",
			filePath:    "/tmp/main.go",
			want:        "/tmp/main.go",
		},
		{
			name:        "project path is prefix of file path",
			projectPath: "/home/user",
			filePath:    "/home/user/project/main.go",
			want:        "/project/main.go",
		},
		{
			name:        "empty project path",
			projectPath: "",
			filePath:    "/home/user/project/main.go",
			want:        "/home/user/project/main.go",
		},
		{
			name:        "empty file path",
			projectPath: "/home/user/project",
			filePath:    "",
			want:        "",
		},
		{
			name:        "project path and file path are same",
			projectPath: "/home/user/project",
			filePath:    "/home/user/project",
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetRelativeFilePath(tt.projectPath, tt.filePath)
			if got != tt.want {
				t.Errorf("getRelativeFilePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsCodeEqual(t *testing.T) {
	tests := []struct {
		name          string
		sourceCode    *[]string
		suggestedCode *[]string
		want          bool
	}{
		{
			name:          "both nil",
			sourceCode:    nil,
			suggestedCode: nil,
			want:          true,
		},
		{
			name:          "source nil",
			sourceCode:    nil,
			suggestedCode: &[]string{"a"},
			want:          false,
		},
		{
			name:          "suggested nil",
			sourceCode:    &[]string{"a"},
			suggestedCode: nil,
			want:          false,
		},
		{
			name:          "both empty",
			sourceCode:    &[]string{},
			suggestedCode: &[]string{},
			want:          true,
		},
		{
			name:          "equal single line",
			sourceCode:    &[]string{"abc"},
			suggestedCode: &[]string{"abc"},
			want:          true,
		},
		{
			name:          "equal multi line",
			sourceCode:    &[]string{"abc", "def"},
			suggestedCode: &[]string{"abc", "def"},
			want:          true,
		},
		{
			name:          "different single line",
			sourceCode:    &[]string{"abc"},
			suggestedCode: &[]string{"def"},
			want:          false,
		},
		{
			name:          "different multi line",
			sourceCode:    &[]string{"abc", "def"},
			suggestedCode: &[]string{"abc", "ghi"},
			want:          false,
		},
		{
			name:          "equal with leading/trailing space",
			sourceCode:    &[]string{"  abc  "},
			suggestedCode: &[]string{"abc"},
			want:          true,
		},
		{
			name:          "equal with internal spaces",
			sourceCode:    &[]string{"ab c"},
			suggestedCode: &[]string{"ab c"},
			want:          true,
		},
		{
			name:          "different with different spacing",
			sourceCode:    &[]string{"ab c"},
			suggestedCode: &[]string{"abc"},
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsCodeEqual(tt.sourceCode, tt.suggestedCode)
			if got != tt.want {
				t.Errorf("isCodeEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseCodeImpact(t *testing.T) {
	projectPath := "/home/user/project"
	filePath := "/home/user/project/src/main.go"
	relativeFilePath := "/src/main.go"

	tests := []struct {
		name    string
		input   map[string]any
		want    *Snippet
		wantErr bool
	}{
		{
			name: "valid input",
			input: map[string]any{
				"original_method_signature": "func(a int) int",
				"new_method_signature":      "func(a int, b string) int",
				"code_sample":               []any{"line1", "line2"},
				"suggested_change":          []any{"line3", "line4"},
				"number_of_affected_lines":  "10",
				"complexity":                "high",
				"description":               "Add parameter b",
			},
			want: &Snippet{
				SourceMethodSignature:    "func(a int) int",
				SuggestedMethodSignature: "func(a int, b string) int",
				SourceCodeSnippet:        []string{"line1", "line2"},
				SuggestedCodeSnippet:     []string{"line3", "line4"},
				NumberOfAffectedLines:    10,
				Complexity:               "high",
				Explanation:              "Add parameter b",
				RelativeFilePath:         relativeFilePath,
				FilePath:                 filePath,
				IsDao:                    false,
			},
			wantErr: false,
		},
		{
			name: "invalid number_of_affected_lines",
			input: map[string]any{
				"original_method_signature": "func(a int) int",
				"new_method_signature":      "func(a int, b string) int",
				"code_sample":               []any{"line1", "line2"},
				"suggested_change":          []any{"line3", "line4"},
				"number_of_affected_lines":  "abc",
				"complexity":                "high",
				"description":               "Add parameter b",
			},
			want: &Snippet{
				SourceMethodSignature:    "func(a int) int",
				SuggestedMethodSignature: "func(a int, b string) int",
				SourceCodeSnippet:        []string{"line1", "line2"},
				SuggestedCodeSnippet:     []string{"line3", "line4"},
				NumberOfAffectedLines:    0,
				Complexity:               "high",
				Explanation:              "Add parameter b",
				RelativeFilePath:         relativeFilePath,
				FilePath:                 filePath,
				IsDao:                    false,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCodeImpact(tt.input, projectPath, filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCodeImpact() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseCodeImpact() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseNonDaoFileChanges(t *testing.T) {
	projectPath := "/home/user/project"
	filePath := "/home/user/project/src/main.go"
	fileIndex := 0

	tests := []struct {
		name         string
		input        string
		wantSnippets []Snippet
		wantWarnings []string
		wantErr      bool
	}{
		{
			name: "valid input with one code impact",
			input: `{
				"file_modifications": [
					{
						"original_method_signature": "func(a int) int",
						"new_method_signature":      "func(a int, b string) int",
						"code_sample":               ["line1", "line2"],
						"suggested_change":          ["line3", "line4"],
						"number_of_affected_lines":  "10",
						"complexity":                "high",
						"description":               "Add parameter b"
					}
				],
				"general_warnings": ["warning1", "warning2"]
			}`,
			wantSnippets: []Snippet{
				{
					Id:                       "snippet_0_0",
					SourceMethodSignature:    "func(a int) int",
					SuggestedMethodSignature: "func(a int, b string) int",
					SourceCodeSnippet:        []string{"line1", "line2"},
					SuggestedCodeSnippet:     []string{"line3", "line4"},
					NumberOfAffectedLines:    10,
					Complexity:               "high",
					Explanation:              "Add parameter b",
					RelativeFilePath:         "/src/main.go",
					FilePath:                 "/home/user/project/src/main.go",
					IsDao:                    false,
				},
			},
			wantWarnings: []string{"warning1", "warning2"},
			wantErr:      false,
		},
		{
			name: "valid input with multiple code impacts",
			input: `{
				"file_modifications": [
					{
						"original_method_signature": "func(a int) int",
						"new_method_signature":      "func(a int, b string) int",
						"code_sample":               ["line1", "line2"],
						"suggested_change":          ["line3", "line4"],
						"number_of_affected_lines":  "10",
						"complexity":                "high",
						"description":               "Add parameter b"
					},
					{
						"original_method_signature": "func(a int) int",
						"new_method_signature":      "func(a int) int",
						"code_sample":               ["line5", "line6"],
						"suggested_change":          ["line7", "line8"],
						"number_of_affected_lines":  "5",
						"complexity":                "medium",
						"description":               "Update variable c"
					}
				],
				"general_warnings": []
			}`,
			wantSnippets: []Snippet{
				{
					Id:                       "snippet_0_0",
					SourceMethodSignature:    "func(a int) int",
					SuggestedMethodSignature: "func(a int, b string) int",
					SourceCodeSnippet:        []string{"line1", "line2"},
					SuggestedCodeSnippet:     []string{"line3", "line4"},
					NumberOfAffectedLines:    10,
					Complexity:               "high",
					Explanation:              "Add parameter b",
					RelativeFilePath:         "/src/main.go",
					FilePath:                 "/home/user/project/src/main.go",
					IsDao:                    false,
				},
				{
					Id:                       "snippet_0_1",
					SourceMethodSignature:    "func(a int) int",
					SuggestedMethodSignature: "func(a int) int",
					SourceCodeSnippet:        []string{"line5", "line6"},
					SuggestedCodeSnippet:     []string{"line7", "line8"},
					NumberOfAffectedLines:    5,
					Complexity:               "medium",
					Explanation:              "Update variable c",
					RelativeFilePath:         "/src/main.go",
					FilePath:                 "/home/user/project/src/main.go",
					IsDao:                    false,
				},
			},
			wantWarnings: []string{},
			wantErr:      false,
		},
		{
			name: "empty file modifications",
			input: `{
				"file_modifications": [],
				"general_warnings": ["warning1"]
			}`,
			wantSnippets: []Snippet{},
			wantWarnings: []string{"warning1"},
			wantErr:      false,
		},
		{
			name: "empty general warnings",
			input: `{
				"file_modifications": [
					{
						"original_method_signature": "func(a int) int",
						"new_method_signature":      "func(a int, b string) int",
						"code_sample":               ["line1", "line2"],
						"suggested_change":          ["line3", "line4"],
						"number_of_affected_lines":  "10",
						"complexity":                "high",
						"description":               "Add parameter b"
					}
				],
				"general_warnings": []
			}`,
			wantSnippets: []Snippet{
				{
					Id:                       "snippet_0_0",
					SourceMethodSignature:    "func(a int) int",
					SuggestedMethodSignature: "func(a int, b string) int",
					SourceCodeSnippet:        []string{"line1", "line2"},
					SuggestedCodeSnippet:     []string{"line3", "line4"},
					NumberOfAffectedLines:    10,
					Complexity:               "high",
					Explanation:              "Add parameter b",
					RelativeFilePath:         "/src/main.go",
					FilePath:                 "/home/user/project/src/main.go",
					IsDao:                    false,
				},
			},
			wantWarnings: []string{},
			wantErr:      false,
		},
		{
			name: "missing general warnings",
			input: `{
				"file_modifications": [
					{
						"original_method_signature": "func(a int) int",
						"new_method_signature":      "func(a int, b string) int",
						"code_sample":               ["line1", "line2"],
						"suggested_change":          ["line3", "line4"],
						"number_of_affected_lines":  "10",
						"complexity":                "high",
						"description":               "Add parameter b"
					}
				]
			}`,
			wantSnippets: []Snippet{
				{
					Id:                       "snippet_0_0",
					SourceMethodSignature:    "func(a int) int",
					SuggestedMethodSignature: "func(a int, b string) int",
					SourceCodeSnippet:        []string{"line1", "line2"},
					SuggestedCodeSnippet:     []string{"line3", "line4"},
					NumberOfAffectedLines:    10,
					Complexity:               "high",
					Explanation:              "Add parameter b",
					RelativeFilePath:         "/src/main.go",
					FilePath:                 "/home/user/project/src/main.go",
					IsDao:                    false,
				},
			},
			wantWarnings: []string{},
			wantErr:      false,
		},
		{
			name:         "invalid json",
			input:        `invalid json`,
			wantSnippets: nil,
			wantWarnings: nil,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSnippets, gotWarnings, err := ParseNonDaoFileChanges(tt.input, projectPath, filePath, fileIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseNonDaoFileChanges() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotSnippets, tt.wantSnippets) {
				t.Errorf("ParseNonDaoFileChanges() gotSnippets = %v, want %v", gotSnippets, tt.wantSnippets)
			}
			if !reflect.DeepEqual(gotWarnings, tt.wantWarnings) {
				t.Errorf("ParseNonDaoFileChanges() gotWarnings = %v, want %v", gotWarnings, tt.wantWarnings)
			}
		})
	}
}

func TestParseDaoFileChanges(t *testing.T) {
	projectPath := "/home/user/project"
	filePath := "/home/user/project/src/main.go"
	fileIndex := 0

	tests := []struct {
		name         string
		input        string
		wantSnippets []Snippet
		wantErr      bool
	}{
		{
			name: "valid input with one schema impact",
			input: `{
				"code_changes": [
					{
						"schema_change": {"is_schema_related_change": true, "table": "mytable", "column": "mycolumn"},
						"number_of_affected_lines": "10",
						"existing_code_lines": ["line1", "line2"],
						"new_code_lines": ["line3", "line4"],
						"explanation":"test change"
					}
				]
			}`,
			wantSnippets: []Snippet{
				{
					Id:                    "snippet_0_0",
					TableName:             "mytable",
					ColumnName:            "mycolumn",
					SchemaChange:          "test change",
					NumberOfAffectedLines: 10,
					SourceCodeSnippet:     []string{"line1", "line2"},
					SuggestedCodeSnippet:  []string{"line3", "line4"},
					RelativeFilePath:      "/src/main.go",
					FilePath:              "/home/user/project/src/main.go",
					IsDao:                 true,
				},
			},
			wantErr: false,
		},
		{
			name: "valid input with multiple schema impacts",
			input: `{
				"code_changes": [
					{
						"schema_change": {"is_schema_related_change": true, "table": "mytable", "column": "mycolumn"},
						"number_of_affected_lines": "10",
						"existing_code_lines": ["line1", "line2"],
						"new_code_lines": ["line3", "line4"],
						"explanation":"test change"
					},
					{
						"schema_change": {"is_schema_related_change": true, "table": "mytable2", "column": "mycolumn2"},
						"number_of_affected_lines": "5",
						"existing_code_lines": ["line5", "line6"],
						"new_code_lines": ["line7", "line8"],
						"explanation":"test change"
					}
				],
				"general_warnings": []
			}`,
			wantSnippets: []Snippet{
				{
					Id:                    "snippet_0_0",
					TableName:             "mytable",
					ColumnName:            "mycolumn",
					SchemaChange:          "test change",
					NumberOfAffectedLines: 10,
					SourceCodeSnippet:     []string{"line1", "line2"},
					SuggestedCodeSnippet:  []string{"line3", "line4"},
					RelativeFilePath:      "/src/main.go",
					FilePath:              "/home/user/project/src/main.go",
					IsDao:                 true,
				},
				{
					Id:                    "snippet_0_1",
					TableName:             "mytable2",
					ColumnName:            "mycolumn2",
					SchemaChange:          "test change",
					NumberOfAffectedLines: 5,
					SourceCodeSnippet:     []string{"line5", "line6"},
					SuggestedCodeSnippet:  []string{"line7", "line8"},
					RelativeFilePath:      "/src/main.go",
					FilePath:              "/home/user/project/src/main.go",
					IsDao:                 true,
				},
			},
			wantErr: false,
		},
		{
			name: "empty schema impacts",
			input: `{
				"code_changes": [],
				"general_warnings": ["warning1"]
			}`,
			wantSnippets: []Snippet{},
			wantErr:      false,
		},
		{
			name: "valid input with query_change",
			input: `{
				"code_changes": [
					{
						"schema_change": {"is_schema_related_change": true, "table": "mytable", "column": "mycolumn"},
						"number_of_affected_lines": "10",
						"existing_code_lines": ["line1", "line2"],
						"new_code_lines": ["line3", "line4"],
						"query_change": {
							"old_query": "SELECT * FROM mytable WHERE id = 1",
							"normalized_query": "SELECT * FROM mytable WHERE id = ?",
							"new_query": "SELECT * FROM mytable WHERE id = @id",
							"explanation": "Parameter syntax changed",
							"complexity": "SIMPLE",
							"number_of_query_occurances": 1,
							"cross_db_joins": false,
							"tables_affected": ["mytable"],
							"ddl_statement": false,
							"functions_used": ["NOW()"],
							"operators_used": ["="],
							"databases_referenced": ["db1"],
							"select_for_update": false
						}
					}
				],
				"general_warnings": []
			}`,
			wantSnippets: []Snippet{
				{
					Id:                    "snippet_0_0",
					TableName:             "mytable",
					ColumnName:            "mycolumn",
					NumberOfAffectedLines: 10,
					SourceCodeSnippet:     []string{"line1", "line2"},
					SuggestedCodeSnippet:  []string{"line3", "line4"},
					RelativeFilePath:      "/src/main.go",
					FilePath:              "/home/user/project/src/main.go",
					IsDao:                 true,
				},
			},
			wantErr: false,
		},
		{
			name:         "invalid json",
			input:        `invalid json`,
			wantSnippets: nil,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snippets, _, err := ParseDaoFileChanges(tt.input, projectPath, filePath, fileIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDaoFileChanges() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(snippets, tt.wantSnippets) {
				t.Errorf("ParseDaoFileChanges() gotSnippets = %v, want %v", snippets, tt.wantSnippets)
			}
		})
	}
}

func TestParseFileAnalyzerResponse(t *testing.T) {
	projectPath := "/home/user/project"
	filePath := "/home/user/project/src/main.go"
	fileIndex := 0

	tests := []struct {
		name    string
		input   string
		isDao   bool
		want    *CodeAssessment
		wantErr bool
	}{
		{
			name: "isDao true, valid input",
			input: `{
				"code_changes": [
					{
						"schema_change": {"is_schema_related_change": true, "table": "mytable", "column": "mycolumn"},
						"number_of_affected_lines": "10",
						"existing_code_lines": ["line1", "line2"],
						"new_code_lines": ["line3", "line4"]
					}
				],
				"general_warnings": ["warning1", "warning2"]
			}`,
			isDao: true,
			want: &CodeAssessment{
				Snippets: &[]Snippet{
					{
						Id:                    "snippet_0_0",
						TableName:             "mytable",
						ColumnName:            "mycolumn",
						NumberOfAffectedLines: 10,
						SourceCodeSnippet:     []string{"line1", "line2"},
						SuggestedCodeSnippet:  []string{"line3", "line4"},
						RelativeFilePath:      "/src/main.go",
						FilePath:              "/home/user/project/src/main.go",
						IsDao:                 true,
						SchemaChange:          "",
					},
				},
				GeneralWarnings: nil,
			},
			wantErr: false,
		},
		{
			name: "isDao false, valid input",
			input: `{
				"file_modifications": [
					{
						"original_method_signature": "func(a int) int",
						"new_method_signature":      "func(a int, b string) int",
						"code_sample":               ["line1", "line2"],
						"suggested_change":          ["line3", "line4"],
						"number_of_affected_lines":  "10",
						"complexity":                "high",
						"description":               "Add parameter b"
					}
				],
				"general_warnings": ["warning1", "warning2"]
			}`,
			isDao: false,
			want: &CodeAssessment{
				Snippets: &[]Snippet{
					{
						Id:                       "snippet_0_0",
						SourceMethodSignature:    "func(a int) int",
						SuggestedMethodSignature: "func(a int, b string) int",
						SourceCodeSnippet:        []string{"line1", "line2"},
						SuggestedCodeSnippet:     []string{"line3", "line4"},
						NumberOfAffectedLines:    10,
						Complexity:               "high",
						Explanation:              "Add parameter b",
						RelativeFilePath:         "/src/main.go",
						FilePath:                 "/home/user/project/src/main.go",
						IsDao:                    false,
					},
				},
				GeneralWarnings: []string{"warning1", "warning2"},
			},
			wantErr: false,
		},
		{
			name:    "invalid json",
			input:   `invalid json`,
			isDao:   true,
			want:    nil,
			wantErr: true,
		},
		{
			name: "isDao true, empty schema impact",
			input: `{
				"code_changes": [],
				"general_warnings": ["warning1"]
			}`,
			isDao: true,
			want: &CodeAssessment{
				Snippets:        &[]Snippet{},
				GeneralWarnings: nil,
			},
			wantErr: false,
		},
		{
			name: "isDao false, empty file modifications",
			input: `{
				"file_modifications": [],
				"general_warnings": ["warning1"]
			}`,
			isDao: false,
			want: &CodeAssessment{
				Snippets:        &[]Snippet{},
				GeneralWarnings: []string{"warning1"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := ParseFileAnalyzerResponse(projectPath, filePath, tt.input, tt.isDao, fileIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseFileAnalyzerResponse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil && tt.want == nil {
				return
			}
			if got == nil || tt.want == nil {
				t.Errorf("ParseFileAnalyzerResponse() = %v, want %v", got, tt.want)
				return
			}
			if !reflect.DeepEqual(*got.Snippets, *tt.want.Snippets) {
				t.Errorf("Snippets = %v, want %v", *got.Snippets, *tt.want.Snippets)
			}
			if !reflect.DeepEqual(got.GeneralWarnings, tt.want.GeneralWarnings) {
				t.Errorf("GeneralWarnings = %v, want %v", got.GeneralWarnings, tt.want.GeneralWarnings)
			}
		})
	}
}
