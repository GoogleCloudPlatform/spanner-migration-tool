/*
	Copyright 2025 Google LLC

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
*/
package assessment

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func init() {
	logger.Log = zap.NewNop() // Set to a no-op logger during tests
}

func TestValidateGoroot(t *testing.T) {
	// Case 1: GOROOT is set
	os.Setenv("GOROOT", "/usr/local/go")
	err := validateGoroot()
	assert.NoError(t, err, "Expected no error when GOROOT is set")

	// Case 2: GOROOT is not set
	os.Unsetenv("GOROOT")
	err = validateGoroot()
	assert.Error(t, err, "Expected error when GOROOT is not set")
	assert.Contains(t, err.Error(), "please set GOROOT path", "Expected specific error message")

	// Clean up: Set GOROOT back if it was set before the test (optional, but good practice)
	// For simplicity, we are not restoring the original GOROOT value here.
}

func TestBaseAnalyzer_RemoveCycle(t *testing.T) {
	analyzer := &BaseAnalyzer{}

	tests := []struct {
		name                    string
		inputGraph              map[string]map[string]struct{}
		expectedGraphAssertions func(t *testing.T, graph map[string]map[string]struct{})
	}{
		{
			name: "Graph with no cycles",
			inputGraph: map[string]map[string]struct{}{
				"a": {"b": {}},
				"b": {"c": {}},
				"c": {},
			},
			expectedGraphAssertions: func(t *testing.T, g map[string]map[string]struct{}) {
				assert.Len(t, g, 3)
				assert.Contains(t, g, "a")
				assert.Contains(t, g["a"], "b")
				assert.Contains(t, g, "b")
				assert.Contains(t, g["b"], "c")
				assert.Contains(t, g, "c")
				assert.Empty(t, g["c"])
			},
		},
		{
			name: "Graph with a simple cycle (a -> b -> a)",
			inputGraph: map[string]map[string]struct{}{
				"a": {"b": {}},
				"b": {"a": {}},
				"c": {},
			},
			expectedGraphAssertions: func(t *testing.T, g map[string]map[string]struct{}) {
				assert.Len(t, g, 3)
				assert.Contains(t, g, "a")
				assert.Contains(t, g, "b")
				assert.Contains(t, g, "c")
				// One of the edges in the cycle should be removed.
				// The specific edge removed depends on the graph library's internal cycle detection.
				// We check that either a->b or b->a is present, but not both.
				_, aToB := g["a"]["b"]
				_, bToA := g["b"]["a"]
				assert.True(t, aToB != bToA, "Expected one edge of the cycle to be removed")
			},
		},
		{
			name: "Graph with a self-loop (a -> a)",
			inputGraph: map[string]map[string]struct{}{
				"a": {"a": {}},
				"b": {"a": {}},
			},
			expectedGraphAssertions: func(t *testing.T, g map[string]map[string]struct{}) {
				assert.Len(t, g, 2)
				assert.Contains(t, g, "a")
				assert.Contains(t, g, "b")
				assert.NotContains(t, g["a"], "a", "Expected self-loop to be removed")
				assert.Contains(t, g["b"], "a")
			},
		},
		{
			name: "Complex graph with multiple cycles",
			inputGraph: map[string]map[string]struct{}{
				"a": {"b": {}, "c": {}},
				"b": {"c": {}, "d": {}},
				"c": {"a": {}, "e": {}}, // c->a forms a cycle with a->b->c
				"d": {"c": {}},
				"e": {},
			},
			expectedGraphAssertions: func(t *testing.T, g map[string]map[string]struct{}) {
				assert.Len(t, g, 5)
				// Verify that the cycles are broken. The exact edges removed depend on the library's internal logic.
				// We can check for the absence of specific cycles or the presence of a directed acyclic graph (DAG).
				// For this test, we'll focus on ensuring that a topological sort can be performed, implying DAG.
				// (However, topological sort is tested separately, so here we focus on the state of the graph).
				// The easiest way to assert this is to check if the graph is a DAG, but that's redundant with the topological sort test.
				// For remove cycle, we specifically check if the cycle forming edges are removed.
				_, _ = g["a"]["b"]
				_, _ = g["b"]["c"]
				_, _ = g["c"]["a"]
				// One of these three edges forming the cycle a->b->c->a should be removed
				removedCount := 0
				if _, ok := g["a"]["b"]; !ok {
					removedCount++
				}
				if _, ok := g["b"]["c"]; !ok {
					removedCount++
				}
				if _, ok := g["c"]["a"]; !ok {
					removedCount++
				}
				assert.GreaterOrEqual(t, removedCount, 1, "Expected at least one edge from cycle a->b->c->a to be removed")
			},
		},
		{
			name:       "Empty graph",
			inputGraph: map[string]map[string]struct{}{},
			expectedGraphAssertions: func(t *testing.T, g map[string]map[string]struct{}) {
				assert.Empty(t, g)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultGraph := analyzer.RemoveCycle(tt.inputGraph)
			tt.expectedGraphAssertions(t, resultGraph)

			// Further verification: ensure no cycles remain in the output graph
			// This requires attempting a topological sort.
			// A successful topological sort implies no cycles.
			_, err := analyzer.TopologicalSort(resultGraph)
			assert.NoError(t, err, "Expected no cycles in the graph after RemoveCycle")
		})
	}
}

func TestGoDependencyAnalyzer_IsDAO(t *testing.T) {
	analyzer := &GoDependencyAnalyzer{}

	tests := []struct {
		name        string
		filePath    string
		fileContent string
		want        bool
	}{
		{
			name:        "File path contains /dao/",
			filePath:    "/project/src/dao/user.go",
			fileContent: "package dao",
			want:        true,
		},
		{
			name:        "File path contains /DAO/",
			filePath:    "/project/src/DAO/user.go",
			fileContent: "package DAO",
			want:        true,
		},
		{
			name:        "File content contains database/sql import",
			filePath:    "/project/src/repository/item.go",
			fileContent: `import "database/sql"`,
			want:        true,
		},
		{
			name:        "File content contains github.com/go-sql-driver/mysql import",
			filePath:    "/project/src/repository/item.go",
			fileContent: `import "github.com/go-sql-driver/mysql"`,
			want:        true,
		},
		{
			name:        "File content contains *sql.DB",
			filePath:    "/project/src/db/connector.go",
			fileContent: `func connect() *sql.DB`,
			want:        true,
		},
		{
			name:        "File content contains *sql.Tx",
			filePath:    "/project/src/db/transaction.go",
			fileContent: `func beginTx() (*sql.Tx, error)`,
			want:        true,
		},
		{
			name:        "File content contains gorm tag",
			filePath:    "/project/src/models/product.go",
			fileContent: `type Product struct { ID int ` + "`gorm:\"primaryKey\"`" + `}`,
			want:        true,
		},
		{
			name:        "File does not match any DAO criteria",
			filePath:    "/project/src/service/logic.go",
			fileContent: `package service`,
			want:        false,
		},
		{
			name:        "Empty file path and content",
			filePath:    "",
			fileContent: "",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := analyzer.IsDAO(tt.filePath, tt.fileContent)
			assert.Equal(t, tt.want, got, fmt.Sprintf("IsDAO(%s, %s) got %v, want %v", tt.filePath, tt.fileContent, got, tt.want))
		})
	}
}

func TestGoDependencyAnalyzer_GetFrameworkFromFileContent(t *testing.T) {
	analyzer := &GoDependencyAnalyzer{}

	tests := []struct {
		name        string
		fileContent string
		want        string
	}{
		{
			name:        "File content contains database/sql import",
			fileContent: `import "database/sql"`,
			want:        "database/sql",
		},
		{
			name:        "File content contains github.com/go-sql-driver/mysql import",
			fileContent: `import "github.com/go-sql-driver/mysql"`,
			want:        "database/sql",
		},
		{
			name:        "File content contains *sql.DB",
			fileContent: `func connect() *sql.DB`,
			want:        "database/sql",
		},
		{
			name:        "File content contains *sql.Tx",
			fileContent: `func beginTx() (*sql.Tx, error)`,
			want:        "database/sql",
		},
		{
			name:        "File content contains gorm tag",
			fileContent: `type Product struct { ID int ` + "`gorm:\"primaryKey\"`" + `}`,
			want:        "gorm",
		},
		{
			name:        "File content indicates no recognized framework",
			fileContent: `package main`,
			want:        "",
		},
		{
			name:        "Empty file content",
			fileContent: "",
			want:        "",
		},
		{
			name:        "Content with both database/sql and gorm (database/sql should take precedence as it's checked first)",
			fileContent: `import "database/sql"; type Product struct { ID int ` + "`gorm:\"primaryKey\"`" + `}`,
			want:        "database/sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := analyzer.GetFrameworkFromFileContent(tt.fileContent)
			assert.Equal(t, tt.want, got, fmt.Sprintf("GetFrameworkFromFileContent(%s) got %v, want %v", tt.fileContent, got, tt.want))
		})
	}
}

// Mock GoDependencyAnalyzer for testing GetExecutionOrder to control getDependencyGraph
type MockGoDependencyAnalyzer struct {
	GoDependencyAnalyzer
	mockGetDependencyGraph func(directory string) map[string]map[string]struct{}
}

func (m *MockGoDependencyAnalyzer) getDependencyGraph(directory string) map[string]map[string]struct{} {
	if m.mockGetDependencyGraph != nil {
		return m.mockGetDependencyGraph(directory)
	}
	// Default behavior if not mocked, though in this test it should always be mocked.
	return m.GoDependencyAnalyzer.getDependencyGraph(directory)
}

func TestBaseAnalyzer_TopologicalSort(t *testing.T) {
	analyzer := &BaseAnalyzer{}

	tests := []struct {
		name        string
		graph       map[string]map[string]struct{}
		expected    [][]string
		expectError bool
	}{
		{
			name:        "Empty graph",
			graph:       map[string]map[string]struct{}{},
			expected:    [][]string{}, // Single empty slice for 0 levels
			expectError: false,
		},
		{
			name: "Simple linear graph",
			graph: map[string]map[string]struct{}{
				"a": {"b": {}},
				"b": {"c": {}},
				"c": {},
			},
			expected:    [][]string{{"c"}, {"b"}, {"a"}},
			expectError: false,
		},
		{
			name: "Graph with branches",
			graph: map[string]map[string]struct{}{
				"a": {"b": {}, "c": {}},
				"b": {"d": {}},
				"c": {"d": {}},
				"d": {},
			},
			// Order within a level can vary
			expected:    [][]string{{"d"}, {"b", "c"}, {"a"}}, // Will verify contents, not strict order within level
			expectError: false,
		},
		{
			name: "Graph with multiple levels and independent nodes",
			graph: map[string]map[string]struct{}{
				"f": {},
				"g": {},
				"a": {"b": {}, "c": {}},
				"b": {"d": {}},
				"c": {"d": {}},
				"d": {"e": {}},
				"e": {},
			},
			expected:    [][]string{{"e", "f", "g"}, {"d"}, {"b", "c"}, {"a"}},
			expectError: false,
		},
		{
			name: "Graph with a cycle (should return error)",
			graph: map[string]map[string]struct{}{
				"a": {"b": {}},
				"b": {"a": {}}, // Cycle: a <-> b
				"c": {},
			},
			expected:    nil, // The `TopologicalSort` will not remove cycle, it's assumed to be removed.
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Deep copy the graph for each test to avoid modification by the function under test impacting subsequent tests
			graphCopy := make(map[string]map[string]struct{})
			for k, v := range tt.graph {
				graphCopy[k] = make(map[string]struct{})
				for kk, vv := range v {
					graphCopy[k][kk] = vv
				}
			}

			got, err := analyzer.TopologicalSort(graphCopy)

			if tt.expectError {
				assert.Error(t, err, "Expected an error for cyclic graph")
				assert.Nil(t, got, "Expected nil result for cyclic graph")
			} else {
				assert.NoError(t, err, "Did not expect an error for acyclic graph")
				assert.NotNil(t, got, "Expected non-nil result for acyclic graph")

				// Sort levels for consistent comparison of expected vs actual (as order within levels can vary)
				for i := range got {
					strings.Join(got[i], ",") // Use strings.Join to create a comparable string
				}
				for i := range tt.expected {
					strings.Join(tt.expected[i], ",") // Use strings.Join to create a comparable string
				}

				// We need a more robust way to compare:
				// 1. Check if the number of levels is the same.
				// 2. For each level, check if the elements are the same (ignoring order within the level).
				// 3. Verify the topological property (dependencies appear in earlier levels).

				assert.Len(t, got, len(tt.expected), "Expected same number of levels")

				// Convert expected to sets for easy comparison regardless of order within a level
				expectedSets := make([]map[string]struct{}, len(tt.expected))
				for i, level := range tt.expected {
					expectedSets[i] = make(map[string]struct{})
					for _, item := range level {
						expectedSets[i][item] = struct{}{}
					}
				}

				// Convert got to sets for easy comparison
				gotSets := make([]map[string]struct{}, len(got))
				for i, level := range got {
					gotSets[i] = make(map[string]struct{})
					for _, item := range level {
						gotSets[i][item] = struct{}{}
					}
				}
				// Special case: if both are empty, it's a match
				if len(got) == 1 && len(tt.expected) == 1 && len(got[0]) == 0 && len(tt.expected[0]) == 0 {
					assert.True(t, true, "Both empty, considered a match")
				} else {
					// Compare the sorted results level by level (using sets for order-independent check)
					for i := 0; i < len(got); i++ {
						assert.True(t, reflect.DeepEqual(gotSets[i], expectedSets[i]), fmt.Sprintf("Level %d mismatch: got %v, want %v", i, got[i], tt.expected[i]))
					}
				}

				// Verify topological property: For every edge (u, v) in G, u appears before v in the sorted order.
				nodeLevelMap := make(map[string]int)
				for levelIdx, level := range got {
					for _, node := range level {
						nodeLevelMap[node] = levelIdx
					}
				}

				for node, dependencies := range tt.graph {
					for dependency := range dependencies {
						if _, ok := nodeLevelMap[node]; !ok {
							t.Fatalf("Node %s not found in sorted tasks", node)
						}
						if _, ok := nodeLevelMap[dependency]; !ok {
							t.Fatalf("Dependency %s not found in sorted tasks", dependency)
						}
						assert.Less(t, nodeLevelMap[dependency], nodeLevelMap[node], "Dependency %s should be at an earlier level than %s", dependency, node)
					}
				}
			}
		})
	}
}

func TestAnalyzerFactory(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		language string
		wantType reflect.Type
		wantErr  bool
	}{
		{
			name:     "Go language",
			language: "go",
			wantType: reflect.TypeOf(&GoDependencyAnalyzer{}),
			wantErr:  false,
		},
		{
			name:     "Java language",
			language: "java",
			wantType: reflect.TypeOf(&JavaDependencyAnalyzer{}),
			wantErr:  false,
		},
		{
			name:     "Unsupported language",
			language: "unsupported",
			wantType: nil,
			wantErr:  true, // Expect panic
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				assert.Panics(t, func() {
					AnalyzerFactory(tt.language, ctx)
				}, "Expected panic for unsupported language: "+tt.language)
			} else {
				analyzer := AnalyzerFactory(tt.language, ctx)
				assert.NotNil(t, analyzer, "Expected non-nil analyzer")
				assert.IsType(t, tt.wantType, reflect.TypeOf(analyzer), "Expected analyzer of type %v, got %v", tt.wantType, reflect.TypeOf(analyzer))
			}
		})
	}
}
