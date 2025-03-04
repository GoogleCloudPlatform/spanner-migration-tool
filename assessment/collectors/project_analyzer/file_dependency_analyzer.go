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
	"fmt"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/dominikbraun/graph"
	"go.uber.org/zap"
	"golang.org/x/tools/go/packages"
)

// DependencyAnalyzer defines the interface for dependency analysis
type DependencyAnalyzer interface {
	getDependencyGraph(directory string) map[string]map[string]struct{}
	IsDAO(filePath string, fileContent string) bool
	GetExecutionOrder(projectDir string) (map[string]map[string]struct{}, [][]string)
	LogDependencyGraph(dependencyGraphmap map[string]map[string]struct{}, projectDir string)
	LogExecutionOrder(groupedTasks [][]string)
}

// BaseAnalyzer provides default implementation for execution order
type BaseAnalyzer struct{}

// GoDependencyAnalyzer implements DependencyAnalyzer for Go projects
type GoDependencyAnalyzer struct {
	BaseAnalyzer
}

func (g *GoDependencyAnalyzer) getDependencyGraph(directory string) map[string]map[string]struct{} {

	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  (directory),
	}

	logger.Log.Debug(directory)

	pkgs, err := packages.Load(cfg, "./...")

	if err != nil {
		logger.Log.Fatal("Error loading packages: ", zap.Error(err))
	}

	// Dependency graph: key = file, value = list of files it depends on
	dependencyGraph := make(map[string]map[string]struct{})

	dependencyGraphCycleCheck := graph.New(graph.StringHash, graph.Directed(), graph.PreventCycles())

	// Iterate through all packages and process their files
	for _, pkg := range pkgs {
		if pkg.TypesInfo == nil {
			continue
		}

		// Process symbol usages (functions, variables, structs)
		for ident, obj := range pkg.TypesInfo.Uses {
			if obj != nil && obj.Pos().IsValid() {
				useFile := pkg.Fset.Position(ident.Pos()).Filename

				// Only process files inside the project directory
				if strings.HasPrefix(useFile, directory) {
					// Get the file where the symbol is defined
					defFile := pkg.Fset.Position(obj.Pos()).Filename

					// Only add if the file is inside the project directory and avoid redundant edges
					if strings.HasPrefix(defFile, directory) && useFile != defFile {
						// Initialize the map for the useFile if not present
						if _, ok := dependencyGraph[useFile]; !ok {
							dependencyGraph[useFile] = make(map[string]struct{})
							dependencyGraphCycleCheck.AddVertex(useFile)
						}

						if _, ok := dependencyGraph[defFile]; !ok {
							dependencyGraph[defFile] = make(map[string]struct{})
							dependencyGraphCycleCheck.AddVertex(defFile)
						}

						if dependencyGraphCycleCheck.AddEdge(useFile, defFile) != nil {
							logger.Log.Debug("Cycle detected: ", zap.String("useFile", useFile), zap.String("defFile", defFile))
						} else if _, exists := dependencyGraph[useFile][defFile]; !exists {
							dependencyGraph[useFile][defFile] = struct{}{}
						}
					}
				}
			}
		}
	}

	// Print the dependency graph
	logger.Log.Debug("\nDependency Graph:")
	for file, dependencies := range dependencyGraph {
		logger.Log.Debug("Source file:", zap.String("file", file))
		//ToDo:Better way to show dependencies
		for dep := range dependencies {
			logger.Log.Debug("Depends on:", zap.String("dep", dep))
		}
	}

	return dependencyGraph
}

func (g *GoDependencyAnalyzer) IsDAO(filePath string, fileContent string) bool {
	filePath = strings.ToLower(filePath)
	if strings.Contains(filePath, "/dao/") {
		return true
	}

	if strings.Contains(fileContent, "database/sql") || strings.Contains(fileContent, "github.com/go-sql-driver/mysql") {
		return true
	}

	if strings.Contains(fileContent, "*sql.DB") || strings.Contains(fileContent, "*sql.Tx") {
		return true
	}

	return false
}

func (g *GoDependencyAnalyzer) GetExecutionOrder(projectDir string) (map[string]map[string]struct{}, [][]string) {
	G := g.getDependencyGraph(projectDir)

	sortedTasks, err := topologicalSort(G)
	if err != nil {
		logger.Log.Debug("Graph still has cycles after relaxation. Sorting not possible: ", zap.Error(err))
		return nil, nil
	}

	logger.Log.Debug("Execution order determined successfully.")
	return G, sortedTasks
}

// AnalyzerFactory creates DependencyAnalyzer instances
func AnalyzerFactory(language string) DependencyAnalyzer {
	switch language {
	case "go":
		return &GoDependencyAnalyzer{}
	default:
		panic("Unsupported language")
	}
}

func detectAndRemoveCycles(G map[string]map[string]struct{}) {
	visited := make(map[string]bool)
	stack := make(map[string]bool)
	edgesToRemove := []struct {
		from string
		to   string
	}{}

	// Recursive DFS function to detect cycles
	var visit func(node string) bool
	visit = func(node string) bool {
		if stack[node] {
			// Found a cycle, mark this edge for removal
			for parent := range G {
				if _, exists := G[parent][node]; exists {
					edgesToRemove = append(edgesToRemove, struct{ from, to string }{parent, node})
					break
				}
			}
			return true
		}
		if visited[node] {
			return false
		}

		visited[node] = true
		stack[node] = true

		for neighbor := range G[node] {
			if visit(neighbor) {
				return true
			}
		}

		stack[node] = false
		return false
	}

	// Run DFS on all nodes
	for node := range G {
		if !visited[node] {
			visit(node)
		}
	}

	// Remove the detected edges from the graph
	for _, edge := range edgesToRemove {
		delete(G[edge.from], edge.to)
		logger.Log.Debug("Removed edge: ", zap.String("from", edge.from), zap.String("to", edge.to))
	}
}

func topologicalSort(G map[string]map[string]struct{}) ([][]string, error) {
	inDegree := make(map[string]int)
	for node := range G {
		inDegree[node] = 0
	}
	var maxDegree int

	for node := range G {
		for neighbor := range G[node] {
			inDegree[neighbor]++
			if inDegree[neighbor] > maxDegree {
				maxDegree = inDegree[neighbor]
			}
		}
	}

	taskLevels := make([][]string, maxDegree+1)

	for node, degree := range inDegree {
		degree = maxDegree - degree
		taskLevels[degree] = append(taskLevels[degree], node)
	}

	return taskLevels, nil
}

func (g *GoDependencyAnalyzer) LogDependencyGraph(dependencyGraph map[string]map[string]struct{}, projectDir string) {

	logger.Log.Debug("\nDependency Graph:")
	for file, dependencies := range dependencyGraph {
		fmt.Println("depends on: ", strings.TrimPrefix(file, projectDir))
		for dep := range dependencies {
			fmt.Println("\t: ", strings.TrimPrefix(dep, projectDir))
		}
	}
}

func (g *GoDependencyAnalyzer) LogExecutionOrder(groupedTasks [][]string) {

	// Print results
	logger.Log.Debug("Execution Order Groups:")
	for i, group := range groupedTasks {
		logger.Log.Debug("Level: ", zap.Int("level", i), zap.String("group", strings.Join(group, ", ")))
	}
}

// func main() {
// 	logger.Log, _ = zap.NewDevelopment()

// 	projectDir := "" // Change this to your actual project directory
// 	language := "go" // Change this to "java" for Java projects

// 	// Create analyzer instance using factory
// 	analyzer := AnalyzerFactory(language)

// 	// Run execution order analysis
// 	G, groupedTasks := analyzer.GetExecutionOrder(projectDir)
// 	analyzer.LogDependencyGraph(G, projectDir)
// 	analyzer.LogExecutionOrder(groupedTasks)
// }
