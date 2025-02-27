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
	"log"
	"strings"

	"golang.org/x/tools/go/packages"
)

// DependencyAnalyzer defines the interface for dependency analysis
type DependencyAnalyzer interface {
	GetDependencyGraph(directory string) map[string]map[string]struct{}
	IsDAO(filePath string, fileContent string) bool
	GetExecutionOrder(projectDir string) (map[string]map[string]struct{}, [][]string)
}

// BaseAnalyzer provides default implementation for execution order
type BaseAnalyzer struct{}

// GoDependencyAnalyzer implements DependencyAnalyzer for Go projects
type GoDependencyAnalyzer struct {
	BaseAnalyzer
}

func (g *GoDependencyAnalyzer) GetDependencyGraph(directory string) map[string]map[string]struct{} {

	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  (directory),
	}

	fmt.Println(directory)

	pkgs, err := packages.Load(cfg, "./...")

	fmt.Println(directory)
	if err != nil {
		log.Fatalf("Error loading packages: %v", err)
	}

	// Dependency graph: key = file, value = list of files it depends on
	dependencyGraph := make(map[string]map[string]struct{})

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
						}

						if _, ok := dependencyGraph[defFile]; !ok {
							dependencyGraph[defFile] = make(map[string]struct{})
						}

						// Add the dependency edge only if it doesn't already exist
						if _, exists := dependencyGraph[useFile][defFile]; !exists {
							dependencyGraph[useFile][defFile] = struct{}{}
						}
					}
				}
			}
		}
	}

	// Print the dependency graph
	fmt.Println("\nDependency Graph:")
	for file, dependencies := range dependencyGraph {
		fmt.Printf("%s depends on:\n", strings.TrimPrefix(file, directory))
		//ToDo:Better way to show dependencies
		for dep := range dependencies {
			fmt.Printf("\t- %s\n", strings.TrimPrefix(dep, directory))
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
	G := g.GetDependencyGraph(projectDir)

	// Detect and relax cycles in the dependency graph
	// Remove cycles before sorting
	detectAndRemoveCycles(G)

	sortedTasks, err := topologicalSort(G)
	if err != nil {
		fmt.Println("Graph still has cycles after relaxation. Sorting not possible.")
		return nil, nil
	}

	fmt.Println(sortedTasks)
	groupedTasks := groupTasksOptimized(sortedTasks, G)

	fmt.Println("Execution order determined successfully.")
	return G, groupedTasks
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
		fmt.Printf("Removed edge: %s -> %s\n", edge.from, edge.to)
	}
}

func topologicalSort(G map[string]map[string]struct{}) ([]string, error) {
	inDegree := make(map[string]int)
	for node := range G {
		inDegree[node] = 0
	}
	for node := range G {
		for neighbor := range G[node] {
			inDegree[neighbor]++
		}
	}

	// Collect nodes with 0 in-degree
	queue := []string{}
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	order := []string{}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)

		// Reduce in-degree for neighbors
		for neighbor := range G[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	// If we didn't process all nodes, there is still a cycle
	if len(order) != len(G) {
		return nil, fmt.Errorf("graph has a cycle, topological sorting is not possible")
	}

	return order, nil
}

// groupTasksOptimized groups tasks with the same execution level.
func groupTasksOptimized(sortedTasks []string, G map[string]map[string]struct{}) [][]string {
	taskLevels := [][]string{}
	taskSet := make(map[string]int)

	for _, task := range sortedTasks {
		maxLevel := 0
		for dep := range G[task] {
			if level, exists := taskSet[dep]; exists && level+1 > maxLevel {
				maxLevel = level + 1
			}
		}
		if maxLevel >= len(taskLevels) {
			taskLevels = append(taskLevels, []string{})
		}
		taskLevels[maxLevel] = append(taskLevels[maxLevel], task)
		taskSet[task] = maxLevel
	}

	return taskLevels
}

// func main() {

// 	projectDir := "/usr/local/google/home/gauravpurohit/migration/spanner-migration-tool/" // Change this to your actual project directory
// 	language := "go"                                                                       // Change this to "java" for Java projects

// 	// Create analyzer instance using factory
// 	analyzer := AnalyzerFactory(language)

// 	// Run execution order analysis
// 	G, groupedTasks := analyzer.GetExecutionOrder(projectDir)

// 	fmt.Println("\nDependency Graph:")
// 	for file, dependencies := range G {
// 		fmt.Printf("%s depends on:\n", strings.TrimPrefix(file, projectDir))
// 		for dep := range dependencies {
// 			fmt.Printf("\t- %s\n", strings.TrimPrefix(dep, projectDir))
// 		}
// 	}

// 	// Print results
// 	fmt.Println("Execution Order Groups:")
// 	for i, group := range groupedTasks {
// 		fmt.Printf("Level %d: %v\n", i+1, group)
// 	}
}
