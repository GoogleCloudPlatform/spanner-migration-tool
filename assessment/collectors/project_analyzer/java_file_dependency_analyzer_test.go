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
	"path/filepath"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	sitter "github.com/smacker/go-tree-sitter"
	"github.com/smacker/go-tree-sitter/java"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func init() {
	logger.Log = zap.NewNop() // Set to a no-op logger during tests
}

func TestJavaDependencyAnalyzer_IsDAO(t *testing.T) {
	analyzer := &JavaDependencyAnalyzer{}

	tests := []struct {
		name        string
		filePath    string
		fileContent string
		want        bool
	}{
		{
			name:        "File path contains 'dao'",
			filePath:    "/project/src/main/java/com/example/dao/UserDAO.java",
			fileContent: "package com.example.dao;",
			want:        true,
		},
		{
			name:        "File path contains 'DAO' (case-insensitive)",
			filePath:    "/project/src/main/java/com/example/DAO/UserRepository.java",
			fileContent: "package com.example.DAO;",
			want:        true,
		},
		{
			name:        "File content contains 'jdbc'",
			filePath:    "/project/src/main/java/com/example/data/JdbcConnector.java",
			fileContent: "import java.sql.DriverManager; public class JdbcConnector { private Connection conn; }",
			want:        true,
		},
		{
			name:        "File content contains 'mysql'",
			filePath:    "/project/src/main/java/com/example/data/MysqlDataSource.java",
			fileContent: "import com.mysql.cj.jdbc.MysqlDataSource;",
			want:        true,
		},
		{
			name:        "Neither file path nor content indicates DAO",
			filePath:    "/project/src/main/java/com/example/service/UserService.java",
			fileContent: "package com.example.service; public class UserService {}",
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
			assert.Equal(t, tt.want, got, fmt.Sprintf("IsDAO(%q, %q) got %v, want %v", tt.filePath, tt.fileContent, got, tt.want))
		})
	}
}

func TestJavaDependencyAnalyzer_GetFrameworkFromFileContent(t *testing.T) {
	analyzer := &JavaDependencyAnalyzer{}

	tests := []struct {
		name        string
		fileContent string
		want        string
	}{
		{
			name:        "File content indicates Hibernate",
			fileContent: "import org.hibernate.Session; public class MyDao {}",
			want:        "Hibernate",
		},
		{
			name:        "File content indicates MyBatis",
			fileContent: "import org.apache.ibatis.session.SqlSession; public class MyDao {}",
			want:        "MyBatis",
		},
		{
			name:        "File content indicates JDBC (DriverManager)",
			fileContent: "import java.sql.DriverManager; public class MyDao {}",
			want:        "JDBC",
		},
		{
			name:        "File content indicates JDBC (DataSource)",
			fileContent: "import javax.sql.DataSource; public class MyDao {}",
			want:        "JDBC",
		},
		{
			name:        "File content indicates Spring Data JPA",
			fileContent: "import org.springframework.data.jpa.repository.JpaRepository; public interface MyRepo extends JpaRepository {}",
			want:        "Spring Data JPA",
		},
		{
			name:        "File content indicates no recognized framework",
			fileContent: "package com.example.service; public class UserService {}",
			want:        "",
		},
		{
			name:        "Empty file content",
			fileContent: "",
			want:        "",
		},
		{
			name:        "Multiple frameworks (Hibernate should be prioritized as it's checked first)",
			fileContent: `import org.hibernate.Session; import org.apache.ibatis.session.SqlSession;`,
			want:        "Hibernate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := analyzer.GetFrameworkFromFileContent(tt.fileContent)
			assert.Equal(t, tt.want, got, fmt.Sprintf("GetFrameworkFromFileContent(%q) got %q, want %q", tt.fileContent, got, tt.want))
		})
	}
}

func TestJavaDependencyAnalyzer_GetExecutionOrder(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "java_assessment_test_project")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create dummy Java files with dependencies
	// main.java -> ServiceA.java -> DAO.java
	// ServiceB.java -> DAO.java
	// Unrelated.java

	daoContent := `
package com.example.data;

public class UserDAO {
    public void save() { /* DB logic */ }
}
`
	serviceAContent := `
package com.example.service;

import com.example.data.UserDAO;

public class UserServiceA {
    private UserDAO userDAO = new UserDAO();
    public void createUser() { userDAO.save(); }
}
`
	serviceBContent := `
package com.example.service;

import com.example.data.UserDAO;

public class UserServiceB {
    public void deleteUser() { new UserDAO().save(); }
}
`
	mainContent := `
package com.example.app;

import com.example.service.UserServiceA;
import com.example.service.UserServiceB;

public class Application {
    public static void main(String[] args) {
        UserServiceA serviceA = new UserServiceA();
        UserServiceB serviceB = new UserServiceB();
        serviceA.createUser();
        serviceB.deleteUser();
    }
}
`
	unrelatedContent := `
package com.example.util;

public class Helper {
    public String greet() { return "Hello"; }
}
`

	// Create directories and files
	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "com", "example", "data"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "data", "UserDAO.java"), []byte(daoContent), 0644))

	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "com", "example", "service"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "service", "UserServiceA.java"), []byte(serviceAContent), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "service", "UserServiceB.java"), []byte(serviceBContent), 0644))

	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "com", "example", "app"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "app", "Application.java"), []byte(mainContent), 0644))

	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "com", "example", "util"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "util", "Helper.java"), []byte(unrelatedContent), 0644))

	analyzer := &JavaDependencyAnalyzer{ctx: context.Background()}
	G, sortedTasks := analyzer.GetExecutionOrder(tmpDir)

	assert.NotNil(t, G, "Expected non-nil dependency graph")
	assert.NotNil(t, sortedTasks, "Expected non-nil sorted tasks")

	// Expected dependencies (filepaths relative to tmpDir)
	userDAOPath := filepath.Join(tmpDir, "com", "example", "data", "UserDAO.java")
	userServiceAPath := filepath.Join(tmpDir, "com", "example", "service", "UserServiceA.java")
	userServiceBPath := filepath.Join(tmpDir, "com", "example", "service", "UserServiceB.java")
	applicationPath := filepath.Join(tmpDir, "com", "example", "app", "Application.java")
	helperPath := filepath.Join(tmpDir, "com", "example", "util", "Helper.java")

	// Verify the graph structure (some flexibility on order due to map iteration)
	assert.Contains(t, G, applicationPath)
	assert.Contains(t, G[applicationPath], userServiceAPath)
	assert.Contains(t, G[applicationPath], userServiceBPath)

	assert.Contains(t, G, userServiceAPath)
	assert.Contains(t, G[userServiceAPath], userDAOPath)

	assert.Contains(t, G, userServiceBPath)
	assert.Contains(t, G[userServiceBPath], userDAOPath)

	assert.Contains(t, G, userDAOPath)
	assert.Empty(t, G[userDAOPath]) // UserDAO should have no outgoing dependencies

	assert.Contains(t, G, helperPath)
	assert.Empty(t, G[helperPath]) // Helper should have no dependencies

	// Verify the topological sort order
	// The specific order within a level can vary, so we verify that dependencies are in earlier levels.
	nodeLevelMap := make(map[string]int)
	for levelIdx, level := range sortedTasks {
		for _, node := range level {
			nodeLevelMap[node] = levelIdx
		}
	}

	// Assert that UserDAO and Helper are at the lowest level (or among the lowest)
	assert.LessOrEqual(t, nodeLevelMap[userDAOPath], 0, "UserDAO should be at the lowest level")

	// Assert dependencies:
	// Application.java depends on UserServiceA.java and UserServiceB.java
	assert.Less(t, nodeLevelMap[userServiceAPath], nodeLevelMap[applicationPath], "UserServiceA should be at an earlier level than Application")
	assert.Less(t, nodeLevelMap[userServiceBPath], nodeLevelMap[applicationPath], "UserServiceB should be at an earlier level than Application")

	// UserServiceA.java depends on UserDAO.java
	assert.Less(t, nodeLevelMap[userDAOPath], nodeLevelMap[userServiceAPath], "UserDAO should be at an earlier level than UserServiceA")

	// UserServiceB.java depends on UserDAO.java
	assert.Less(t, nodeLevelMap[userDAOPath], nodeLevelMap[userServiceBPath], "UserDAO should be at an earlier level than UserServiceB")

	// Test case with a cycle (to ensure GetExecutionOrder returns nil if cycle persists)
	// Create a temporary directory for testing cyclic graph
	tmpDirCycle, err := os.MkdirTemp("", "java_assessment_test_cycle")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDirCycle)

	cyclicAContent := `
package com.example.cycle;
import com.example.cycle.B;
public class A { B b; }
`
	cyclicBContent := `
package com.example.cycle;
import com.example.cycle.A;
public class B { A a; }
`
	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDirCycle, "com", "example", "cycle"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDirCycle, "com", "example", "cycle", "A.java"), []byte(cyclicAContent), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDirCycle, "com", "example", "cycle", "B.java"), []byte(cyclicBContent), 0644))

	// For cyclic graph, the `RemoveCycle` in `getDependencyGraph` should break the cycle.
	// So GetExecutionOrder should still return a sorted list.
	// We expect the graph to be handled by RemoveCycle.
	G_cycle, sortedTasks_cycle := analyzer.GetExecutionOrder(tmpDirCycle)
	assert.NotNil(t, G_cycle, "Expected non-nil graph even with initial cycle (after removal)")
	assert.NotNil(t, sortedTasks_cycle, "Expected non-nil sorted tasks even with initial cycle (after removal)")

	// Verify that the cycle is broken. The graph should become acyclic.
	// We expect one of the A-B or B-A dependencies to be removed.
	aPath := filepath.Join(tmpDirCycle, "com", "example", "cycle", "A.java")
	bPath := filepath.Join(tmpDirCycle, "com", "example", "cycle", "B.java")

	_, aToBExists := G_cycle[aPath][bPath]
	_, bToAExists := G_cycle[bPath][aPath]

	// Assert that not both edges exist (i.e., at least one was removed)
	assert.True(t, aToBExists != bToAExists, "Expected one edge of the cycle to be removed")
	// Also, ensure that both A and B are still present in the graph
	assert.Contains(t, G_cycle, aPath)
	assert.Contains(t, G_cycle, bPath)

	// Verify topological sort property after cycle removal
	nodeLevelMapCycle := make(map[string]int)
	for levelIdx, level := range sortedTasks_cycle {
		for _, node := range level {
			nodeLevelMapCycle[node] = levelIdx
		}
	}
	for node, deps := range G_cycle {
		for dep := range deps {
			assert.Less(t, nodeLevelMapCycle[dep], nodeLevelMapCycle[node], "Dependency should be at an earlier level")
		}
	}
}

func Test_resolveClassDependencies(t *testing.T) {
	// Setup mock data for classToFileInfosMap
	mockClassToFileInfosMap := map[string][]*JavaFileParsedInfo{
		"UserDAO": {
			{FilePath: "/project/data/UserDAO.java", Package: "com.example.data", DeclaredClasses: []string{"UserDAO"}},
		},
		"UserService": {
			{FilePath: "/project/service/UserService.java", Package: "com.example.service", DeclaredClasses: []string{"UserService"}},
		},
		"com.example.data.UserDAO": { // Fully qualified name mapping
			{FilePath: "/project/data/UserDAO.java", Package: "com.example.data", DeclaredClasses: []string{"UserDAO"}},
		},
		"com.example.service.UserService": { // Fully qualified name mapping
			{FilePath: "/project/service/UserService.java", Package: "com.example.service", DeclaredClasses: []string{"UserService"}},
		},
	}

	tests := []struct {
		name                 string
		classToFileInfosMap  map[string][]*JavaFileParsedInfo
		sourcePackage        string
		referencedClasses    []string
		referencedClassIndex int
		want                 string
	}{
		{
			name:                 "Resolve non-fully qualified class in the same package",
			classToFileInfosMap:  mockClassToFileInfosMap,
			sourcePackage:        "com.example.service",
			referencedClasses:    []string{"UserService"},
			referencedClassIndex: 0,
			want:                 "/project/service/UserService.java",
		},
		{
			name:                 "Resolve non-fully qualified class from another package (no specific import, assumes current package only)",
			classToFileInfosMap:  mockClassToFileInfosMap,
			sourcePackage:        "com.example.app", // Different package
			referencedClasses:    []string{"UserDAO"},
			referencedClassIndex: 0,
			want:                 "", // Should not resolve without explicit import/FQN in referencedClasses logic
		},
		{
			name:                 "Resolve fully qualified class name",
			classToFileInfosMap:  mockClassToFileInfosMap,
			sourcePackage:        "com.example.app",
			referencedClasses:    []string{"com", "example", "data", "UserDAO"},
			referencedClassIndex: 3, // Index of "UserDAO" in the fully qualified name
			want:                 "/project/data/UserDAO.java",
		},
		{
			name:                 "Resolve ambiguous class name (should pick one if logic relies on first match)",
			classToFileInfosMap:  mockClassToFileInfosMap,
			sourcePackage:        "com.example.app",
			referencedClasses:    []string{"Builder"},
			referencedClassIndex: 0,
			want:                 "", // Should not resolve as it's ambiguous and not in the same package or FQN
		},
		{
			name:                 "Class not found in map",
			classToFileInfosMap:  mockClassToFileInfosMap,
			sourcePackage:        "com.example.app",
			referencedClasses:    []string{"NonExistentClass"},
			referencedClassIndex: 0,
			want:                 "",
		},
		{
			name:                 "Referenced class is a package (prefix match)",
			classToFileInfosMap:  mockClassToFileInfosMap,
			sourcePackage:        "com.example.app",
			referencedClasses:    []string{"com.example.data.UserDAO"},
			referencedClassIndex: 0,
			want:                 "/project/data/UserDAO.java", // It picks the first one found in map for "com.example.data"
		},
		{
			name:                 "Referenced class is a package, but no matching file exists",
			classToFileInfosMap:  map[string][]*JavaFileParsedInfo{},
			sourcePackage:        "com.example.app",
			referencedClasses:    []string{"com.example.nonexistent"},
			referencedClassIndex: 0,
			want:                 "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveClassDependencies(tt.classToFileInfosMap, tt.sourcePackage, tt.referencedClasses, tt.referencedClassIndex)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_isResolvedClassNameEqual(t *testing.T) {
	tests := []struct {
		name                 string
		referencedClasses    []string
		referencedClassIndex int
		targetPackage        string
		isPackage            bool
		want                 bool
	}{
		{
			name:                 "Match simple class name in package",
			referencedClasses:    []string{"com", "example", "data", "UserDAO"},
			referencedClassIndex: 3,
			targetPackage:        "com.example.data",
			isPackage:            false,
			want:                 true,
		},
		{
			name:                 "No match - different class name",
			referencedClasses:    []string{"com", "example", "data", "UserDAO"},
			referencedClassIndex: 3,
			targetPackage:        "com.example.service",
			isPackage:            false,
			want:                 false,
		},
		{
			name:                 "No match - incorrect package prefix",
			referencedClasses:    []string{"com", "example", "data", "UserDAO"},
			referencedClassIndex: 3,
			targetPackage:        "com.wrong.data",
			isPackage:            false,
			want:                 false,
		},
		{
			name:                 "Match with shorter package and class",
			referencedClasses:    []string{"example", "data", "UserDAO"}, // Assuming "com" is implied or not present
			referencedClassIndex: 2,
			targetPackage:        "example.data",
			isPackage:            false,
			want:                 true,
		},
		{
			name:                 "Class is a package (should return false)",
			referencedClasses:    []string{"com.example.data"},
			referencedClassIndex: 0,
			targetPackage:        "com.example.data",
			isPackage:            true,
			want:                 false,
		},
		{
			name:                 "Referenced class index out of bounds (at start)",
			referencedClasses:    []string{"UserDAO"},
			referencedClassIndex: 0, // packageStartIndex will be negative
			targetPackage:        "com.example.data",
			isPackage:            false,
			want:                 false,
		},
		{
			name:                 "Referenced class index out of bounds (too large)",
			referencedClasses:    []string{"UserDAO"},
			referencedClassIndex: 1,
			targetPackage:        "com.example.data",
			isPackage:            false,
			want:                 false,
		},
		{
			name:                 "Complex package structure, match",
			referencedClasses:    []string{"org", "springframework", "boot", "Application"},
			referencedClassIndex: 3,
			targetPackage:        "org.springframework.boot",
			isPackage:            false,
			want:                 true,
		},
		{
			name:                 "Complex package structure, no match",
			referencedClasses:    []string{"org", "springframework", "boot", "Application"},
			referencedClassIndex: 3,
			targetPackage:        "org.springframework.data",
			isPackage:            false,
			want:                 false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isResolvedClassNameEqual(tt.referencedClasses, tt.referencedClassIndex, tt.targetPackage, tt.isPackage)
			assert.Equal(t, tt.want, got, fmt.Sprintf("isResolvedClassNameEqual(%v, %d, %q, %v) got %v, want %v", tt.referencedClasses, tt.referencedClassIndex, tt.targetPackage, tt.isPackage, got, tt.want))
		})
	}
}

func Test_isPackage(t *testing.T) {
	tests := []struct {
		name      string
		reference string
		want      bool
	}{
		{
			name:      "Contains dot (is package)",
			reference: "com.example.data",
			want:      true,
		},
		{
			name:      "Does not contain dot (is not package)",
			reference: "UserDAO",
			want:      false,
		},
		{
			name:      "Empty string",
			reference: "",
			want:      false,
		},
		{
			name:      "Just a dot",
			reference: ".",
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref := tt.reference
			got := isPackage(&ref)
			assert.Equal(t, tt.want, got, fmt.Sprintf("isPackage(%q) got %v, want %v", tt.reference, got, tt.want))
		})
	}
}

func Test_fetchFileParsedInfo(t *testing.T) {
	// Create a temporary directory and a dummy Java file
	tmpDir, err := os.MkdirTemp("", "java_parser_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	javaFilePath := filepath.Join(tmpDir, "MyClass.java")
	javaContent := `
package com.example.app;

import java.util.List;

public class MyClass {
    private String name;
    public interface MyInnerInterface {}
    public class MyInnerClass {}
    public MyClass() {}
    public void doSomething() {
        List<String> items;
    }
}

interface AnotherInterface {
	void someMethod();
}
`
	err = os.WriteFile(javaFilePath, []byte(javaContent), 0644)
	assert.NoError(t, err)

	ctx := context.Background()
	parser := sitter.NewParser()
	defer parser.Close()
	parser.SetLanguage(java.GetLanguage())

	parsedInfo, err := fetchFileParsedInfo(ctx, parser, javaFilePath, tmpDir)
	assert.NoError(t, err)
	assert.NotNil(t, parsedInfo)

	assert.Equal(t, "MyClass.java", parsedInfo.FileName)
	assert.Equal(t, javaFilePath, parsedInfo.FilePath)
	assert.Equal(t, "com.example.app", parsedInfo.Package)
	assert.Contains(t, parsedInfo.DeclaredClasses, "MyClass")
	assert.Contains(t, parsedInfo.DeclaredClasses, "MyInnerInterface")
	assert.Contains(t, parsedInfo.DeclaredClasses, "MyInnerClass")
	assert.Contains(t, parsedInfo.DeclaredClasses, "AnotherInterface")
	assert.Equal(t, []byte(javaContent), parsedInfo.FileContent)

	// Test with non-existent file
	_, err = fetchFileParsedInfo(ctx, parser, filepath.Join(tmpDir, "NonExistent.java"), tmpDir)
	assert.Error(t, err)
	// On Linux: "no such file or directory"
	// On Windows: "The system cannot find the file specified"
	// We check for "no such file" OR "cannot find the file" to cover both.
	assert.True(t, strings.Contains(err.Error(), "no such file") || strings.Contains(err.Error(), "cannot find the file"), "Error message should indicate file not found: %v", err)

	// Test with malformed Java content (e.g., syntax error, though parser might still return something)
	malformedContent := `package com.example; public class Malformed { int x; ` // Missing closing brace
	malformedFilePath := filepath.Join(tmpDir, "Malformed.java")
	err = os.WriteFile(malformedFilePath, []byte(malformedContent), 0644)
	assert.NoError(t, err)

	parsedInfoMalformed, err := fetchFileParsedInfo(ctx, parser, malformedFilePath, tmpDir)
	assert.NoError(t, err) // Tree-sitter might still parse partially without error
	assert.NotNil(t, parsedInfoMalformed)
	assert.Equal(t, "Malformed.java", parsedInfoMalformed.FileName)
	assert.Equal(t, "com.example", parsedInfoMalformed.Package)
	assert.Contains(t, parsedInfoMalformed.DeclaredClasses, "Malformed")
}

func Test_fetchClassReferences(t *testing.T) {
	ctx := context.Background()
	parser := sitter.NewParser()
	defer parser.Close()
	parser.SetLanguage(java.GetLanguage())

	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name: "Basic class references",
			content: `
package com.example.app;
import java.util.List;
import com.example.model.User;

public class UserService {
    private UserDAO userDAO;
    public void process(User user, List<String> names) {
        AnotherClass ac = new AnotherClass();
    }
}
`,
			want: []string{"java.util.List", "com.example.model.User", "UserDAO", "AnotherClass"},
		},
		{
			name: "No class references",
			content: `
package com.example.app;
public class SimpleClass {}
`,
			want: []string{},
		},
		{
			name: "References with primitive types and void (should be filtered)",
			content: `
package com.example.app;
public class DataTypes {
    int a;
    double b;
    void doNothing() {}
    boolean isActive;
}
`,
			want: []string{},
		},
		{
			name: "References in method signatures and parameters",
			content: `
package com.example.app;
import com.example.dto.Product;
public class Store {
    public Product getProductById(int id) { return null; }
    private OrderService orderService;
}
`,
			want: []string{"Product", "com.example.dto.Product", "OrderService"},
		},
		{
			name:    "Empty content",
			content: ``,
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fetchClassReferences(ctx, parser, []byte(tt.content))
			assert.NoError(t, err)
			for _, x := range tt.want {
				assert.Contains(t, got, x)
			}
		})
	}
}
func Test_fetchClassDeclaration(t *testing.T) {
	ctx := context.Background()
	parser := sitter.NewParser()
	defer parser.Close()
	parser.SetLanguage(java.GetLanguage())

	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name: "Single class declaration",
			content: `
package com.example.app;
public class MyClass {}
`,
			want: []string{"MyClass"},
		},
		{
			name: "Multiple class declarations",
			content: `
public class ClassA {}
class ClassB {}
public final class ClassC {}
`,
			want: []string{"ClassA", "ClassB", "ClassC"},
		},
		{
			name: "Interface declaration",
			content: `
public interface MyInterface {}
`,
			want: []string{"MyInterface"},
		},
		{
			name: "Class and interface declarations",
			content: `
public class MyClass {}
interface MyInterface {}
`,
			want: []string{"MyClass", "MyInterface"},
		},
		{
			name: "Nested classes and interfaces",
			content: `
public class OuterClass {
    public class InnerClass {}
    private interface InnerInterface {}
}
`,
			want: []string{"OuterClass", "InnerClass", "InnerInterface"},
		},
		{
			name:    "Empty content",
			content: ``,
			want:    []string{},
		},
		{
			name: "No class or interface declarations (e.g., just imports)",
			content: `
import java.util.List;
import java.util.Map;
`,
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := parser.ParseCtx(ctx, nil, []byte(tt.content))
			assert.NoError(t, err)
			rootNode := tree.RootNode()

			got, err := fetchClassDeclaration(rootNode, []byte(tt.content))
			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got) // Use ElementsMatch for order-independent comparison
		})
	}
}

func Test_fetchedFileClassPackageMap(t *testing.T) {
	// Create a temporary directory and dummy Java files
	tmpDir, err := os.MkdirTemp("", "java_file_map_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create directories and files
	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "src", "com", "example", "data"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "src", "com", "example", "data", "User.java"), []byte(`
		package com.example.data;
		public class User {}
	`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "src", "com", "example", "data", "Product.java"), []byte(`
		package com.example.data;
		public class Product {}
	`), 0644))

	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "src", "com", "example", "service"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "src", "com", "example", "service", "OrderService.java"), []byte(`
		package com.example.service;
		import com.example.data.Product;
		public class OrderService {}
	`), 0644))

	// Create a file with a duplicate class name in a different package
	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "src", "com", "another", "data"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "src", "com", "another", "data", "User.java"), []byte(`
		package com.another.data;
		public class User {}
	`), 0644))

	ctx := context.Background()
	parser := sitter.NewParser()
	defer parser.Close()
	parser.SetLanguage(java.GetLanguage())

	classToFileInfosMap, fileInfoPathMap, err := fetchedFileClassPackageMap(ctx, parser, tmpDir)
	assert.NoError(t, err)
	assert.NotNil(t, classToFileInfosMap)
	assert.NotNil(t, fileInfoPathMap)

	// Verify fileInfoPathMap
	userPath1 := filepath.Join(tmpDir, "src", "com", "example", "data", "User.java")
	productPath := filepath.Join(tmpDir, "src", "com", "example", "data", "Product.java")
	orderServicePath := filepath.Join(tmpDir, "src", "com", "example", "service", "OrderService.java")
	userPath2 := filepath.Join(tmpDir, "src", "com", "another", "data", "User.java")

	assert.Contains(t, fileInfoPathMap, userPath1)
	assert.Equal(t, "com.example.data", fileInfoPathMap[userPath1].Package)
	assert.Contains(t, fileInfoPathMap[userPath1].DeclaredClasses, "User")

	assert.Contains(t, fileInfoPathMap, productPath)
	assert.Equal(t, "com.example.data", fileInfoPathMap[productPath].Package)
	assert.Contains(t, fileInfoPathMap[productPath].DeclaredClasses, "Product")

	assert.Contains(t, fileInfoPathMap, orderServicePath)
	assert.Equal(t, "com.example.service", fileInfoPathMap[orderServicePath].Package)
	assert.Contains(t, fileInfoPathMap[orderServicePath].DeclaredClasses, "OrderService")

	assert.Contains(t, fileInfoPathMap, userPath2)
	assert.Equal(t, "com.another.data", fileInfoPathMap[userPath2].Package)
	assert.Contains(t, fileInfoPathMap[userPath2].DeclaredClasses, "User")

	// Verify classToFileInfosMap
	assert.Contains(t, classToFileInfosMap, "User")
	assert.Len(t, classToFileInfosMap["User"], 2)
	assert.Contains(t, []string{classToFileInfosMap["User"][0].FilePath, classToFileInfosMap["User"][1].FilePath}, userPath1)
	assert.Contains(t, []string{classToFileInfosMap["User"][0].FilePath, classToFileInfosMap["User"][1].FilePath}, userPath2)

	assert.Contains(t, classToFileInfosMap, "Product")
	assert.Len(t, classToFileInfosMap["Product"], 1)
	assert.Equal(t, productPath, classToFileInfosMap["Product"][0].FilePath)

	assert.Contains(t, classToFileInfosMap, "OrderService")
	assert.Len(t, classToFileInfosMap["OrderService"], 1)
	assert.Equal(t, orderServicePath, classToFileInfosMap["OrderService"][0].FilePath)

	// Verify fully qualified names
	assert.Contains(t, classToFileInfosMap, "com.example.data.User")
	assert.Len(t, classToFileInfosMap["com.example.data.User"], 1)
	assert.Equal(t, userPath1, classToFileInfosMap["com.example.data.User"][0].FilePath)

	assert.Contains(t, classToFileInfosMap, "com.another.data.User")
	assert.Len(t, classToFileInfosMap["com.another.data.User"], 1)
	assert.Equal(t, userPath2, classToFileInfosMap["com.another.data.User"][0].FilePath)

	assert.Contains(t, classToFileInfosMap, "com.example.data.Product")
	assert.Len(t, classToFileInfosMap["com.example.data.Product"], 1)
	assert.Equal(t, productPath, classToFileInfosMap["com.example.data.Product"][0].FilePath)

	// Test with empty directory
	emptyDir, err := os.MkdirTemp("", "java_empty_dir_test")
	assert.NoError(t, err)
	defer os.RemoveAll(emptyDir)

	emptyClassToFileInfosMap, emptyFileInfoPathMap, err := fetchedFileClassPackageMap(ctx, parser, emptyDir)
	assert.NoError(t, err)
	assert.Empty(t, emptyClassToFileInfosMap)
	assert.Empty(t, emptyFileInfoPathMap)

	// Test with directory containing non-java files
	nonJavaDir, err := os.MkdirTemp("", "java_non_java_test")
	assert.NoError(t, err)
	defer os.RemoveAll(nonJavaDir)
	assert.NoError(t, os.WriteFile(filepath.Join(nonJavaDir, "readme.txt"), []byte("hello"), 0644))

	nonJavaClassToFileInfosMap, nonJavaFileInfoPathMap, err := fetchedFileClassPackageMap(ctx, parser, nonJavaDir)
	assert.NoError(t, err)
	assert.Empty(t, nonJavaClassToFileInfosMap)
	assert.Empty(t, nonJavaFileInfoPathMap)

	// Test with a non-existent directory
	nonExistentDir := filepath.Join(tmpDir, "non_existent")
	_, _, err = fetchedFileClassPackageMap(ctx, parser, nonExistentDir)
	assert.Error(t, err)
}

func TestJavaDependencyAnalyzer_getDependencyGraph(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "java_get_dep_graph")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create dummy Java files with dependencies
	// A.java -> B.java
	// B.java -> C.java
	// C.java
	// D.java -> B.java
	// E.java (no dependencies)
	// F.java -> G.java (cycle to be removed by RemoveCycle)
	// G.java -> F.java

	cContent := `
package com.example.deps;
public class C {}
`
	bContent := `
package com.example.deps;
import com.example.deps.C;
public class B { C c; }
`
	aContent := `
package com.example.deps;
import com.example.deps.B;
public class A { B b; }
`
	dContent := `
package com.example.deps;
import com.example.deps.B;
public class D { B b; }
`
	eContent := `
package com.example.deps;
public class E {}
`
	fContent := `
package com.example.cycle;
import com.example.cycle.G;
public class F { G g; }
`
	gContent := `
package com.example.cycle;
import com.example.cycle.F;
public class G { F f; }
`

	// Create directories and files
	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "com", "example", "deps"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "deps", "C.java"), []byte(cContent), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "deps", "B.java"), []byte(bContent), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "deps", "A.java"), []byte(aContent), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "deps", "D.java"), []byte(dContent), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "deps", "E.java"), []byte(eContent), 0644))

	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "com", "example", "cycle"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "cycle", "F.java"), []byte(fContent), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(tmpDir, "com", "example", "cycle", "G.java"), []byte(gContent), 0644))

	analyzer := &JavaDependencyAnalyzer{ctx: context.Background()}
	dependencyGraph := analyzer.getDependencyGraph(tmpDir)

	assert.NotNil(t, dependencyGraph)

	// Expected file paths
	aPath := filepath.Join(tmpDir, "com", "example", "deps", "A.java")
	bPath := filepath.Join(tmpDir, "com", "example", "deps", "B.java")
	cPath := filepath.Join(tmpDir, "com", "example", "deps", "C.java")
	dPath := filepath.Join(tmpDir, "com", "example", "deps", "D.java")
	ePath := filepath.Join(tmpDir, "com", "example", "deps", "E.java")
	fPath := filepath.Join(tmpDir, "com", "example", "cycle", "F.java")
	gPath := filepath.Join(tmpDir, "com", "example", "cycle", "G.java")

	// Verify direct dependencies
	assert.Contains(t, dependencyGraph, aPath)
	assert.Contains(t, dependencyGraph[aPath], bPath)
	assert.Len(t, dependencyGraph[aPath], 1)

	assert.Contains(t, dependencyGraph, bPath)
	assert.Contains(t, dependencyGraph[bPath], cPath)
	assert.Len(t, dependencyGraph[bPath], 1)

	assert.Contains(t, dependencyGraph, cPath)
	assert.Empty(t, dependencyGraph[cPath])

	assert.Contains(t, dependencyGraph, dPath)
	assert.Contains(t, dependencyGraph[dPath], bPath)
	assert.Len(t, dependencyGraph[dPath], 1)

	assert.Contains(t, dependencyGraph, ePath)
	assert.Empty(t, dependencyGraph[ePath])

	// Verify cycle removal for F and G
	assert.Contains(t, dependencyGraph, fPath)
	assert.Contains(t, dependencyGraph, gPath)
	// One of the edges (F->G or G->F) should be removed
	_, fToGExists := dependencyGraph[fPath][gPath]
	_, gToFExists := dependencyGraph[gPath][fPath]
	assert.True(t, fToGExists != gToFExists, "Expected one edge of the cycle F<->G to be removed")

	// Ensure all relevant files are keys in the graph
	expectedNodes := []string{aPath, bPath, cPath, dPath, ePath, fPath, gPath}
	assert.Len(t, dependencyGraph, len(expectedNodes))
	for _, node := range expectedNodes {
		assert.Contains(t, dependencyGraph, node)
	}

	// Test with an empty directory
	emptyDir, err := os.MkdirTemp("", "java_empty_get_dep_graph")
	assert.NoError(t, err)
	defer os.RemoveAll(emptyDir)
	emptyGraph := analyzer.getDependencyGraph(emptyDir)
	assert.Empty(t, emptyGraph)

	// Test with non-existent directory (logs error and returns empty map)
	nonExistentDir := filepath.Join(tmpDir, "non_existent")

	core, observedLogs := observer.New(zap.ErrorLevel)
	logger.Log = zap.New(core)

	errorGraph := analyzer.getDependencyGraph(nonExistentDir)
	assert.Empty(t, errorGraph)
	logs := observedLogs.All()
	if assert.NotEmpty(t, logs, "Expected error logs for non-existent directory") {
		assert.Contains(t, logs[0].Message, "Error walking the directory")
	}
}
