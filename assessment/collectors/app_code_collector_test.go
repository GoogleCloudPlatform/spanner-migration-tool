package assessment

import (
	"os"
	"path/filepath"
	"testing"

	dependencyAnalyzer "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/project_analyzer"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// init silences logs during tests.
func init() {
	logger.Log = zap.NewNop()
}

// --- Minimal Mock for Dependency Analyzer ---

type minimalMockAnalyzer struct {
	dependencyAnalyzer.DependencyAnalyzer // Embed interface to satisfy it
	IsDAOResult                           bool
	FrameworkResult                       string
	// ERROR FIX: Added a function field to allow custom logic per test.
	GetFrameworkFromFileContentFunc func(fileContent string) string
}

func (m *minimalMockAnalyzer) IsDAO(filePath, fileContent string) bool {
	return m.IsDAOResult
}

// ERROR FIX: The method now calls the function field if it's defined.
func (m *minimalMockAnalyzer) GetFrameworkFromFileContent(fileContent string) string {
	if m.GetFrameworkFromFileContentFunc != nil {
		return m.GetFrameworkFromFileContentFunc(fileContent)
	}
	return m.FrameworkResult
}

// --- Unit Tests for Helper Functions ---

func TestIsProgrammingLanguageSupported(t *testing.T) {
	supported := map[string]bool{"go": true, "java": true}
	assert.True(t, isProgrammingLanguageSupported("go", supported), "Should be true for supported language 'go'")
	assert.True(t, isProgrammingLanguageSupported("java", supported), "Should be true for supported language 'java'")
	assert.False(t, isProgrammingLanguageSupported("python", supported), "Should be false for unsupported language 'python'")
	assert.True(t, isProgrammingLanguageSupported("Go", supported), "Should be true for 'Go' (case-insensitive)")
}

func TestIsFrameworkCombinationSupported(t *testing.T) {
	supported := map[FrameworkPair]bool{
		{Source: "jdbc", Target: "jdbc"}:                            true,
		{Source: "go-sql-mysql", Target: "go-sql-spanner"}:          true,
		{Source: "go-sql-driver/mysql", Target: "go-sql-spanner"}:   true,
		{Source: "vertx-mysql-client", Target: "vertx-jdbc-client"}: true,
	}

	assert.True(t, isFrameworkCombinationSupported("jdbc", "jdbc", supported), "Should be true for supported pair")
	assert.True(t, isFrameworkCombinationSupported("JDBC", "JDBC", supported), "Should be true for supported pair (case-insensitive)")
	assert.False(t, isFrameworkCombinationSupported("jdbc", "go-sql-spanner", supported), "Should be false for unsupported pair")
	assert.True(t, isFrameworkCombinationSupported("go-sql-driver/mysql", "go-sql-spanner", supported), "Should be true for supported pair (case-insensitive)")
}

func TestDetectProgrammingLanguage(t *testing.T) {
	tempDir := t.TempDir()

	javaDir := filepath.Join(tempDir, "java_proj")
	os.Mkdir(javaDir, 0755)
	os.WriteFile(filepath.Join(javaDir, "main.java"), []byte("class Main {}"), 0644)
	os.WriteFile(filepath.Join(javaDir, "util.java"), []byte("class Util {}"), 0644)
	os.WriteFile(filepath.Join(javaDir, "helper.go"), []byte("package main"), 0644)
	assert.Equal(t, "java", detectProgrammingLanguage(javaDir), "Should detect 'java' as dominant language")

	emptyDir := filepath.Join(tempDir, "empty_proj")
	os.Mkdir(emptyDir, 0755)
	assert.Equal(t, "", detectProgrammingLanguage(emptyDir), "Should return empty string for empty directory")
}

func TestGetDatabaseSourceFramework(t *testing.T) {
	tempDir := t.TempDir()
	os.WriteFile(filepath.Join(tempDir, "file1.java"), []byte("content for spring"), 0644)
	os.WriteFile(filepath.Join(tempDir, "file2.java"), []byte("content for jdbc"), 0644)
	os.WriteFile(filepath.Join(tempDir, "file3.java"), []byte("content for jdbc"), 0644)

	mockAnalyzer := &minimalMockAnalyzer{}
	// ERROR FIX: Assigning the custom function to the new field, not the method.
	mockAnalyzer.GetFrameworkFromFileContentFunc = func(fileContent string) string {
		if fileContent == "content for spring" {
			return "spring-jdbc"
		}
		if fileContent == "content for jdbc" {
			return "jdbc"
		}
		return ""
	}

	dominantFramework := GetDatabaseSourceFramework(tempDir, ".java", mockAnalyzer)
	assert.Equal(t, "jdbc", dominantFramework, "JDBC should be dominant with two occurrences")
}

func TestGetPromptForDAOClass(t *testing.T) {
	summarizer := &MigrationCodeSummarizer{
		sourceDatabaseFramework: "GO-SQL-MYSQL",
		targetDatabaseFramework: "GO-SQL-SPANNER",
	}
	content := "type a struct{}"
	filepath := "/app/models/user.go"
	methodChanges := `[{"method": "bar"}]`
	oldSchema := "CREATE TABLE users(...)"
	newSchema := "CREATE TABLE users_new(...)"

	prompt := summarizer.getPromptForDAOClass(content, filepath, &methodChanges, &oldSchema, &newSchema)

	assert.Contains(t, prompt, content)
	assert.Contains(t, prompt, oldSchema)
	assert.Contains(t, prompt, "GO-SQL-SPANNER")
}

func TestGetPromptForNonDAOClass(t *testing.T) {
	summarizer := &MigrationCodeSummarizer{
		sourceDatabaseFramework: "JDBC",
		targetDatabaseFramework: "SPANNER_JDBC",
	}
	content := "public class MyService {}"
	filepath := "/src/com/test/MyService.java"
	methodChanges := `[{"method": "foo"}]`

	prompt := summarizer.getPromptForNonDAOClass(content, filepath, &methodChanges)

	assert.Contains(t, prompt, content)
	assert.Contains(t, prompt, methodChanges)
	assert.Contains(t, prompt, "SPANNER_JDBC")
}

func TestExtractPublicMethodSignatures(t *testing.T) {
	summarizer := &MigrationCodeSummarizer{}

	validJSON := `{"method_signature_changes": [{"name": "methodA"}, {"name": "methodB"}]}`
	signatures, err := summarizer.extractPublicMethodSignatures(validJSON)
	assert.NoError(t, err)
	assert.Len(t, signatures, 2)
	assert.Equal(t, "methodA", signatures[0].(map[string]any)["name"])

	malformedJSON := `{"method_signature_changes": [`
	_, err = summarizer.extractPublicMethodSignatures(malformedJSON)
	assert.Error(t, err)

	missingKeyJSON := `{"other_key": "value"}`
	_, err = summarizer.extractPublicMethodSignatures(missingKeyJSON)
	assert.Error(t, err)
}

func TestFormatQuestionsAndSearchResults(t *testing.T) {
	questions := []string{"How to connect?", "How to write?"}
	searchResults := [][]string{
		{"Use Connection A.", "Use Connection B."},
		{"Use Write-Op C."},
	}

	formatted := formatQuestionsAndSearchResults(questions, searchResults)
	assert.Contains(t, formatted, "* **Question 1:** How to connect?")
	assert.Contains(t, formatted, "* **Potential Solution 1:** Use Connection A.")
	assert.Contains(t, formatted, "* **Potential Solution 2:** Use Connection B.")
	assert.Contains(t, formatted, "* **Question 2:** How to write?")
	assert.Contains(t, formatted, "* **Potential Solution 1:** Use Write-Op C.")
}

func TestAnalyzeFileDependencies(t *testing.T) {
	mockDAOAnalyzer := &minimalMockAnalyzer{IsDAOResult: true}
	mockNonDAOAnalyzer := &minimalMockAnalyzer{IsDAOResult: false}

	s1 := &MigrationCodeSummarizer{projectDependencyAnalyzer: mockDAOAnalyzer}
	isDep, _ := s1.analyzeFileDependencies("file.dao", "")
	assert.True(t, isDep, "Should be true if file is DAO")

	s2 := &MigrationCodeSummarizer{
		projectDependencyAnalyzer: mockNonDAOAnalyzer,
		dependencyGraph: map[string]map[string]struct{}{
			"fileA": {"fileB_is_DAO": {}},
		},
		fileDependencyAnalysis: map[string]FileDependencyInfo{
			"fileB_is_DAO": {IsDAODependent: true},
		},
	}
	isDep, _ = s2.analyzeFileDependencies("fileA", "")
	assert.True(t, isDep, "Should be true if a dependency is DAO-dependent")

	s3 := &MigrationCodeSummarizer{
		projectDependencyAnalyzer: mockNonDAOAnalyzer,
		dependencyGraph: map[string]map[string]struct{}{
			"fileA": {"fileC_not_DAO": {}},
		},
		fileDependencyAnalysis: map[string]FileDependencyInfo{
			"fileC_not_DAO": {IsDAODependent: false},
		},
	}
	isDep, _ = s3.analyzeFileDependencies("fileA", "")
	assert.False(t, isDep, "Should be false if no dependencies are DAO-dependent")
}
