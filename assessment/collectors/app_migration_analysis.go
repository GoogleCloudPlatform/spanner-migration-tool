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
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/task"

	"cloud.google.com/go/vertexai/genai"
	assessment "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/embeddings"
	parser "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/parser"
	dependencyAnalyzer "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/project_analyzer"
	. "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
)

//go:embed prompts/analyze-code-prompt.txt
var AnalyzeCodePromptTemplate string

//go:embed prompts/dao-migration-prompt.txt
var DAOMigrationPromptTemplate string

//go:embed prompts/non-dao-migration-prompt.txt
var NonDAOMigrationPromptTemplate string

// MigrationSummarizer holds the LLM models and example databases
type MigrationSummarizer struct {
	projectID                     string
	location                      string
	client                        *genai.Client
	modelPro                      *genai.GenerativeModel
	modelFlash                    *genai.GenerativeModel
	conceptExampleDB              *assessment.MysqlConceptDb
	sourceFramework               string
	targetFramework               string
	dependencyAnalyzer            dependencyAnalyzer.DependencyAnalyzer
	sourceSchema                  string
	targetSchema                  string
	projectPath                   string
	dependencyGraph               map[string]map[string]struct{}
	fileDependencyAnalysisDataMap map[string]FileDependencyAnalysisData
}

type FileDependencyAnalysisData struct {
	publicSignatures []any
	isDaoDependent   bool
}

// AnalyzeFileResponse response from analyzing single file.
type AnalyzeFileResponse struct {
	CodeAssessment  *CodeAssessment
	methodSignature []any
	projectPath     string
	filePath        string
}

// AnalyzeFileInput input for analyzing file.
type AnalyzeFileInput struct {
	ctx           context.Context
	projectPath   string
	filepath      string
	methodChanges string
	content       string
	fileIndex     int
}

type AskQuestionsOutput struct {
	Questions []string `json:"questions"`
}

const JsonParserRetry = 3

// NewMigrationSummarizer initializes a new MigrationSummarizer
func NewMigrationSummarizer(ctx context.Context, googleGenerativeAIAPIKey *string, projectID, location, sourceSchema, targetSchema, projectPath, language string) (*MigrationSummarizer, error) {
	if googleGenerativeAIAPIKey != nil {
		os.Setenv("GOOGLE_API_KEY", *googleGenerativeAIAPIKey)
	}

	client, err := genai.NewClient(ctx, projectID, location)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vertex AI client: %w", err)
	}

	conceptExampleDB, err := assessment.NewMysqlConceptDb(projectID, location, language)
	if err != nil {
		return nil, fmt.Errorf("failed to load code example DB: %w", err)
	}

	var sourceFramework, targetFramework string

	//Todo: Either derive it or take it as input
	switch language {
	case "go":
		sourceFramework = "go-sql-mysql"
		targetFramework = "go-sql-spanner"
	case "java":
		sourceFramework = "JDBC"
		targetFramework = "JDBC"
	default:
		panic("unsupported language")
	}

	m := &MigrationSummarizer{
		projectID:                     projectID,
		location:                      location,
		client:                        client,
		modelPro:                      client.GenerativeModel("gemini-1.5-pro-002"),
		modelFlash:                    client.GenerativeModel("gemini-2.0-flash-001"),
		conceptExampleDB:              conceptExampleDB,
		dependencyAnalyzer:            dependencyAnalyzer.AnalyzerFactory(language, ctx),
		sourceSchema:                  sourceSchema,
		sourceFramework:               sourceFramework,
		targetFramework:               targetFramework,
		targetSchema:                  targetSchema,
		projectPath:                   projectPath,
		dependencyGraph:               make(map[string]map[string]struct{}),
		fileDependencyAnalysisDataMap: make(map[string]FileDependencyAnalysisData),
	}
	m.modelFlash.ResponseMIMEType = "application/json"
	m.modelPro.ResponseMIMEType = "application/json"

	return m, nil
}

func (m *MigrationSummarizer) MigrationCodeConversionInvoke(
	ctx context.Context,
	originalPrompt, sourceCode, olderSchema, newSchema, identifier string,
) (string, error) {
	prompt := AnalyzeCodePromptTemplate
	prompt = strings.ReplaceAll(prompt, "{{SOURCE_FRAMEWORK}}", m.sourceFramework)
	prompt = strings.ReplaceAll(prompt, "{{TARGET_FRAMEWORK}}", m.targetFramework)
	prompt = strings.ReplaceAll(prompt, "{{SOURCE_CODE}}", sourceCode)
	prompt = strings.ReplaceAll(prompt, "{{OLDER_SCHEMA}}", olderSchema)
	prompt = strings.ReplaceAll(prompt, "{{NEW_SCHEMA}}", newSchema)

	resp, err := m.modelFlash.GenerateContent(ctx, genai.Text(prompt))
	if err != nil {
		return "", err
	}
	logger.Log.Debug("Token: ",
		zap.Int32("Prompt Token:", resp.UsageMetadata.PromptTokenCount),
		zap.Int32("Candidate Token:", resp.UsageMetadata.CandidatesTokenCount),
		zap.Int32("Total Token:", resp.UsageMetadata.TotalTokenCount))

	var response string
	if p, ok := resp.Candidates[0].Content.Parts[0].(genai.Text); ok {
		response = string(p)
	}

	response = ParseJSONWithRetries(m.modelFlash, prompt, response, identifier)

	var output AskQuestionsOutput
	err = json.Unmarshal([]byte(response), &output) // Convert JSON string to struct
	if err != nil {
		logger.Log.Debug("Error converting to struct: ", zap.Error(err))
	}

	finalPrompt := originalPrompt
	if len(output.Questions) > 0 {
		conceptSearchResults := make([][]string, len(output.Questions))
		answersPresent := false

		for i, question := range output.Questions {
			relevantRecords := m.conceptExampleDB.Search([]string{question}, m.projectID, m.location, 0.25, 2)
			if len(relevantRecords) > 0 {
				answersPresent = true
				for _, record := range relevantRecords {
					if rewrite, ok := record["rewrite"].(string); ok {
						conceptSearchResults[i] = append(conceptSearchResults[i], rewrite)
					} else {
						logger.Log.Debug("Error: 'rewrite' field is not a string")
					}
				}
			}
		}

		if answersPresent {
			formattedResults := formatQuestionsAndResults(output.Questions, conceptSearchResults)
			finalPrompt += "\n" + formattedResults
		}
	}

	resp, err = m.modelPro.GenerateContent(ctx, genai.Text(finalPrompt))
	if err != nil {
		logger.Log.Error("Error generating content:", zap.Error(err))
		return "", err
	}
	logger.Log.Debug("Token: ",
		zap.Int32("Prompt Token:", resp.UsageMetadata.PromptTokenCount),
		zap.Int32("Candidate Token:", resp.UsageMetadata.CandidatesTokenCount),
		zap.Int32("Total Token:", resp.UsageMetadata.TotalTokenCount))
	if p, ok := resp.Candidates[0].Content.Parts[0].(genai.Text); ok {
		response = string(p)
	}

	logger.Log.Debug("Final Response: ", zap.String("response", response))

	response = ParseJSONWithRetries(m.modelFlash, finalPrompt, response, identifier)

	return response, nil
}

func formatQuestionsAndResults(questions []string, searchResults [][]string) string {
	//TODO: Move prompts to promt file.
	formattedString := "Use the following questions and their answers required for the code conversions\n**Questions and Search Results:**\n\n"

	for i, question := range questions {
		if len(searchResults[i]) > 0 {
			formattedString += fmt.Sprintf("* **Question %d:** %s\n", i+1, question)
			for j, result := range searchResults[i] {
				formattedString += fmt.Sprintf("    * **Search Result %d:** %s\n", j+1, result)
			}
		}
	}

	return formattedString
}

func ParseJSONWithRetries(model *genai.GenerativeModel, originalPrompt string, originalResponse string, identifier string) string {
	//TODO: Move prompts to promt file.
	promptTemplate := `
		You are a JSON parser expert tasked with fixing parsing errors in JSON string. Golang's json.Unmarshal library is 
		being used for parsing the json string. The following JSON string is currently failing with error message: %s.  
		Ensure that all the parsing errors are resolved and output string is parsable by json.Unmarshal library. Also, 
		ensure that the output only contain JSON string.
		
		%s
		`

	for i := 0; i < JsonParserRetry; i++ {
		logger.Log.Debug("ParseJSONWithRetries original response: ", zap.String("response", originalResponse))
		response := strings.TrimSpace(originalResponse)

		if response == "" {
			return response
		}

		response = strings.TrimPrefix(response, "```json\n")
		response = strings.TrimPrefix(response, "@@@json\n")
		response = strings.TrimSuffix(response, "```")
		response = strings.TrimSuffix(response, "@@@")
		response = strings.ReplaceAll(response, "\t", "")
		response = strings.TrimSpace(response)

		var result map[string]any
		err := json.Unmarshal([]byte(response), &result)
		if err == nil {
			logger.Log.Debug("Parsed response: ", zap.String("response", response))
			return response
		}

		logger.Log.Debug("Received error while parsing: ", zap.Error(err))

		newPrompt := fmt.Sprintf(promptTemplate, err.Error(), response)

		logger.Log.Debug("Json retry Prompt: ", zap.String("prompt", newPrompt))
		resp, err := model.GenerateContent(context.Background(), genai.Text(newPrompt))
		if err != nil {
			logger.Log.Fatal("Failed to get response from model: " + fmt.Sprintf("Error: %v", err))
		}
		logger.Log.Debug("Token: ",
			zap.Int32("Prompt Token:", resp.UsageMetadata.PromptTokenCount),
			zap.Int32("Candidate Token:", resp.UsageMetadata.CandidatesTokenCount),
			zap.Int32("Total Token:", resp.UsageMetadata.TotalTokenCount))
		if p, ok := resp.Candidates[0].Content.Parts[0].(genai.Text); ok {
			originalResponse = string(p)
		}
	}
	return ""
}

func (m *MigrationSummarizer) fetchFileContent(filepath string) (string, error) {

	// Read file content if not provided
	content, err := ReadFileWithExplicitBuffer(filepath, bufio.MaxScanTokenSize*10)
	if err != nil {
		logger.Log.Fatal("Failed read file: ", zap.Error(err), zap.String("filepath", filepath))
		return "", err
	}

	return content, nil
}

func (m *MigrationSummarizer) AnalyzeFileTask(analyzeFileInput *AnalyzeFileInput, mutex *sync.Mutex) task.TaskResult[*AnalyzeFileResponse] {
	analyzeFileResponse := m.AnalyzeFile(
		analyzeFileInput.ctx,
		analyzeFileInput.projectPath,
		analyzeFileInput.filepath,
		analyzeFileInput.methodChanges,
		analyzeFileInput.content,
		analyzeFileInput.fileIndex)
	return task.TaskResult[*AnalyzeFileResponse]{Result: analyzeFileResponse, Err: nil}
}

func (m *MigrationSummarizer) AnalyzeFile(ctx context.Context, projectPath, filepath, methodChanges, content string, fileIndex int) *AnalyzeFileResponse {
	snippetsArr := make([]Snippet, 0)
	emptyAssessment := &CodeAssessment{
		Snippets:        &snippetsArr,
		GeneralWarnings: make([]string, 0),
	}

	codeAssessment := emptyAssessment

	var response string
	var isDao bool
	methodSignatureChanges := make([]any, 0)
	if m.dependencyAnalyzer.IsDAO(filepath, content) {
		logger.Log.Debug("Analyze File: "+filepath, zap.Bool("isDao", true))
		var err error
		prompt := m.getPromptForDAOClass(content, filepath, &methodChanges, &m.sourceSchema, &m.targetSchema)
		response, err = m.MigrationCodeConversionInvoke(ctx, prompt, content, m.sourceSchema, m.targetSchema, "analyze-dao-class-"+filepath)
		isDao = true
		if err != nil {
			logger.Log.Error("Error analyzing DAO class: ", zap.Error(err))
			return &AnalyzeFileResponse{codeAssessment, methodSignatureChanges, projectPath, filepath}
		}

		publicMethodSignatures, err := m.fetchPublicMethodSignature(response)
		if err != nil {
			logger.Log.Error("Error analyzing DAO class(public method signature)")
		} else {
			methodSignatureChanges = publicMethodSignatures
		}

	} else {
		logger.Log.Debug("Analyze File: "+filepath, zap.Bool("isDao", false))
		prompt := m.getPromptForNonDAOClass(content, filepath, &methodChanges)
		res, err := m.modelFlash.GenerateContent(ctx, genai.Text(prompt))

		if err != nil {
			return &AnalyzeFileResponse{codeAssessment, methodSignatureChanges, projectPath, filepath}
		}
		logger.Log.Debug("Token: ",
			zap.Int32("Prompt Token:", res.UsageMetadata.PromptTokenCount),
			zap.Int32("Candidate Token:", res.UsageMetadata.CandidatesTokenCount),
			zap.Int32("Total Token:", res.UsageMetadata.TotalTokenCount))

		if p, ok := res.Candidates[0].Content.Parts[0].(genai.Text); ok {
			response = string(p)
		}

		response = ParseJSONWithRetries(m.modelFlash, prompt, response, "analyze-dao-class-"+filepath)
		isDao = false

		methodSignatureChangesResponse, err := m.fetchMethodSignature(response)
		if err != nil {
			logger.Log.Error("Error analyzing Non-DAO class(public method signature): ", zap.Error(err))
		} else {
			methodSignatureChanges = methodSignatureChangesResponse
		}
	}
	logger.Log.Debug("Analyze File Response: " + response)

	codeAssessment, err := parser.ParseFileAnalyzerResponse(projectPath, filepath, response, isDao, fileIndex)

	if err != nil {
		return &AnalyzeFileResponse{emptyAssessment, methodSignatureChanges, projectPath, filepath}
	}

	return &AnalyzeFileResponse{codeAssessment, methodSignatureChanges, projectPath, filepath}
}

func (m *MigrationSummarizer) fetchPublicMethodSignature(fileAnalyzerResponse string) ([]any, error) {

	var responseMapStructure map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &responseMapStructure)
	if err != nil {
		logger.Log.Error("Error parsing file analyzer response: ", zap.Error(err))
		return nil, err
	}

	publicMethodChanges, ok := responseMapStructure["public_method_changes"].([]any)
	if !ok {
		return nil, fmt.Errorf("public_method_changes not found or not a list")
	}

	return publicMethodChanges, nil
}

func (m *MigrationSummarizer) fetchMethodSignature(fileAnalyzerResponse string) ([]any, error) {

	var responseMapStructure map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &responseMapStructure)
	if err != nil {
		logger.Log.Error("Error parsing file analyzer response: ", zap.Error(err))
		return nil, err
	}

	publicMethodChanges, ok := responseMapStructure["method_signature_changes"].([]any)
	if !ok {
		return nil, fmt.Errorf("method_signature_changes not found or not a list")
	}

	return publicMethodChanges, nil
}

func (m *MigrationSummarizer) fetchDependentMethodSignatureChange(filePath string) string {
	publicMethodSignatures := make([]any, 0, 10)
	for dependency := range m.dependencyGraph[filePath] {
		if fileDependencyAnalysisData, ok := m.fileDependencyAnalysisDataMap[dependency]; ok {
			publicMethodSignatures = append(publicMethodSignatures, fileDependencyAnalysisData.publicSignatures...)
		}
	}

	publicMethodSignatureString, err := json.MarshalIndent(publicMethodSignatures, "", "  ")
	if err != nil {
		logger.Log.Error("Error fetching dependent method signature: ", zap.Error(err))
		return ""
	}
	return string(publicMethodSignatureString)
}

func (m *MigrationSummarizer) analyzeFileDependencies(filePath, fileContent string) (bool, string) {
	if m.dependencyAnalyzer.IsDAO(filePath, fileContent) {
		return true, m.fetchDependentMethodSignatureChange(filePath)
	}

	isDaoDependent := false
	for dependency := range m.dependencyGraph[filePath] {
		if m.fileDependencyAnalysisDataMap[dependency].isDaoDependent {
			isDaoDependent = true
			break
		}
	}

	if isDaoDependent {
		return true, m.fetchDependentMethodSignatureChange(filePath)
	}

	return false, ""
}

func (m *MigrationSummarizer) AnalyzeProject(ctx context.Context) (*CodeAssessment, error) {
	logger.Log.Info(fmt.Sprintf("analyzing project: %s", m.projectPath))
	dependencyGraph, executionOrder := m.dependencyAnalyzer.GetExecutionOrder(m.projectPath)
	m.dependencyAnalyzer.LogDependencyGraph(dependencyGraph, m.projectPath)
	m.dependencyAnalyzer.LogExecutionOrder(executionOrder)

	m.dependencyGraph = dependencyGraph

	snippetsArr := make([]Snippet, 0, 10)
	codeAssessment := &CodeAssessment{
		ProjectPath:     m.projectPath,
		Snippets:        &snippetsArr,
		GeneralWarnings: make([]string, 0, 10),
	}

	runParallel := &task.RunParallelTasksImpl[*AnalyzeFileInput, *AnalyzeFileResponse]{}
	fileIndex := 0
	totalLoc := 0
	language := ""
	framework := ""

	logger.Log.Info("initiating file scanning. this may take a few minutes")
	for _, singleOrder := range executionOrder {
		analyzeFileInputs := make([]*AnalyzeFileInput, 0, len(singleOrder))
		for _, filePath := range singleOrder {
			fileIndex++
			if language == "" {
				language = getLanguage(filePath)
			}
			content, err := m.fetchFileContent(filePath)
			if err != nil {
				logger.Log.Error("Error fetching file content: ", zap.Error(err))
				continue
			}
			if framework == "" {
				framework = getFramework(content)
			}
			totalLoc += strings.Count(content, "\n")

			isDaoDepndent, methodChanges := m.analyzeFileDependencies(filePath, content)
			if !isDaoDepndent {
				continue
			}
			analyzeFileInputs = append(analyzeFileInputs, &AnalyzeFileInput{
				ctx:           ctx,
				projectPath:   m.projectPath,
				filepath:      filePath,
				methodChanges: methodChanges,
				content:       content,
				fileIndex:     fileIndex,
			})
		}
		if len(analyzeFileInputs) == 0 {
			continue
		}
		taskResults, err := runParallel.RunParallelTasks(analyzeFileInputs, 20, m.AnalyzeFileTask, false)
		if err != nil {
			logger.Log.Error("Error running parallel analyze files: ", zap.Error(err))
		} else {
			for _, analyzeFileResponse := range taskResults {
				analyzeFileResponse := analyzeFileResponse.Result
				logger.Log.Debug("File Code Assessment: ",
					zap.Any("fileCodeAssessment", analyzeFileResponse.CodeAssessment), zap.Any("filePath", analyzeFileResponse.filePath))

				*codeAssessment.Snippets = append(*codeAssessment.Snippets, *analyzeFileResponse.CodeAssessment.Snippets...)
				codeAssessment.GeneralWarnings = append(codeAssessment.GeneralWarnings, analyzeFileResponse.CodeAssessment.GeneralWarnings...)

				m.fileDependencyAnalysisDataMap[analyzeFileResponse.filePath] = FileDependencyAnalysisData{
					publicSignatures: analyzeFileResponse.methodSignature,
					isDaoDependent:   true,
				}

			}
		}
	}
	codeAssessment.Language = language
	codeAssessment.Framework = framework
	codeAssessment.TotalLoc = totalLoc
	codeAssessment.TotalFiles = fileIndex
	return codeAssessment, nil
}

func getFramework(fileContent string) string {
	//TODO - move into language specific implementations
	if strings.Contains(fileContent, "database/sql") || strings.Contains(fileContent, "github.com/go-sql-driver/mysql") {
		return "database/sql"
	}

	if strings.Contains(fileContent, "*sql.DB") || strings.Contains(fileContent, "*sql.Tx") {
		return "database/sql"
	}

	if strings.Contains(fileContent, "`gorm:\"") {
		return "gorm"
	}

	return ""
}

func getLanguage(filePath string) string {
	//TODO - move into language specific implementations
	if strings.HasSuffix(filePath, ".go") {
		return "golang"
	}
	return ""
}

func (m *MigrationSummarizer) getPromptForNonDAOClass(content, filepath string, methodChanges *string) string {
	var prompt = NonDAOMigrationPromptTemplate

	prompt = strings.ReplaceAll(prompt, "{{FILEPATH}}", filepath)
	prompt = strings.ReplaceAll(prompt, "{{CONTENT}}", content)
	prompt = strings.ReplaceAll(prompt, "{{METHOD_CHANGES}}", *methodChanges)
	prompt = strings.ReplaceAll(prompt, "{{SOURCE_FRAMEWORK}}", m.sourceFramework)
	prompt = strings.ReplaceAll(prompt, "{{TARGET_FRAMEWORK}}", m.targetFramework)

	return prompt
}

func (m *MigrationSummarizer) getPromptForDAOClass(content, filepath string, methodChanges, oldSchema, newSchema *string) string {
	var prompt = DAOMigrationPromptTemplate

	prompt = strings.ReplaceAll(prompt, "{{OLDER_SCHEMA}}", *oldSchema)
	prompt = strings.ReplaceAll(prompt, "{{NEW_SCHEMA}}", *newSchema)
	prompt = strings.ReplaceAll(prompt, "{{FILEPATH}}", filepath)
	prompt = strings.ReplaceAll(prompt, "{{CONTENT}}", content)
	prompt = strings.ReplaceAll(prompt, "{{METHOD_CHANGES}}", *methodChanges)
	prompt = strings.ReplaceAll(prompt, "{{SOURCE_FRAMEWORK}}", m.sourceFramework)
	prompt = strings.ReplaceAll(prompt, "{{TARGET_FRAMEWORK}}", m.targetFramework)

	return prompt
}
