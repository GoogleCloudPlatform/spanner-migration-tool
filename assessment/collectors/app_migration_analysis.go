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

// MigrationSummarizer holds the LLM models and example databases
type MigrationSummarizer struct {
	projectID                     string
	location                      string
	client                        *genai.Client
	modelPro                      *genai.GenerativeModel
	modelFlash                    *genai.GenerativeModel
	conceptExampleDB              *assessment.MysqlConceptDb
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
func NewMigrationSummarizer(ctx context.Context, googleGenerativeAIAPIKey *string, projectID, location, sourceSchema, targetSchema string, projectPath string) (*MigrationSummarizer, error) {
	if googleGenerativeAIAPIKey != nil {
		os.Setenv("GOOGLE_API_KEY", *googleGenerativeAIAPIKey)
	}

	client, err := genai.NewClient(ctx, projectID, location)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vertex AI client: %w", err)
	}

	conceptExampleDB, err := assessment.NewMysqlConceptDb(projectID, location)
	if err != nil {
		return nil, fmt.Errorf("failed to load code example DB: %w", err)
	}
	m := &MigrationSummarizer{
		projectID:                     projectID,
		location:                      location,
		client:                        client,
		modelPro:                      client.GenerativeModel("gemini-1.5-pro-002"),
		modelFlash:                    client.GenerativeModel("gemini-2.0-flash-001"),
		conceptExampleDB:              conceptExampleDB,
		dependencyAnalyzer:            dependencyAnalyzer.AnalyzerFactory("go"),
		sourceSchema:                  sourceSchema,
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
	//TODO: Move prompts to promt file.
	prompt := fmt.Sprintf(`
		You are a Cloud Spanner expert tasked with migrating an application from MySQL go-sql-mysql to Spanner go-sql-spanner.

		Analyze the provided code, old MySQL schema, and new Spanner schema. The schemas may be extensive as they represent the full database,
		so focus your analysis on the DDL statements relevant to the provided code snippet. Identify areas where the application logic needs to be adapted
		for Spanner and formulate specific questions requiring human expertise.

		Only ask crisp and concise questions where you need actual guidance, human input, or assistance with complex functionalities. Focus on practical challenges and
		differences between MySQL and Spanner go-sql-spanner, such as:
		* How specific MySQL features or queries can be replicated in Spanner.
		* Workarounds for unsupported MySQL features in Spanner.
		* Necessary code changes due to schema differences.
		* Check for performance improvements and ask performance optimizations related questions as well.


		**Instructions**
		* Keep your questions general and focused on Spanner functionality, avoiding application-specific details.
		* Also ask questions on performance optimizations and recommended approaches to work with spanner.
		* Ensure each question is unique and hasn't been asked before.
		* Ensure that the ouput follows strict JSON parsable format

		**Example questions:**
		* "MySQL handles X this way... how can we achieve the same result in Spanner?"
		* "Feature Y is not supported in Spanner... what are the alternative approaches?"

    **Input:**
    * Source_code: %s
    * Older_schema: %s
    * Newer_schema: %s

    **Output:**
    @@@json
    {
        "questions": [
            "Question 1",
            "Question 2"
        ]
    }
    @@@
    `, sourceCode, olderSchema, newSchema)

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
		prompt := getPromptForDAOClass(content, filepath, &methodChanges, &m.sourceSchema, &m.targetSchema)
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
		prompt := getPromptForNonDAOClass(content, filepath, &methodChanges)
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

	logger.Log.Info("initiating file scanning. this may take a few minutes")
	for _, singleOrder := range executionOrder {
		analyzeFileInputs := make([]*AnalyzeFileInput, 0, len(singleOrder))
		for _, filePath := range singleOrder {
			fileIndex++
			content, err := m.fetchFileContent(filePath)
			if err != nil {
				logger.Log.Error("Error fetching file content: ", zap.Error(err))
				continue
			}

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
	return codeAssessment, nil
}

func getPromptForNonDAOClass(content, filepath string, methodChanges *string) string {
	//TODO: Move prompts to promt file.
	return fmt.Sprintf(`
		You are tasked with adapting a Java class to function correctly within an application that has migrated its persistence layer to Cloud Spanner.

		**Objective:**
		Analyze the provided Java code and identify the necessary modifications for compatibility with the updated application architecture.

    **Output:**
		Return your analysis in JSON format with the following keys and ensure strict JSON parsable format:

		*   **file_modifications**: A list of required code changes.
		*   **method_signature_changes**: A list of required public method signature changes for callers (excluding parameter name changes).
		*   **general_warnings**: A list of general warnings or considerations for the migration, especially regarding Spanner-specific limitations and best practices.
		*	**pagination**: Information about the pagination of the response.

		**Format for "file_modifications"**:
		@@@json
		[
		{
				"original_method_signature": "<original method signature where the change is required>",
				"new_method_signature": "<modified method signature>",
				"code_sample": : ["Line1", "Line2", ... ],
				"start_line": <starting line number of the affected code>,
				"end_line": <ending line number of the affected code>,
				"suggested_change": ["Line1", "Line2", ... ],
				"description": "<human-readable description of the required change>",
				"number_of_affected_lines": <number_of_lines_impacted>,
				"complexity": "<SIMPLE|MODERATE|COMPLEX>",
				"warnings": [
				"<thing to be aware of>",
				"<another thing to be aware of>",
				...]
		},
		...],
		@@@

		**Format for method_signature_changes**
		@@@json
		[
		{
				"original_signature": "<original method signature>",
				"new_signature": "<modified method signature>",
				"explanation": "<description of why the change is needed and how to update the code>"
		},
		...],
		@@@

		**Format for general_warnings**
		@@@json
		[
			"Warning 1",
			"Warning 2",
		...],
		@@@

		**Format for pagination**
		@@@json
		{
				"total_page": "Total number of pages that the response has",
				"current_page": "Current page number of the response"
		}
		@@@

    **Instructions:**
		1. Line numbers in file_modifications must be accurate and include all lines in the original code.
		2. All generated result values should be single-line strings. Avoid hallucinations and suggest only relevant changes.
		3. Consider the class's role within the application.
						a. If it interacts with a service layer, identify any calls to service methods that have changed due to the underlying DAO updates and suggest appropriate modifications.
						b. If it's a POJO, analyze if any changes in data types or structures are required due to the Spanner migration.
						c. If it's a utility class, determine if any of its functionalities are affected by the new persistence layer.
		4. Consider potential impacts on business logic or data flow due to changes in the underlying architecture.
		5. Ensure that the output is a valid JSON string and parsable.
		6. Capture larger code snippets for modification and provide cumulative descriptions instead of line-by-line changes.
		7. Classify complexity as SIMPLE, MODERATE, or COMPLEX based on implementation difficulty, required expertise, and clarity of requirements.
		8. Please paginate your output if the token limit is getting reached. Do ensure that the json string is complete and parsable.



		**INPUT:**
		File Path: %s
		File Content:
		%s
		Method Changes:
		%s`, filepath, content, *methodChanges)
}

func getPromptForDAOClass(content, filepath string, methodChanges, oldSchema, newSchema *string) string {
	//TODO: Move prompts to promt file.
	return fmt.Sprintf(`
        You are a Cloud Spanner expert tasked with migrating a DAO class from MySQL go-sql-mysql to Spanner go-sql-spanner.

				**Objective:**
				Analyze the provided DAO code and identify the necessary modifications for compatibility with Cloud Spanner. The code may include comments, blank lines, and other non-executable elements. Use function documentation and comments to understand the code's purpose, particularly how it interacts with the database.

				**Schema Changes:**
				First, analyze the schema changes between the provided MySQL schema and the new Spanner schema. These changes may include column definitions, indexes, constraints, etc. Each schema change could potentially impact the DAO class code, particularly the SQL queries or method signatures.

				**Output Format: Please strictly follow the following format and ensure strict JSON parsable format:**
				@@@json
				{
            "schema_impact": [
									{
											"schema_change": "Exact change in schema",
											"table": "Name of the affected table",
											"column": "Name of the affected column",
											"number_of_affected_lines": <number_of_lines_impacted>,
											"existing_code_lines": ["Line1", "Line2", ... ],
											"new_code_lines": ["Line1", "Line2", ... ]
									},
									...
								],
            "public_method_changes:[
									{
											"original_signature": "<original method signature>",
											"new_signature": "<modified method signature>",
											"complexity": "<SIMPLE|MODERATE|COMPLEX>",
											"number_of_affected_lines": <number_of_lines_impacted>,
											"explanation": "<description of why the change is needed and how to update the code>"
									},
									...
                ],
				"pagination": {
					"total_page": "Total number of pages that the response has",
					"current_page": "Current page number of the response"
				}

        }
				@@@

        **Instructions:**
        1. Ensure consistency between schema_impact and method_signature_changes.
        2. Output should strictly be in given json format and ensure strict JSON parsable format.
        3. All generated result values should be single-line strings. Avoid hallucinations and suggest only relevant changes.
        4. Pay close attention to SQL queries within the DAO code. Identify any queries that are incompatible with Spanner and suggest appropriate modifications.
		    5. Please paginate your output if the token limit is getting reached. Do ensure that the json string is complete and parsable.

        **INPUT**
        **Older MySQL Schema**
        @@@
        %s
        @@@
        **New Spanner Schema**
        @@@
        %s
        @@@

        Please analyze the following file:
        %s

        @@@
        %s
        @@@

        **Dependent File Method Changes:**
        Consider the impact of method changes in dependent files on the DAO code being analyzed.
        @@@
        %s
        @@@`, *oldSchema, *newSchema, filepath, content, *methodChanges)
}
