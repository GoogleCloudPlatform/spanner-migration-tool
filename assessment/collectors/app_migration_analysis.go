//go:build ignore

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

	"cloud.google.com/go/vertexai/genai"
	assessment "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/embeddings"
	parser "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/parser"
	dependencyAnalyzer "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/project_analyzer"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
)

// MigrationSummarizer holds the LLM models and example databases
type MigrationSummarizer struct {
	projectID          string
	location           string
	client             *genai.Client
	modelPro           *genai.GenerativeModel
	modelFlash         *genai.GenerativeModel
	conceptExampleDB   *assessment.MysqlConceptDb
	dependencyAnalyzer dependencyAnalyzer.DependencyAnalyzer
	sourceSchema       string
	targetSchema       string
}

type AskQuestionsOutput struct {
	Questions []string `json:"questions"`
}

// NewMigrationSummarizer initializes a new MigrationSummarizer
func NewMigrationSummarizer(ctx context.Context, googleGenerativeAIAPIKey *string, projectID, location, sourceSchema, targetSchema string) (*MigrationSummarizer, error) {
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
	return &MigrationSummarizer{
		projectID:          projectID,
		location:           location,
		client:             client,
		modelPro:           client.GenerativeModel("gemini-1.5-pro-001"),
		modelFlash:         client.GenerativeModel("gemini-1.5-flash-001"),
		conceptExampleDB:   conceptExampleDB,
		dependencyAnalyzer: dependencyAnalyzer.AnalyzerFactory("go"),
		sourceSchema:       sourceSchema,
		targetSchema:       targetSchema,
	}, nil
}

func (m *MigrationSummarizer) MigrationCodeConversionInvoke(
	ctx context.Context,
	originalPrompt, sourceCode, olderSchema, newSchema, identifier string,
) (string, error) {
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

	var response string
	if p, ok := resp.Candidates[0].Content.Parts[0].(genai.Text); ok {
		response = string(p)
	}

	response = ParseJSONWithRetries(m.modelPro, prompt, response, 2, identifier)

	var output AskQuestionsOutput
	err = json.Unmarshal([]byte(response), &output) // Convert JSON string to struct
	if err != nil {
		logger.Log.Debug("Error converting to struct: " + err.Error())
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

	logger.Log.Debug("Final Prompt: " + finalPrompt)

	resp, err = m.modelPro.GenerateContent(ctx, genai.Text(finalPrompt))
	if err != nil {
		return "", err
	}
	if p, ok := resp.Candidates[0].Content.Parts[0].(genai.Text); ok {
		response = string(p)
	}

	logger.Log.Debug("Final Response: " + response)

	response = ParseJSONWithRetries(m.modelPro, finalPrompt, response, 2, identifier)

	return response, nil
}

func formatQuestionsAndResults(questions []string, searchResults [][]string) string {
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

func ParseJSONWithRetries(model *genai.GenerativeModel, originalPrompt string, originalResponse string, retries int, identifier string) string {
	promptTemplate := `
		The following generated JSON value failed to parse, it contained the following
		error. Please return corrected string as a valid JSON in the dictionary format. All strings should be
		single-line strings.

		The original prompt was:
		%s

		And the generated JSON is:

		@@@json
		%s
		@@@

		Error: %s `

	for i := 0; i < retries; i++ {
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

		var result map[string]interface{}
		err := json.Unmarshal([]byte(response), &result)
		if err == nil {
			return response
		}

		errMessage := fmt.Sprintf("Error: %v", err)
		logger.Log.Debug("Received error while parsing: " + errMessage)

		newPrompt := fmt.Sprintf(promptTemplate, originalPrompt, originalResponse, errMessage)

		logger.Log.Debug(response)
		resp, err := model.GenerateContent(context.Background(), genai.Text(newPrompt))
		if err != nil {
			logger.Log.Fatal("Failed to get response from model: " + fmt.Sprintf("Error: %v", err))
		}
		if p, ok := resp.Candidates[0].Content.Parts[0].(genai.Text); ok {
			originalResponse = string(p)
		}
	}
	return ""
}

func (m *MigrationSummarizer) AnalyzeFile(ctx context.Context, filepath string, methodChanges string) (*CodeAssessment, string) {
	var content string
	var err error
	var codeAssessment *CodeAssessment

	// Read file content if not provided
	content, err = readFile(filepath)
	if err != nil {
		logger.Log.Fatal("Failed read file: " + fmt.Sprintf("Error: %v", err))
		return codeAssessment, ""
	}

	var response string
	var isDao bool
	if m.dependencyAnalyzer.IsDAO(filepath, content) {
		prompt := getPromptForDAOClass(content, filepath, &methodChanges, &m.sourceSchema, &m.targetSchema)
		response, err = m.MigrationCodeConversionInvoke(ctx, prompt, content, m.sourceSchema, m.targetSchema, "analyze-dao-class-"+filepath)
		isDao = true
	} else {
		prompt := getPromptForNonDAOClass(content, filepath, &methodChanges)
		res, err := m.modelFlash.GenerateContent(ctx, genai.Text(prompt))

		if err != nil {
			return codeAssessment, ""
		}

		if p, ok := res.Candidates[0].Content.Parts[0].(genai.Text); ok {
			response = string(p)
		}

		response = ParseJSONWithRetries(m.modelFlash, prompt, response, 2, "analyze-dao-class-"+filepath)
		isDao = false
	}
	logger.Log.Debug("Analyze File Response: " + response)

	codeAssessment, error := parser.ParseFileAnalyzerResponse(filepath, response, isDao)

	if error != nil {
		return codeAssessment, ""
	}

	return codeAssessment, response
}

func getPromptForNonDAOClass(content, filepath string, methodChanges *string) string {
	return fmt.Sprintf(`
		You are tasked with adapting a Java class to function correctly within an application that has migrated its persistence layer to Cloud Spanner.

		**Objective:**
		Analyze the provided Java code and identify the necessary modifications for compatibility with the updated application architecture.

    **Output:**
		Return your analysis in JSON format with the following keys:

		*   **file_modifications**: A list of required code changes.
		*   **method_signature_changes**: A list of required public method signature changes for callers (excluding parameter name changes).
		*   **general_warnings**: A list of general warnings or considerations for the migration, especially regarding Spanner-specific limitations and best practices.

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
		...]
		@@@

		**Format for method_signature_changes**
		@@@json
		[
		{
				"original_signature": "<original method signature>",
				"new_signature": "<modified method signature>",
				"explanation": "<description of why the change is needed and how to update the code>"
		},
		...]
		@@@

    **Instructions:**
		1. Line numbers in file_modifications must be accurate and include all lines in the original code.
		2. All generated result values should be single-line strings. Avoid hallucinations and suggest only relevant changes.
		3. Consider the class's role within the application.
						a. If it interacts with a service layer, identify any calls to service methods that have changed due to the underlying DAO updates and suggest appropriate modifications.
						b. If it's a POJO, analyze if any changes in data types or structures are required due to the Spanner migration.
						c. If it's a utility class, determine if any of its functionalities are affected by the new persistence layer.
		4. Consider potential impacts on business logic or data flow due to changes in the underlying architecture.
		5. Ensure the returned JSON is valid and parsable.
		6. Capture larger code snippets for modification and provide cumulative descriptions instead of line-by-line changes.
		7. Classify complexity as SIMPLE, MODERATE, or COMPLEX based on implementation difficulty, required expertise, and clarity of requirements.


		**INPUT:**
		File Path: %s
		File Content:
		%s
		Method Changes:
		%s`, filepath, content, *methodChanges)
}

func readFile(filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var content string
	for scanner.Scan() {
		content += scanner.Text() + "\n"
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return content, nil
}

// func main() {
// 	logger.Log = zap.NewNop()
// 	ctx := context.Background()
// 	projectID := ""
// 	location := ""
// 	apiKey := "<API_KEY>"
// 	filePath := ""

// 	mysqlSchemaPath := ""
// 	spannerSchemaPath := ""
// 	mysqlSchema, err := readFile(mysqlSchemaPath)
// 	if err != nil {
// 		fmt.Println("Error reading MySQL schema file:", err)
// 		return
// 	}

// 	spannerSchema, err := readFile(spannerSchemaPath)
// 	if err != nil {
// 		fmt.Println("Error reading Spanner schema file:", err)
// 		return
// 	}
// 	summarizer, err := NewMigrationSummarizer(ctx, &apiKey, projectID, location, mysqlSchema, spannerSchema)
// 	if err != nil {
// 		log.Fatalf("Error initializing summarizer: %v", err)
// 	}

// 	codeAssessment, result := summarizer.AnalyzeFile(ctx, filePath, "")

// 	var firstResult map[string]interface{}
// 	err = json.Unmarshal([]byte(result), &firstResult)
// 	if err != nil {
// 		logger.Log.Debug("Error converting to struct: " + err.Error())
// 		return
// 	}

// 	publicMethod, err1 := json.MarshalIndent(firstResult["public_method_changes"], "", "  ")
// 	if err1 != nil {
// 		logger.Log.Debug("Error converting to struct: " + err.Error())
// 		return
// 	}

// 	secondFilePath := ""
// 	codeAssessment, result = summarizer.AnalyzeFile(ctx, secondFilePath, string(publicMethod))
// }

func getPromptForDAOClass(content, filepath string, methodChanges, oldSchema, newSchema *string) string {
	return fmt.Sprintf(`
        You are a Cloud Spanner expert tasked with migrating a DAO class from MySQL go-sql-mysql to Spanner go-sql-spanner.

				**Objective:**
				Analyze the provided DAO code and identify the necessary modifications for compatibility with Cloud Spanner. The code may include comments, blank lines, and other non-executable elements. Use function documentation and comments to understand the code's purpose, particularly how it interacts with the database.

				**Schema Changes:**
				First, analyze the schema changes between the provided MySQL schema and the new Spanner schema. These changes may include column definitions, indexes, constraints, etc. Each schema change could potentially impact the DAO class code, particularly the SQL queries or method signatures.

				**Output Format: Please strictly follow the following format:**
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
								]
            "public_method_changes:[
									{
											"original_signature": "<original method signature>",
											"new_signature": "<modified method signature>",
											"complexity": "<SIMPLE|MODERATE|COMPLEX>",
											"number_of_affected_lines": <number_of_lines_impacted>,
											"explanation": "<description of why the change is needed and how to update the code>"
									},
									...
                ]

        }
				@@@

        **Instructions:**
        1. Ensure consistency between schema_impact and method_signature_changes.
        2. Output should strictly be in given json format.
        3. All generated result values should be single-line strings. Avoid hallucinations and suggest only relevant changes.
        4. Pay close attention to SQL queries within the DAO code. Identify any queries that are incompatible with Spanner and suggest appropriate modifications.

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
