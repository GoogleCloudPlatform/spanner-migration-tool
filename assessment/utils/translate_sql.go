package utils

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/vertexai/genai"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/task"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
)

//go:embed query_translation_prompt.txt
var queryTranslationPromptTemplate string

//go:embed mysql_query_examples.json
var QueryTranslationExamples []byte

// LLMQueryTranslationInput represents input for query translation
type LLMQueryTranslationInput struct {
	Context       context.Context
	MySQLQuery    string
	MySQLSchema   string
	SpannerSchema string
	AIClient      *genai.Client
	Count         int
	RetryClient   LLMRetryClient
}

func TranslateQueriesToSpanner(ctx context.Context, queries []QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]QueryTranslationResult, error) {

	if len(queries) == 0 {
		return nil, fmt.Errorf("no performance schema queries to translate")
	}
	logger.Log.Info("starting query translation to Spanner", zap.Int("query_count", len(queries)))

	// Create translation inputs
	translationInputs := make([]*LLMQueryTranslationInput, 0, len(queries))
	retryClient := DefaultLLMRetryClient{}
	for _, query := range queries {
		translationInputs = append(translationInputs, &LLMQueryTranslationInput{
			Context:       ctx,
			MySQLQuery:    query.Query,
			MySQLSchema:   mysqlSchema,
			SpannerSchema: spannerSchema,
			AIClient:      aiClient,
			Count:         query.Count,
			RetryClient:   &retryClient,
		})
	}

	// Process translations in parallel
	parallelTaskRunner := &task.RunParallelTasksImpl[*LLMQueryTranslationInput, *QueryTranslationResult]{}

	translationResults, err := parallelTaskRunner.RunParallelTasks(translationInputs, 10, TranslateQueryTask, false)
	if err != nil {
		return nil, fmt.Errorf("failed to run parallel query translation: %w", err)
	}

	// Convert results to final format
	results := make([]QueryTranslationResult, len(translationResults))
	for i, result := range translationResults {
		if result.Result != nil {
			results[i] = *result.Result
		} else {
			results[i] = QueryTranslationResult{
				OriginalQuery:    queries[i].Query,
				TranslationError: "Translation failed",
			}
		}
	}

	logger.Log.Info("query translation completed", zap.Int("translated_count", len(results)))
	return results, nil
}

// TranslateQueryTaskFunc is the function type for query translation tasks.
type TranslateQueryTaskFunc func(input *LLMQueryTranslationInput, mutex *sync.Mutex) task.TaskResult[*QueryTranslationResult]

// TranslateQueryTask is the package-level variable that holds the task function.
// This is the variable that is mocked in our tests.
var TranslateQueryTask TranslateQueryTaskFunc = defaultTranslateQueryTask

// TranslateQueryTask is the task function for translating a single query
func defaultTranslateQueryTask(input *LLMQueryTranslationInput, mutex *sync.Mutex) task.TaskResult[*QueryTranslationResult] {
	// Use the provided AI client
	if input.AIClient == nil {
		logger.Log.Error("AI client not provided")
		return task.TaskResult[*QueryTranslationResult]{
			Result: &QueryTranslationResult{
				OriginalQuery:    input.MySQLQuery,
				TranslationError: "AI client not provided",
			},
		}
	}

	model := input.AIClient.GenerativeModel("gemini-1.5-pro-002")
	model.ResponseMIMEType = "application/json"

	// Build prompt
	prompt := buildTranslationPrompt(input.MySQLQuery, input.MySQLSchema, input.SpannerSchema)
	logger.Log.Debug("Prompt: " + prompt)

	// Generate translation
	response, err := input.RetryClient.GenerateContentWithRetry(input.Context, model, genai.Text(prompt), 5, logger.Log)
	if err != nil {
		logger.Log.Error("failed to generate query translation",
			zap.String("query", input.MySQLQuery),
			zap.Error(err))
		return task.TaskResult[*QueryTranslationResult]{
			Result: &QueryTranslationResult{
				OriginalQuery:    input.MySQLQuery,
				TranslationError: fmt.Sprintf("LLM error: %v", err),
			},
			Err: err,
		}
	}

	// Parse response
	var llmResponse string
	if part, ok := response.Candidates[0].Content.Parts[0].(genai.Text); ok {
		llmResponse = string(part)
	}
	logger.Log.Debug("Response: " + llmResponse)
	// Parse JSON response
	var translationResult QueryTranslationResult
	if err := json.Unmarshal([]byte(llmResponse), &translationResult); err != nil {
		logger.Log.Error("failed to parse LLM response",
			zap.String("response", llmResponse),
			zap.Error(err))
		return task.TaskResult[*QueryTranslationResult]{
			Result: &QueryTranslationResult{
				OriginalQuery:    input.MySQLQuery,
				TranslationError: fmt.Sprintf("JSON parsing error: %v", err),
			},
			Err: err,
		}
	}

	translationResult.OriginalQuery = input.MySQLQuery
	translationResult.Source = "performance_schema"
	translationResult.ExecutionCount = input.Count

	return task.TaskResult[*QueryTranslationResult]{
		Result: &translationResult,
		Err:    nil,
	}
}

// buildTranslationPrompt builds the prompt for query translation
func buildTranslationPrompt(mysqlQuery, mysqlSchema, spannerSchema string) string {
	prompt := queryTranslationPromptTemplate

	// Replace placeholders
	prompt = strings.ReplaceAll(prompt, "{{MYSQL_QUERY}}", mysqlQuery)
	prompt = strings.ReplaceAll(prompt, "{{MYSQL_SCHEMA}}", mysqlSchema)
	prompt = strings.ReplaceAll(prompt, "{{SPANNER_SCHEMA}}", spannerSchema)
	prompt = strings.ReplaceAll(prompt, "{{QUERY_EXAMPLES}}", string(QueryTranslationExamples))

	return prompt
}
