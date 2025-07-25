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
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/vertexai/genai"
	collectorCommon "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/common"
	sourcesCommon "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/task"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"go.uber.org/zap"
)

//go:embed prompts/query_translation_prompt.txt
var queryTranslationPromptTemplate string

//go:embed embeddings/mysql_query_examples.json
var queryTranslationExamples string

// generateContentFunc defines a function type for generating content.
type generateContentFunc func(ctx context.Context, model *genai.GenerativeModel, parts ...genai.Part) (*genai.GenerateContentResponse, error)

// PerformanceSchemaCollector collects performance schema data from MySQL databases
type PerformanceSchemaCollector struct {
	queries           []utils.QueryAssessmentInfo
	generateContentFn generateContentFunc
}

// QueryTranslationInput represents input for query translation
type QueryTranslationInput struct {
	Context       context.Context
	MySQLQuery    string
	MySQLSchema   string
	SpannerSchema string
	QueryIndex    int
	AIClient      *genai.Client
}

// QueryTranslationResponse represents the response after translating a query
type QueryTranslationResponse struct {
	Result         *utils.QueryTranslationResult
	QueryIndex     int
	ProcessingTime int64
}

// IsEmpty checks if the collector has any data
func (c PerformanceSchemaCollector) IsEmpty() bool {
	return len(c.queries) == 0
}

// GetDefaultPerformanceSchemaCollector creates a new PerformanceSchemaCollector with default settings
func GetDefaultPerformanceSchemaCollector(sourceProfile profiles.SourceProfile) (PerformanceSchemaCollector, error) {
	return GetPerformanceSchemaCollector(sourceProfile, collectorCommon.SQLDBConnector{}, collectorCommon.DefaultConnectionConfigProvider{}, getPerformanceSchema)
}

// GetPerformanceSchemaCollector creates a new PerformanceSchemaCollector with custom dependencies
func GetPerformanceSchemaCollector(sourceProfile profiles.SourceProfile, dbConnector collectorCommon.DBConnector, configProvider collectorCommon.ConnectionConfigProvider, performanceSchemaProvider func(*sql.DB, profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error)) (PerformanceSchemaCollector, error) {
	logger.Log.Info("initializing performance schema collector")

	connectionConfig, err := configProvider.GetConnectionConfig(sourceProfile)
	if err != nil {
		return PerformanceSchemaCollector{}, fmt.Errorf("failed to get connection config: %w", err)
	}

	db, err := dbConnector.Connect(sourceProfile.Driver, connectionConfig)
	if err != nil {
		return PerformanceSchemaCollector{}, fmt.Errorf("failed to connect to database: %w", err)
	}
	// Only close db if it is a real connection (for test safety, avoid closing dummy db)
	if db != nil {
		// In real use, db.Stats().InUse > 0 for active connections; in tests, dummy db will have 0
		if db.Stats().InUse > 0 {
			db.Close()
		}
	}

	performanceSchema, err := performanceSchemaProvider(db, sourceProfile)
	if err != nil {
		return PerformanceSchemaCollector{}, fmt.Errorf("failed to get performance schema: %w", err)
	}

	queries, err := performanceSchema.GetAllQueries()
	if err != nil {
		return PerformanceSchemaCollector{}, fmt.Errorf("failed to get all queries: %w", err)
	}

	logger.Log.Info("performance schema collector initialized successfully",
		zap.Int("query_count", len(queries)))

	return PerformanceSchemaCollector{
		queries:           queries,
		generateContentFn: defaultGenerateContent,
	}, nil
}

func defaultGenerateContent(ctx context.Context, model *genai.GenerativeModel, parts ...genai.Part) (*genai.GenerateContentResponse, error) {
	return utils.GenerateContentWithRetry(ctx, model, parts[0], 5, logger.Log)
}

// getPerformanceSchema creates a performance schema implementation based on the database driver
func getPerformanceSchema(db *sql.DB, sourceProfile profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error) {
	driver := sourceProfile.Driver
	switch driver {
	case constants.MYSQL:
		return mysql.PerformanceSchemaImpl{
			Db:     db,
			DbName: sourceProfile.Conn.Mysql.Db,
		}, nil
	default:
		return nil, fmt.Errorf("driver %s not supported for performance schema", driver)
	}
}

// TranslateQueriesToSpanner translates MySQL queries to Spanner equivalents using LLM
func (c PerformanceSchemaCollector) TranslateQueriesToSpanner(ctx context.Context, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
	if c.IsEmpty() {
		return nil, fmt.Errorf("no queries to translate")
	}

	logger.Log.Info("starting query translation to Spanner", zap.Int("query_count", len(c.queries)))

	// Create translation inputs
	translationInputs := make([]*QueryTranslationInput, 0, len(c.queries))
	for i, query := range c.queries {
		translationInputs = append(translationInputs, &QueryTranslationInput{
			Context:       ctx,
			MySQLQuery:    query.Query,
			MySQLSchema:   mysqlSchema,
			SpannerSchema: spannerSchema,
			QueryIndex:    i,
			AIClient:      aiClient,
		})
	}

	// Process translations in parallel
	parallelTaskRunner := &task.RunParallelTasksImpl[*QueryTranslationInput, *QueryTranslationResponse]{}

	translationResults, err := parallelTaskRunner.RunParallelTasks(translationInputs, 10, c.TranslateQueryTask, false)
	if err != nil {
		return nil, fmt.Errorf("failed to run parallel query translation: %w", err)
	}

	// Convert results to final format
	results := make([]utils.QueryTranslationResult, len(translationResults))
	for i, result := range translationResults {
		if result.Result != nil {
			results[i] = *result.Result.Result
		} else {
			results[i] = utils.QueryTranslationResult{
				OriginalQuery:    c.queries[i].Query,
				TranslationError: "Translation failed",
			}
		}
	}

	logger.Log.Info("query translation completed", zap.Int("translated_count", len(results)))
	return results, nil
}

// TranslateQueryTask is the task function for translating a single query
func (c PerformanceSchemaCollector) TranslateQueryTask(input *QueryTranslationInput, mutex *sync.Mutex) task.TaskResult[*QueryTranslationResponse] {
	return c.translateQuery(input.Context, input.AIClient, input.MySQLQuery, input.MySQLSchema, input.SpannerSchema, input.QueryIndex)
}

func (c *PerformanceSchemaCollector) translateQuery(ctx context.Context, aiClient *genai.Client, mysqlQuery, mysqlSchema, spannerSchema string, queryIndex int) task.TaskResult[*QueryTranslationResponse] {
	startTime := time.Now()

	if aiClient == nil {
		logger.Log.Error("AI client not provided")
		return task.TaskResult[*QueryTranslationResponse]{
			Result: &QueryTranslationResponse{
				Result: &utils.QueryTranslationResult{
					OriginalQuery:    mysqlQuery,
					TranslationError: "AI client not provided",
				},
				QueryIndex:     queryIndex,
				ProcessingTime: time.Since(startTime).Milliseconds(),
			},
		}
	}

	model := aiClient.GenerativeModel("gemini-1.5-pro-002")
	model.ResponseMIMEType = "application/json"
	return c.doTranslateQuery(ctx, model, mysqlQuery, mysqlSchema, spannerSchema, queryIndex, startTime)
}

func (c *PerformanceSchemaCollector) doTranslateQuery(ctx context.Context, model *genai.GenerativeModel, mysqlQuery, mysqlSchema, spannerSchema string, queryIndex int, startTime time.Time) task.TaskResult[*QueryTranslationResponse] {
	prompt := c.buildTranslationPrompt(mysqlQuery, mysqlSchema, spannerSchema)

	// Generate translation
	response, err := c.generateContentFn(ctx, model, genai.Text(prompt))
	if err != nil {
		logger.Log.Error("failed to generate query translation",
			zap.String("query", mysqlQuery),
			zap.Error(err))
		return task.TaskResult[*QueryTranslationResponse]{
			Result: &QueryTranslationResponse{
				Result: &utils.QueryTranslationResult{
					OriginalQuery:    mysqlQuery,
					TranslationError: fmt.Sprintf("LLM error: %v", err),
				},
				QueryIndex:     queryIndex,
				ProcessingTime: time.Since(startTime).Milliseconds(),
			},
			Err: err,
		}
	}

	// Parse response
	var llmResponse string
	if len(response.Candidates) > 0 && len(response.Candidates[0].Content.Parts) > 0 {
		if part, ok := response.Candidates[0].Content.Parts[0].(genai.Text); ok {
			llmResponse = string(part)
		}
	}

	// Parse JSON response
	var translationResult utils.QueryTranslationResult
	if err := json.Unmarshal([]byte(llmResponse), &translationResult); err != nil {
		logger.Log.Error("failed to parse LLM response",
			zap.String("response", llmResponse),
			zap.Error(err))
		return task.TaskResult[*QueryTranslationResponse]{
			Result: &QueryTranslationResponse{
				Result: &utils.QueryTranslationResult{
					OriginalQuery:    mysqlQuery,
					TranslationError: fmt.Sprintf("JSON parsing error: %v", err),
				},
				QueryIndex:     queryIndex,
				ProcessingTime: time.Since(startTime).Milliseconds(),
			},
			Err: err,
		}
	}

	translationResult.OriginalQuery = mysqlQuery
	translationResult.Source = "performance_schema"
	translationResult.ExecutionCount = c.queries[queryIndex].Count

	logger.Log.Info("query migration analysis completed",
		zap.String("overall_compatibility", translationResult.Complexity),
		zap.String("complexity", translationResult.Complexity))

	println("Spanner Query: ", translationResult.SpannerQuery)

	return task.TaskResult[*QueryTranslationResponse]{
		Result: &QueryTranslationResponse{
			Result:         &translationResult,
			QueryIndex:     queryIndex,
			ProcessingTime: time.Since(startTime).Milliseconds(),
		},
		Err: nil,
	}
}

// buildTranslationPrompt builds the prompt for query translation
func (c PerformanceSchemaCollector) buildTranslationPrompt(mysqlQuery, mysqlSchema, spannerSchema string) string {
	prompt := queryTranslationPromptTemplate

	// Replace placeholders
	prompt = strings.ReplaceAll(prompt, "{{MYSQL_QUERY}}", mysqlQuery)
	prompt = strings.ReplaceAll(prompt, "{{MYSQL_SCHEMA}}", mysqlSchema)
	prompt = strings.ReplaceAll(prompt, "{{SPANNER_SCHEMA}}", spannerSchema)
	prompt = strings.ReplaceAll(prompt, "{{QUERY_EXAMPLES}}", queryTranslationExamples)

	return prompt
}

// ListQueries returns all collected queries with their assessment information
func (c PerformanceSchemaCollector) ListQueries() []utils.QueryAssessmentInfo {
	return c.queries
}
