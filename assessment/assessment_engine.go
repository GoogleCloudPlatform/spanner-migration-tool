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
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/vertexai/genai"
	assessment "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/task"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

type assessmentCollectors struct {
	sampleCollector            *assessment.SampleCollector
	infoSchemaCollector        *assessment.InfoSchemaCollector
	appAssessmentCollector     assessment.AppCodeAssessor
	performanceSchemaCollector *assessment.PerformanceSchemaCollector
}

type assessmentTaskInput struct {
	taskName string
	taskFunc func(ctx context.Context, c assessmentCollectors) (utils.AssessmentOutput, error)
}

func PerformAssessment(conv *internal.Conv, sourceProfile profiles.SourceProfile, assessmentConfig map[string]string, projectId string) (utils.AssessmentOutput, error) {

	logger.Log.Info("performing assessment")
	logger.Log.Info(fmt.Sprintf("assessment config %+v", assessmentConfig))
	logger.Log.Info(fmt.Sprintf("project id %+v", projectId))

	ctx := context.Background()

	output := utils.AssessmentOutput{}
	// Initialize collectors
	c, err := initializeCollectors(conv, sourceProfile, assessmentConfig, projectId, ctx)
	if err != nil {
		logger.Log.Error("unable to initialize collectors")
		return output, err
	}

	// perform each type of assessment (in parallel) - cost, schema, app code, query, performance
	// within each type of assessment (in parallel) - invoke each collector to fetch information from relevant rules
	// Iterate over assessment rules and order output by confidence of each element. Merge outputs where required
	// Select the highest confidence output for each attribute
	// Populate assessment struct
	parallelTaskRunner := &task.RunParallelTasksImpl[assessmentTaskInput, utils.AssessmentOutput]{}

	assessmentTasksInput := []assessmentTaskInput{
		{
			taskName: "schemaAssessment",
			taskFunc: func(ctx context.Context, c assessmentCollectors) (utils.AssessmentOutput, error) {
				result, err := performSchemaAssessment(ctx, c)
				return utils.AssessmentOutput{SchemaAssessment: result}, err
			},
		},
		{
			taskName: "appAssessment",
			taskFunc: func(ctx context.Context, c assessmentCollectors) (utils.AssessmentOutput, error) {
				result, err := performAppAssessment(ctx, c)
				return utils.AssessmentOutput{AppCodeAssessment: result}, err
			},
		},
	}

	assessmentResults, err := parallelTaskRunner.RunParallelTasks(assessmentTasksInput, 2, func(input assessmentTaskInput, mutex *sync.Mutex) task.TaskResult[utils.AssessmentOutput] {
		result, err := input.taskFunc(ctx, c)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("could not complete %s: ", input.taskName), zap.Error(err))
		}
		return task.TaskResult[utils.AssessmentOutput]{Result: result, Err: err}
	}, false)

	if err != nil {
		// Handle any error from the parallel task runner itself
		return output, err
	}

	for _, result := range assessmentResults {
		if result.Result.SchemaAssessment != nil {
			output.SchemaAssessment = result.Result.SchemaAssessment
		}
		if result.Result.AppCodeAssessment != nil {
			output.AppCodeAssessment = result.Result.AppCodeAssessment
		}
	}

	combinedQueries := combineAndDeduplicateQueries(c.performanceSchemaCollector.Queries, output.AppCodeAssessment)
	logger.Log.Info("Combined deduplicated queries", zap.Int("count", len(combinedQueries)))
	translatedQueries, err := performQueryAssessment(ctx, c, combinedQueries, projectId, assessmentConfig, conv)
	output.QueryAssessment = utils.QueryAssessmentOutput{QueryTranslationResult: &translatedQueries}
	if err != nil {
		logger.Log.Error("error translating queries", zap.Error(err))
		return output, err
	}

	return output, nil
}

type AIClientService struct {
	NewClientFunc        func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error)
	TranslateQueriesFunc func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error)
}

var aiClientService = &AIClientService{
	NewClientFunc:        genai.NewClient,
	TranslateQueriesFunc: utils.TranslateQueriesToSpanner,
}

func performQueryAssessment(ctx context.Context, collectors assessmentCollectors, queries []utils.QueryTranslationResult, projectId string, assessmentConfig map[string]string, conv *internal.Conv) ([]utils.QueryTranslationResult, error) {
	logger.Log.Info("starting query assessment...")
	var performanceSchemaQueries []utils.QueryTranslationInput
	var translationResult []utils.QueryTranslationResult

	mysqlSchema := utils.GetDDL(conv.SrcSchema)
	spannerSchema := strings.Join(
		ddl.GetDDL(
			ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: "mysql"},
			conv.SpSchema,
			conv.SpSequences),
		"\n")

	for _, query := range queries {
		if query.AssessmentSource == "performance_schema" {
			performanceSchemaQueries = append(performanceSchemaQueries, utils.QueryTranslationInput{
				Query: query.NormalizedQuery,
				Count: query.ExecutionCount,
			})
		} else {
			query.SpannerTablesAffected, query.TranslationError = fetchSpannerTableNames(conv, query.SourceTablesAffected)

			translationResult = append(translationResult, query)
		}
	}
	aiClient, err := aiClientService.NewClientFunc(ctx, projectId, assessmentConfig["location"])
	if err != nil {
		return translationResult, fmt.Errorf("Error creating ai client")
	}
	translatedQueries, err := aiClientService.TranslateQueriesFunc(ctx, performanceSchemaQueries, aiClient, mysqlSchema, spannerSchema)
	if translatedQueries != nil {
		for _, translatedQuery := range translatedQueries {
			translatedQuery.SpannerTablesAffected, translatedQuery.TranslationError = fetchSpannerTableNames(conv, translatedQuery.SourceTablesAffected)
			translationResult = append(translationResult, translatedQuery)
		}
	}
	logger.Log.Info("query assessment completed successfully.")
	if err != nil {
		return translationResult, fmt.Errorf("Error translating queries: %v", err)
	}
	return translationResult, nil
}

func fetchSpannerTableNames(conv *internal.Conv, tableNames []string) ([]string, string) {
	spannerTableNames := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		tableId, err := internal.GetTableIdFromSrcName(conv.SrcSchema, tableName)
		if err != nil {
			logger.Log.Warn("error getting table id from source name", zap.String("tableName", tableName), zap.Error(err))
			return nil, fmt.Sprintf("error getting table id from source name: %v", err)
		}
		if sp, found := conv.SpSchema[tableId]; found {
			spannerTableNames = append(spannerTableNames, sp.Name)
			continue
		}
		return nil, fmt.Sprintf("spanner table not found for source table: %s", tableName)
	}
	return spannerTableNames, ""
}

// Initilize collectors. Take a decision here on which collectors are mandatory and which are optional
func initializeCollectors(conv *internal.Conv, sourceProfile profiles.SourceProfile, assessmentConfig map[string]string, projectId string, ctx context.Context) (assessmentCollectors, error) {
	c := assessmentCollectors{}
	sampleCollector, err := assessment.CreateSampleCollector()
	if err != nil {
		return c, err
	}
	c.sampleCollector = &sampleCollector
	infoSchemaCollector, err := assessment.GetDefaultInfoSchemaCollector(conv, sourceProfile)
	if infoSchemaCollector.IsEmpty() {
		return c, err
	}
	c.infoSchemaCollector = &infoSchemaCollector

	//Initialize App Assessment Collector
	language, exists := assessmentConfig["language"]
	sourceFramework, exists := assessmentConfig["sourceFramework"]
	targetFramework, exists := assessmentConfig["targetFramework"]

	codeDirectory, exists := assessmentConfig["codeDirectory"]
	if exists {
		logger.Log.Info("initializing app collector")
		mysqlSchema := utils.GetDDL(conv.SrcSchema)
		spannerSchema := strings.Join(
			ddl.GetDDL(
				ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: "mysql"},
				conv.SpSchema,
				conv.SpSequences),
			"\n")

		logger.Log.Debug("mysqlSchema", zap.String("schema", mysqlSchema))
		logger.Log.Debug("spannerSchema", zap.String("schema", spannerSchema))

		summarizer, err := assessment.NewMigrationCodeSummarizer(
			ctx, nil, projectId, assessmentConfig["location"], mysqlSchema, spannerSchema, codeDirectory, language, sourceFramework, targetFramework)
		if err != nil {
			logger.Log.Error("error initiating migration summarizer")
			return c, err
		}
		c.appAssessmentCollector = summarizer
		logger.Log.Info("initialized app collector")
	} else {
		logger.Log.Info("app code info unavailable")
	}

	// Initialize Performance Schema Collector
	logger.Log.Info("initializing performance schema collector")
	performanceSchemaCollector, err := assessment.GetDefaultPerformanceSchemaCollector(sourceProfile)
	if err != nil {
		logger.Log.Warn("failed to initialize performance schema collector", zap.Error(err))
		logger.Log.Info("performance schema assessment will be skipped")
	} else {
		c.performanceSchemaCollector = &performanceSchemaCollector
		logger.Log.Info("initialized performance schema collector")
	}

	return c, err
}

func combineAndDeduplicateQueries(
	performanceSchemaQueries []utils.QueryAssessmentInfo,
	appCodeQueries *utils.AppCodeAssessmentOutput,
) []utils.QueryTranslationResult {
	queryMap := make(map[string]utils.QueryTranslationResult)

	// Process queries from the performance schema collector first.
	for _, q := range performanceSchemaQueries {
		key := q.Query
		queryMap[key] = utils.QueryTranslationResult{
			OriginalQuery:    key,
			NormalizedQuery:  key,
			AssessmentSource: "performance_schema",
			ExecutionCount:   q.Count,
		}
	}

	// Process and merge queries from the app code collector.
	if appCodeQueries != nil && appCodeQueries.QueryTranslationResult != nil {
		for _, q := range *appCodeQueries.QueryTranslationResult {
			key := q.NormalizedQuery
			if key == "" {
				key = q.OriginalQuery
				q.NormalizedQuery = q.OriginalQuery
			}
			if existingQuery, ok := queryMap[key]; ok {
				q.AssessmentSource = "app_code, performance_schema"
				q.ExecutionCount = existingQuery.ExecutionCount
				queryMap[key] = q
			} else {
				queryMap[key] = q
			}
		}

	}
	// Convert map back to slice.
	var combinedQueries []utils.QueryTranslationResult
	for _, q := range queryMap {
		combinedQueries = append(combinedQueries, q)
	}
	return combinedQueries
}

func performSchemaAssessment(ctx context.Context, collectors assessmentCollectors) (*utils.SchemaAssessmentOutput, error) {
	logger.Log.Info("starting schema assessment...")
	schemaOut := &utils.SchemaAssessmentOutput{}

	srcTableDefs, spTableDefs := collectors.infoSchemaCollector.ListTables()
	srcColDefs, spColDefs := collectors.infoSchemaCollector.ListColumnDefinitions()
	srcIndexes, spIndexes := collectors.infoSchemaCollector.ListIndexes()

	tableAssessments := []utils.TableAssessment{}
	for tableId, srcTableDef := range srcTableDefs {
		spTableDef := spTableDefs[tableId]
		tableSizeDiff := tableSizeDiffBytes(&srcTableDef, &spTableDef)

		columnAssessments := []utils.ColumnAssessment{}

		//Populate column info
		for id, srcColumn := range srcColDefs {
			spColumn := spColDefs[id]
			if srcColumn.TableId != tableId {
				//Column not of current table
				continue
			}
			isTypeCompatible := mysql.SourceSpecificComparisonImpl{}.IsDataTypeCodeCompatible(srcColumn, spColumn) // Make generic when more sources added
			sizeIncreaseInBytes := getSpColSizeBytes(spColumn) - srcColumn.MaxColumnSize
			colAssessment := utils.ColumnAssessment{SourceColDef: &srcColumn, SpannerColDef: &spColumn, CompatibleDataType: isTypeCompatible, SizeIncreaseInBytes: int(sizeIncreaseInBytes)}
			columnAssessments = append(columnAssessments, colAssessment)
		}

		//Populate indexes
		tableSrcIndexes := []utils.SrcIndexDetails{}
		for _, srcIndex := range srcIndexes {
			if srcIndex.TableId == tableId {
				tableSrcIndexes = append(tableSrcIndexes, srcIndex)
			}
		}

		tableSpIndexes := []utils.SpIndexDetails{}
		for _, spIndex := range spIndexes {
			if spIndex.TableId == tableId {
				tableSpIndexes = append(tableSpIndexes, spIndex)
			}
		}

		tableAssessment := utils.TableAssessment{
			SourceTableDef:      &srcTableDef,
			SpannerTableDef:     &spTableDef,
			Columns:             columnAssessments,
			SourceIndexDef:      tableSrcIndexes,
			SpannerIndexDef:     tableSpIndexes,
			CompatibleCharset:   isCharsetCompatible(srcTableDef.Charset),
			SizeIncreaseInBytes: tableSizeDiff,
		}

		tableAssessments = append(tableAssessments, tableAssessment)
	}
	schemaOut.TableAssessmentOutput = tableAssessments

	schemaOut.TriggerAssessmentOutput = collectors.infoSchemaCollector.ListTriggers()
	schemaOut.StoredProcedureAssessmentOutput = collectors.infoSchemaCollector.ListStoredProcedures()
	schemaOut.FunctionAssessmentOutput = collectors.infoSchemaCollector.ListFunctions()
	schemaOut.ViewAssessmentOutput = collectors.infoSchemaCollector.ListViews()
	schemaOut.SpSequences = collectors.infoSchemaCollector.ListSpannerSequences()

	logger.Log.Info("schema assessment completed successfully.")
	return schemaOut, nil
}

func performAppAssessment(ctx context.Context, collectors assessmentCollectors) (*utils.AppCodeAssessmentOutput, error) {

	if collectors.appAssessmentCollector == nil {
		logger.Log.Info("not proceeding with app assessment as app collector was not initialized")
		return nil, nil
	}

	logger.Log.Info("starting app assessment...")
	codeAssessment, queryResults, err := collectors.appAssessmentCollector.AnalyzeProject(ctx)

	if err != nil {
		logger.Log.Error("error analyzing project", zap.Error(err))
		return nil, err
	}

	logger.Log.Debug("snippets: ", zap.Any("codeAssessment.Snippets", codeAssessment.Snippets))

	logger.Log.Info("app assessment completed successfully.")
	return &utils.AppCodeAssessmentOutput{
		Language:               codeAssessment.Language,
		Framework:              codeAssessment.Framework,
		TotalLoc:               codeAssessment.TotalLoc,
		TotalFiles:             codeAssessment.TotalFiles,
		CodeSnippets:           codeAssessment.Snippets,
		QueryTranslationResult: &queryResults,
	}, nil
}

func isCharsetCompatible(srcCharset string) bool {
	if !strings.Contains(srcCharset, "utf8") { // TODO add charset level comparisons - per source
		return true
	}
	return false
}

func tableSizeDiffBytes(srcTableDef *utils.SrcTableDetails, spTableDef *utils.SpTableDetails) int {
	// TODO - if no spanner table exists - return nil
	return 1 //TODO - currently dummy implementation assuming spanner will always be bigger - to calculate based on charset and column size differences
}

// TODO - move to spanner interface?
func getSpColSizeBytes(spCol utils.SpColumnDetails) int64 {
	var size int64

	switch strings.ToUpper(spCol.Datatype) {
	case "ARRAY":
		return 10 * 1024 * 1024
	case "BOOL":
		size = 1
	case "BYTES":
		size = spCol.Len
	case "DATE":
		size = 4
	case "FLOAT32":
		size = 4
	case "FLOAT64":
		size = 8
	case "INT64":
		size = 8
	case "JSON":
		return 10 * 1024 * 1024
	case "NUMERIC":
		size = 22
	case "PROTO":
		size = spCol.Len
	case "STRING":
		size = spCol.Len
	case "STRUCT":
		return 10 * 1024 * 1024
	case "TIMESTAMP":
		return 12
	default:
		return 8
	}
	return 8 + size //Overhead per col plus size
}
