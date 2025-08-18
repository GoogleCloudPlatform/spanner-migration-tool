package assessment

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/vertexai/genai"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func TestCombineAndDeduplicateQueries(t *testing.T) {
	// Helper function to find a specific result in a slice.
	findResult := func(results []utils.QueryTranslationResult, normalizedQuery string) (utils.QueryTranslationResult, bool) {
		for _, res := range results {
			if res.NormalizedQuery == normalizedQuery {
				return res, true
			}
		}
		return utils.QueryTranslationResult{}, false
	}

	t.Run("empty inputs should return empty slice", func(t *testing.T) {
		result := combineAndDeduplicateQueries(
			[]utils.QueryAssessmentInfo{},
			&utils.AppCodeAssessmentOutput{QueryTranslationResult: &[]utils.QueryTranslationResult{}},
		)
		assert.Empty(t, result)
	})

	t.Run("only performance schema queries", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users WHERE id = ?", Count: 100},
			{Query: "SELECT * FROM products WHERE id = ?", Count: 50},
		}
		result := combineAndDeduplicateQueries(perfQueries, nil)
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "performance_schema", q1.Source)
		assert.Equal(t, 100, q1.ExecutionCount)

		q2, ok2 := findResult(result, "SELECT * FROM products WHERE id = ?")
		assert.True(t, ok2)
		assert.Equal(t, "performance_schema", q2.Source)
		assert.Equal(t, 50, q2.ExecutionCount)
	})

	t.Run("only app code queries", func(t *testing.T) {
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM users WHERE id = ?", OriginalQuery: "SELECT * FROM users WHERE id = 1", Source: "app_code"},
			{NormalizedQuery: "INSERT INTO orders VALUES (?)", OriginalQuery: "INSERT INTO orders VALUES (1)", Source: "app_code"},
		}
		result := combineAndDeduplicateQueries([]utils.QueryAssessmentInfo{}, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "app_code", q1.Source)
		assert.Equal(t, "SELECT * FROM users WHERE id = 1", q1.OriginalQuery)

		q2, ok2 := findResult(result, "INSERT INTO orders VALUES (?)")
		assert.True(t, ok2)
		assert.Equal(t, "app_code", q2.Source)
	})

	t.Run("no common queries", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users WHERE id = ?", Count: 100},
		}
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM products WHERE id = ?", OriginalQuery: "SELECT * FROM products", Source: "app_code"},
		}
		result := combineAndDeduplicateQueries(perfQueries, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "performance_schema", q1.Source)

		q2, ok2 := findResult(result, "SELECT * FROM products WHERE id = ?")
		assert.True(t, ok2)
		assert.Equal(t, "app_code", q2.Source)
	})

	t.Run("some common queries should be deduplicated", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users WHERE id = ?", Count: 100}, // Common query
			{Query: "SELECT * FROM orders WHERE id = ?", Count: 25}, // Unique to perf
		}
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM users WHERE id = ?", OriginalQuery: "SELECT * FROM users WHERE id = 1", Source: "app_code"}, // Common query
			{NormalizedQuery: "INSERT INTO products values(?)", OriginalQuery: "INSERT INTO products values(1)", Source: "app_code"},     // Unique to app code
		}
		result := combineAndDeduplicateQueries(perfQueries, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 3)

		// Check the deduplicated query
		dedupQuery, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "SELECT * FROM users WHERE id = 1", dedupQuery.OriginalQuery)
		assert.Equal(t, "app_code, performance_schema", dedupQuery.Source)
		assert.Equal(t, 100, dedupQuery.ExecutionCount) // Should be from performance schema

		// Check the unique perf query
		perfQuery, ok2 := findResult(result, "SELECT * FROM orders WHERE id = ?")
		assert.True(t, ok2)
		assert.Equal(t, "SELECT * FROM orders WHERE id = ?", perfQuery.OriginalQuery)
		assert.Equal(t, "performance_schema", perfQuery.Source)
		assert.Equal(t, 25, perfQuery.ExecutionCount)

		// Check the unique app code query
		appQuery, ok3 := findResult(result, "INSERT INTO products values(?)")
		assert.True(t, ok3)
		assert.Equal(t, "INSERT INTO products values(1)", appQuery.OriginalQuery)
		assert.Equal(t, "app_code", appQuery.Source)
	})
}

func TestPerformQueryAssessment(t *testing.T) {

	ctx := context.Background()
	projectId := "test-project"
	assessmentConfig := map[string]string{"location": "us-central1"}
	conv := &internal.Conv{
		SpDialect: constants.DIALECT_GOOGLESQL,
		SrcSchema: make(map[string]schema.Table),
		SpSchema:  make(map[string]ddl.CreateTable),
	}
	collectors := assessmentCollectors{}

	t.Run("success with a mix of queries", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return &genai.Client{}, nil
		}

		aiClientService.TranslateQueriesFunc = func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
			assert.Len(t, queries, 1) // Only one performance schema query should be passed.
			assert.Equal(t, "SELECT * FROM users WHERE id = ?", queries[0].Query)
			return []utils.QueryTranslationResult{
				{
					OriginalQuery: queries[0].Query,
					SpannerQuery:  "SELECT * FROM `users` WHERE id = ?",
					Source:        "performance_schema",
				},
			}, nil
		}

		// Input queries: one from perf, one from app code.
		queries := []utils.QueryTranslationResult{
			{OriginalQuery: "SELECT * FROM users", NormalizedQuery: "SELECT * FROM users WHERE id = ?", Source: "performance_schema", ExecutionCount: 100},
			{OriginalQuery: "INSERT INTO products", NormalizedQuery: "INSERT INTO products", Source: "app_code", ExecutionCount: 50},
		}

		result, err := performQueryAssessment(ctx, collectors, queries, projectId, assessmentConfig, conv)

		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, "INSERT INTO products", result[0].OriginalQuery)
		assert.Equal(t, "SELECT * FROM users WHERE id = ?", result[1].OriginalQuery)
		assert.Equal(t, "SELECT * FROM `users` WHERE id = ?", result[1].SpannerQuery)
	})

	t.Run("genai.NewClient returns an error", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return nil, errors.New("client creation error")
		}

		queries := []utils.QueryTranslationResult{}
		result, err := performQueryAssessment(ctx, collectors, queries, projectId, assessmentConfig, conv)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Error creating ai client")
		assert.Nil(t, result)
	})

	t.Run("TranslateQueriesToSpanner returns an error", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return &genai.Client{}, nil
		}

		aiClientService.TranslateQueriesFunc = func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
			return nil, errors.New("translation failed")
		}

		queries := []utils.QueryTranslationResult{
			{OriginalQuery: "SELECT * FROM users", NormalizedQuery: "SELECT * FROM users WHERE id = ?", Source: "performance_schema"},
		}

		result, err := performQueryAssessment(ctx, collectors, queries, projectId, assessmentConfig, conv)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Error translating queries")
		assert.Nil(t, result)
	})

	t.Run("input queries is empty", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return &genai.Client{}, nil
		}

		aiClientService.TranslateQueriesFunc = func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
			assert.Empty(t, queries)
			return nil, fmt.Errorf("no performance schema queries to translate")
		}

		result, err := performQueryAssessment(ctx, collectors, []utils.QueryTranslationResult{}, projectId, assessmentConfig, conv)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no performance schema queries to translate")
		assert.Nil(t, result)
	})

	t.Run("only non-performance_schema queries", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return &genai.Client{}, nil
		}

		aiClientService.TranslateQueriesFunc = func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
			assert.Empty(t, queries)
			return nil, fmt.Errorf("no performance schema queries to translate")
		}

		queries := []utils.QueryTranslationResult{
			{OriginalQuery: "INSERT INTO products", Source: "app_code"},
		}

		result, err := performQueryAssessment(ctx, collectors, queries, projectId, assessmentConfig, conv)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no performance schema queries to translate")
		assert.Len(t, result, 1)
		assert.Equal(t, "INSERT INTO products", result[0].OriginalQuery)
	})
}
