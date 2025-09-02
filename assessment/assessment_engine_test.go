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

func TestPerformSchemaAssessment(t *testing.T) {
	ctx := context.Background()

	t.Run("panics if infoSchemaCollector is nil", func(t *testing.T) {
		collectors := assessmentCollectors{
			infoSchemaCollector: nil, // Intentionally nil
		}
		assert.Panics(t, func() {
			performSchemaAssessment(ctx, collectors)
		}, "The code should panic on a nil-pointer dereference")
	})

	// Note: A comprehensive unit test for the success path of performSchemaAssessment
	// is challenging because it depends on `assessment.InfoSchemaCollector`, a concrete
	// type from another package. To test the logic thoroughly, this dependency
	// would ideally be an interface, allowing for a mock implementation.
	// An integration-style test would be required to test the current implementation.
}

func TestPerformAppAssessment(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil when app collector is nil", func(t *testing.T) {
		collectors := assessmentCollectors{
			appAssessmentCollector: nil,
		}
		output, err := performAppAssessment(ctx, collectors)
		assert.NoError(t, err)
		assert.Nil(t, output)
	})

	// Note: Testing the success and error paths of performAppAssessment is difficult
	// as it requires mocking `AnalyzeProject` on the concrete `assessment.MigrationCodeSummarizer` type.
	// A full test would require either refactoring the production code to use an interface
	// for the collector, or setting up a complex integration test.
}

func TestIsCharsetCompatible(t *testing.T) {
	testCases := []struct {
		name    string
		charset string
		want    bool
	}{
		{
			name:    "compatible charset (non-utf8)",
			charset: "latin1",
			want:    true,
		},
		{
			name:    "incompatible charset (utf8)",
			charset: "utf8",
			want:    false,
		},
		{
			name:    "incompatible charset (utf8mb4)",
			charset: "utf8mb4",
			want:    false,
		},
		{
			name:    "empty charset string",
			charset: "",
			want:    true,
		},
		{
			name:    "case sensitivity check (UTF8)",
			charset: "UTF8",
			want:    true, // strings.Contains is case-sensitive
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isCharsetCompatible(tc.charset)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTableSizeDiffBytes(t *testing.T) {
	// Note: This test reflects the current dummy implementation.
	// It should be updated when the function logic is implemented.
	t.Run("dummy implementation returns 1", func(t *testing.T) {
		got := tableSizeDiffBytes(&utils.SrcTableDetails{}, &utils.SpTableDetails{})
		assert.Equal(t, 1, got, "Expected dummy implementation to return 1")
	})
}

func TestGetSpColSizeBytes(t *testing.T) {
	const mb = 10 * 1024 * 1024 // 10MB
	testCases := []struct {
		name  string
		input utils.SpColumnDetails
		want  int64
	}{
		{
			name:  "ARRAY type",
			input: utils.SpColumnDetails{Datatype: "ARRAY"},
			want:  mb,
		},
		{
			name:  "BOOL type",
			input: utils.SpColumnDetails{Datatype: "BOOL"},
			want:  8 + 1,
		},
		{
			name:  "BYTES type with length",
			input: utils.SpColumnDetails{Datatype: "BYTES", Len: 256},
			want:  8 + 256,
		},
		{
			name:  "DATE type",
			input: utils.SpColumnDetails{Datatype: "DATE"},
			want:  8 + 4,
		},
		{
			name:  "FLOAT32 type",
			input: utils.SpColumnDetails{Datatype: "FLOAT32"},
			want:  8 + 4,
		},
		{
			name:  "FLOAT64 type",
			input: utils.SpColumnDetails{Datatype: "FLOAT64"},
			want:  8 + 8,
		},
		{
			name:  "INT64 type",
			input: utils.SpColumnDetails{Datatype: "INT64"},
			want:  8 + 8,
		},
		{
			name:  "JSON type",
			input: utils.SpColumnDetails{Datatype: "JSON"},
			want:  mb,
		},
		{
			name:  "NUMERIC type",
			input: utils.SpColumnDetails{Datatype: "NUMERIC"},
			want:  8 + 22,
		},
		{
			name:  "PROTO type with length",
			input: utils.SpColumnDetails{Datatype: "PROTO", Len: 1024},
			want:  8 + 1024,
		},
		{
			name:  "STRING type with length",
			input: utils.SpColumnDetails{Datatype: "STRING", Len: 512},
			want:  8 + 512,
		},
		{
			name:  "STRUCT type",
			input: utils.SpColumnDetails{Datatype: "STRUCT"},
			want:  mb,
		},
		{
			name:  "TIMESTAMP type",
			input: utils.SpColumnDetails{Datatype: "TIMESTAMP"},
			want:  12, // This case returns directly
		},
		{
			name:  "Default case for unknown type",
			input: utils.SpColumnDetails{Datatype: "UNKNOWN_TYPE"},
			want:  8, // This case returns directly
		},
		{
			name:  "Case-insensitivity check (string)",
			input: utils.SpColumnDetails{Datatype: "string", Len: 100},
			want:  8 + 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getSpColSizeBytes(tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}
