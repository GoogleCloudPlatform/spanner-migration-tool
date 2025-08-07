package assessment

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/stretchr/testify/assert"
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
			{Query: "SELECT * FROM users", Count: 100},
			{Query: "SELECT * FROM products", Count: 50},
		}
		result := combineAndDeduplicateQueries(perfQueries, nil)
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users")
		assert.True(t, ok1)
		assert.Equal(t, "performance_schema", q1.Source)
		assert.Equal(t, 100, q1.ExecutionCount)

		q2, ok2 := findResult(result, "SELECT * FROM products")
		assert.True(t, ok2)
		assert.Equal(t, "performance_schema", q2.Source)
		assert.Equal(t, 50, q2.ExecutionCount)
	})

	t.Run("only app code queries", func(t *testing.T) {
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM users WHERE id = ?", OriginalQuery: "SELECT * FROM users WHERE id = 1", Source: "app_code"},
			{NormalizedQuery: "INSERT INTO orders", OriginalQuery: "INSERT INTO orders VALUES (1)", Source: "app_code"},
		}
		result := combineAndDeduplicateQueries([]utils.QueryAssessmentInfo{}, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "app_code", q1.Source)
		assert.Equal(t, "SELECT * FROM users WHERE id = 1", q1.OriginalQuery)

		q2, ok2 := findResult(result, "INSERT INTO orders")
		assert.True(t, ok2)
		assert.Equal(t, "app_code", q2.Source)
	})

	t.Run("no common queries", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users", Count: 100},
		}
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM products", OriginalQuery: "SELECT * FROM products", Source: "app_code"},
		}
		result := combineAndDeduplicateQueries(perfQueries, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users")
		assert.True(t, ok1)
		assert.Equal(t, "performance_schema", q1.Source)

		q2, ok2 := findResult(result, "SELECT * FROM products")
		assert.True(t, ok2)
		assert.Equal(t, "app_code", q2.Source)
	})

	t.Run("some common queries should be deduplicated", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users", Count: 100}, // Common query
			{Query: "SELECT * FROM orders", Count: 25}, // Unique to perf
		}
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM users", OriginalQuery: "SELECT * FROM users WHERE id = 1", Source: "app_code"}, // Common query
			{NormalizedQuery: "INSERT INTO products", OriginalQuery: "INSERT INTO products", Source: "app_code"},            // Unique to app code
		}
		result := combineAndDeduplicateQueries(perfQueries, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 3)

		// Check the deduplicated query
		dedupQuery, ok1 := findResult(result, "SELECT * FROM users")
		assert.True(t, ok1)
		assert.Equal(t, "SELECT * FROM users WHERE id = 1", dedupQuery.OriginalQuery)
		assert.Equal(t, "app_code, performance_schema", dedupQuery.Source)
		assert.Equal(t, 100, dedupQuery.ExecutionCount) // Should be from performance schema

		// Check the unique perf query
		perfQuery, ok2 := findResult(result, "SELECT * FROM orders")
		assert.True(t, ok2)
		assert.Equal(t, "SELECT * FROM orders", perfQuery.OriginalQuery)
		assert.Equal(t, "performance_schema", perfQuery.Source)
		assert.Equal(t, 25, perfQuery.ExecutionCount)

		// Check the unique app code query
		appQuery, ok3 := findResult(result, "INSERT INTO products")
		assert.True(t, ok3)
		assert.Equal(t, "INSERT INTO products", appQuery.OriginalQuery)
		assert.Equal(t, "app_code", appQuery.Source)
	})

	t.Run("app code query with empty NormalizedQuery", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users", Count: 100},
		}
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "", OriginalQuery: "SELECT * FROM users"}, // Normalized query is empty
		}
		result := combineAndDeduplicateQueries(perfQueries, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 1)

		dedupQuery, ok := findResult(result, "SELECT * FROM users")
		assert.True(t, ok)
		assert.Equal(t, "SELECT * FROM users", dedupQuery.OriginalQuery)
		assert.Equal(t, "app_code, performance_schema", dedupQuery.Source)
		assert.Equal(t, 100, dedupQuery.ExecutionCount)
	})
}
